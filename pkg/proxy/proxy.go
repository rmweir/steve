package proxy

import (
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/http/httpguts"
	"k8s.io/apimachinery/pkg/util/proxy"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
)

func ImpersonatingHandler(prefix string, cfg *rest.Config) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		impersonate(rw, req, prefix, cfg)
	})
}

func setupUserAuth(req *http.Request, user user.Info, cfg *rest.Config) (*rest.Config, bool) {
	authed := true
	for _, group := range user.GetGroups() {
		if group == "system:unauthenticated" && strings.HasPrefix(req.Header.Get("Authorization"), "Bearer ") {
			cfg := rest.CopyConfig(cfg)
			cfg.Username = ""
			cfg.Password = ""
			cfg.BearerTokenFile = ""
			cfg.TLSClientConfig.CertFile = ""
			cfg.TLSClientConfig.CertData = nil
			cfg.ExecProvider = nil
			cfg.AuthConfigPersister = nil
			cfg.Impersonate = rest.ImpersonationConfig{}

			cfg.BearerToken = strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")
			return cfg, true
		} else if group == "system:unauthenticated" {
			authed = false
		}
	}

	cfg = rest.CopyConfig(cfg)
	cfg.Impersonate.UserName = user.GetName()
	cfg.Impersonate.Groups = user.GetGroups()
	cfg.Impersonate.Extra = user.GetExtra()
	return cfg, authed
}

func impersonate(rw http.ResponseWriter, req *http.Request, prefix string, cfg *rest.Config) {
	user, ok := request.UserFrom(req.Context())
	if !ok {
		http.Error(rw, "not authorized", http.StatusUnauthorized)
		return
	}

	cfg, authed := setupUserAuth(req, user, cfg)
	// we want to send a 401, not a 403 for unauthed API requests to make the UI happier
	if !authed && strings.HasPrefix(req.URL.Path, "/api") {
		http.Error(rw, "not authorized", http.StatusUnauthorized)
		return
	}
	handler, err := Handler(prefix, cfg)
	if err != nil {
		logrus.Errorf("failed to impersonate %v for proxy: %v", user, err)
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	req = req.Clone(req.Context())
	for k := range req.Header {
		if strings.HasPrefix(k, "Impersonate-") {
			delete(req.Header, k)
		}
	}
	handler.ServeHTTP(rw, req)
}

// test a roundtrip wrapper
type RTWrapper struct {
	rt http.RoundTripper
}

type EOFReader struct {
	rd io.ReadCloser
	n  int
}

func (e *EOFReader) Read(buff []byte) (int, error) {
	logrus.Infof("%d", e.n)
	if e.n == 1 {
		logrus.Infof("read initial message, exiting")
		return 0, io.EOF
	}
	n, err := e.rd.Read(buff)
	if err != nil {
		logrus.Infof("exiting with error: %v", err)
		logrus.Infof(string(buff))
		return n, err
	}
	logrus.Infof("Message:", string(buff))
	e.n++
	return n, err
}

func (r *RTWrapper) RoundTrip(req *http.Request) (*http.Response, error) {
	logrus.Infof("[asdf] enter rt: %s", req.RequestURI)
	res := &http.Response{}
	var err error
	res, err = r.rt.RoundTrip(req)
	logrus.Infof("[asdf] after rt: %v, Status: %s", req.RequestURI, res.Status)
	if strings.Contains(req.RequestURI, "watch=true") {
		res.Body = io.NopCloser(&EOFReader{rd: res.Body})
		logrus.Infof("closing, %s", req.RequestURI)
	}
	return res, err
}

func isUpgradeType(req *http.Request) bool {
	if !httpguts.HeaderValuesContainsToken(req.Header["Connection"], "Upgrade") {
		return false
	}
	return req.Header.Get("Upgrade") != ""
}

// Mostly copied from "kubectl proxy" code
func Handler(prefix string, cfg *rest.Config) (http.Handler, error) {
	host := cfg.Host
	if !strings.HasSuffix(host, "/") {
		host = host + "/"
	}
	target, err := url.Parse(host)
	if err != nil {
		return nil, err
	}

	transport, err := rest.TransportFor(cfg)
	if err != nil {
		return nil, err
	}
	transport = &RTWrapper{rt: transport}
	upgradeTransport, err := makeUpgradeTransport(cfg, transport)
	if err != nil {
		return nil, err
	}

	proxy := proxy.NewUpgradeAwareHandler(target, transport, false, false, er)
	proxy.UpgradeTransport = upgradeTransport
	proxy.UseRequestLocation = true

	handler := http.Handler(proxy)

	if len(target.Path) > 1 {
		handler = prependPath(target.Path[:len(target.Path)-1], handler)
	}

	if len(prefix) > 2 {
		handler = stripLeaveSlash(prefix, handler)
	}

	return proxyHeaders(handler), nil
}

func proxyHeaders(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		req.Header.Del("Authorization")
		if req.Header.Get("X-Forwarded-Proto") == "" && req.TLS != nil {
			req.Header.Set("X-Forwarded-Proto", "https")
		}
		handler.ServeHTTP(rw, req)
	})
}

func SetHost(host string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		req.Host = host
		h.ServeHTTP(w, req)
	})
}

func prependPath(prefix string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if len(req.URL.Path) > 1 {
			req.URL.Path = prefix + req.URL.Path
		} else {
			req.URL.Path = prefix
		}
		h.ServeHTTP(w, req)
	})
}

// like http.StripPrefix, but always leaves an initial slash. (so that our
// regexps will work.)
func stripLeaveSlash(prefix string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		p := strings.TrimPrefix(req.URL.Path, prefix)
		if len(p) > 0 && p[:1] != "/" {
			p = "/" + p
		}
		req.URL.Path = p
		h.ServeHTTP(w, req)
	})
}

func makeUpgradeTransport(config *rest.Config, rt http.RoundTripper) (proxy.UpgradeRequestRoundTripper, error) {
	transportConfig, err := config.TransportConfig()
	if err != nil {
		return nil, err
	}

	upgrader, err := transport.HTTPWrappersForConfig(transportConfig, proxy.MirrorRequest)
	if err != nil {
		return nil, err
	}

	return proxy.NewUpgradeRequestRoundTripper(rt, upgrader), nil
}
