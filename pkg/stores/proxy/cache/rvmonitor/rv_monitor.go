package rvmonitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/stores/proxy/cache/cachekey"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

type ResourceVersionMonitor struct {
	latestRV          map[cachekey.CacheKey]string
	isResourceWatched map[schema.GroupVersionResource]bool
	monitorLock       sync.Mutex
	clientGetter      clientGetter
	ticker            *time.Ticker
}

type LatestResourceVersionManager interface {
	GetLatestRV(key cachekey.CacheKey) string
	AddRVAndWatch(ctx *types.APIRequest, schema *types.APISchema, key cachekey.CacheKey, drainSize int)
}

type clientGetter interface {
	DynamicClient(ctx *types.APIRequest, warningHandler rest.WarningHandler) (dynamic.Interface, error)
}

func NewResourceVersionMonitor(clientGetter clientGetter) *ResourceVersionMonitor {
	return &ResourceVersionMonitor{
		latestRV:          make(map[cachekey.CacheKey]string),
		isResourceWatched: make(map[schema.GroupVersionResource]bool),
		clientGetter:      clientGetter,
		ticker:            time.NewTicker(30 * time.Minute),
	}
}

func (r *ResourceVersionMonitor) GetLatestRV(key cachekey.CacheKey) string {
	key.ListOptions.ResourceVersion = ""
	r.monitorLock.Lock()
	defer r.monitorLock.Unlock()

	return r.latestRV[key]
}

func (r *ResourceVersionMonitor) AddRVAndWatch(ctx *types.APIRequest, schema *types.APISchema, key cachekey.CacheKey, drainSize int) {
	fmt.Println("ADDRVANDWATCH")
	r.monitorLock.Lock()
	defer r.monitorLock.Unlock()

	gvr := attributes.GVR(schema)
	rv := key.ListOptions.ResourceVersion
	key.ListOptions.ResourceVersion = ""
	r.latestRV[key] = rv
	if r.isResourceWatched[gvr] {
		return
	}

	dc, err := r.clientGetter.DynamicClient(ctx, rest.NoWarnings{})
	if err != nil {
		logrus.Errorf("ERROR IN ADDRVANDWATCH")
		return
	}

	w, err := dc.Resource(attributes.GVR(ctx.Schema)).Watch(context.TODO(), v1.ListOptions{})
	if err != nil {
		logrus.Errorf("ERROR IN ADDRVANDWATCH")
		return
	}

	r.isResourceWatched[gvr] = true
	go func() {
		c := w.ResultChan()
		var inc int
		for range c {
			if inc == drainSize {
				break
			}
			inc++
		}
		select {
		case obj, ok := <-w.ResultChan():
			fmt.Println(obj)
			if ok {
				w.Stop()
			}
			r.monitorLock.Lock()
			defer r.monitorLock.Unlock()
			delete(r.isResourceWatched, gvr)
			delete(r.latestRV, key)
		}
	}()
}
