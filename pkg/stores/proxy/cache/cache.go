// Package cache implements the sized revision cache. The size revision cache maintains a cache of Unstructuredlists while
// attempting to limit the aggregate values stored by the given memory size (in bytes) parameter.
package cache

import (
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/stores/proxy/cache/cachekey"
	"github.com/rancher/steve/pkg/stores/proxy/cache/rvmonitor"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

var (
	ErrNotFound = errors.New("key was not found in cache")
)

// SizedRevisionCache maintains a cache of Unstructuredlists while attempting to limit the aggregate values stored by
// the given memory size (in bytes) parameter.
type SizedRevisionCache struct {
	listRevisionCache *cache.LRUExpireCache
	cacheLock         sync.Mutex
	size              int
	sizeLimit         int
	rvm               rvmonitor.LatestResourceVersionManager
}

// Cacher is a cache that stores and retrieves UnstructuredLists.
type Cacher interface {
	Get(key cachekey.CacheKey) (*unstructured.UnstructuredList, error)
	Add(key cachekey.CacheKey, list *unstructured.UnstructuredList, setLatest *Latest) error
}

type Latest struct {
	APIOp  *types.APIRequest
	Schema *types.APISchema
}

type cacheObj struct {
	size int
	obj  interface{}
}
type clientGetter interface {
	DynamicClient(ctx *types.APIRequest, warningHandler rest.WarningHandler) (dynamic.Interface, error)
}

// NewSizedRevisionCache accepts a sizeLimit, in bytes, and a maxElements parameter to create a SizedRevisionCache.
func NewSizedRevisionCache(sizeLimit, maxElements int, clientGetter clientGetter) *SizedRevisionCache {
	return &SizedRevisionCache{
		listRevisionCache: cache.NewLRUExpireCache(maxElements),
		sizeLimit:         sizeLimit,
		rvm:               rvmonitor.NewResourceVersionMonitor(clientGetter),
	}
}

// Get returns the UnstructuredList stored under the given cacheKey if available. If not, as error is returned.
func (s *SizedRevisionCache) Get(key cachekey.CacheKey) (*unstructured.UnstructuredList, error) {
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()
	// check if cache stored all namespaces

	if key.ListOptions.ResourceVersion == "" {
		rv := s.rvm.GetLatestRV(key)
		if rv == "" {
			return nil, ErrNotFound
		}
		key.ListOptions.ResourceVersion = rv
	}

	obj, ok := s.listRevisionCache.Get(key)
	if !ok {
		return nil, ErrNotFound
	}
	uList, ok := obj.(cacheObj)
	if !ok {
		return nil, fmt.Errorf("could not assert object stored with key [%s] key as UnstructuredList", key)
	}
	return uList.obj.(*unstructured.UnstructuredList), nil
}

// Add attempts to add the given UnstructuredListed under the given key.
func (s *SizedRevisionCache) Add(key cachekey.CacheKey, list *unstructured.UnstructuredList, setLatest *Latest) error {
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()

	cacheListObj := cacheObj{
		size: getListSize(list),
		obj:  list,
	}

	currentSize := s.sizeOfCurrentEntry(key)

	if err := s.adjustSize(cacheListObj.size - currentSize); err != nil {
		return err
	}
	s.listRevisionCache.Add(key, cacheListObj, 30*time.Minute)

	if setLatest != nil {
		s.rvm.AddRVAndWatch(setLatest.APIOp, setLatest.Schema, key, len(list.Items))
	}

	return nil
}

func (s *SizedRevisionCache) calculateSize() int {
	var total int
	for _, key := range s.listRevisionCache.Keys() {
		total += s.sizeOfCurrentEntry(key)
	}
	return total
}

func (s *SizedRevisionCache) sizeOfCurrentEntry(key interface{}) int {
	obj, ok := s.listRevisionCache.Get(key)
	if !ok {
		return 0
	}

	cacheObject, ok := obj.(cacheObj)
	if !ok {
		return 0
	}

	return cacheObject.size
}

func (s *SizedRevisionCache) adjustSize(diff int) error {
	if !(s.size+diff > s.sizeLimit) {
		s.size += diff
		return nil
	}

	// the size is recalculated here to check whether entries have expired that would
	// make s.size+diff stay below sizeLimit.
	s.size = s.calculateSize()

	if !(s.size+diff > s.sizeLimit) {
		s.sizeLimit += diff
		return nil
	}

	return fmt.Errorf("[steve proxy cache]: cache is near full with a size of [%d] and limit of [%d], cannot increment size by [%d]", s.size, s.sizeLimit, diff)
}

// GetCacheKey returns a cacheKey with the given field values set.
func GetCacheKey(options v1.ListOptions, resourcePath, ns string) cachekey.CacheKey {
	return cachekey.CacheKey{
		ListOptions:  options,
		ResourcePath: resourcePath,
		Namespace:    ns,
	}
}

func getListSize(ul *unstructured.UnstructuredList) int {
	size := int(unsafe.Sizeof(ul))
	for _, item := range ul.Items {
		size += getSize(item)
	}
	return size
}

func getSize(i interface{}) int {
	// sizeOf does not include size of objects pointed to for maps and slices. Therefore, it is necessary to recursively
	// call sizeOf on nested slices, maps, and their respective items, [source](https://pkg.go.dev/unsafe?#Sizeof).
	var size int
	switch val := i.(type) {
	// unstructured interfaces will have a type of string, float, int, bool, []interface{}, or map[string]interface{},
	// [source](https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1/unstructured#Unstructured).

	// Why use unsafe.sizeOf instead of Marshalling:
	// Using unsafe.sizeOf recursively on items in an UnstructuredList has shown to be more performant during benchmarks.
	// Using unsafe.sizeOf is more accurate because it evaluates the size of struct headers and json marshalling includes
	// json related formatting that, other than for storing the json in a var for evaluation, would not be in memory.
	case unstructured.Unstructured:
		return int(unsafe.Sizeof(val)) + getSize(val.Object)
	case []interface{}:
		size += int(unsafe.Sizeof(val))
		for _, v := range val {
			size += getSize(v)
		}
		return size
	case map[string]interface{}:
		size += int(unsafe.Sizeof(val))
		for key, v := range val {
			size += int(unsafe.Sizeof(key)) + getSize(v)
		}
		return size
	case string:
		return int(unsafe.Sizeof(val)) + len(val)
	default:
		return int(unsafe.Sizeof(val))
	}
}
