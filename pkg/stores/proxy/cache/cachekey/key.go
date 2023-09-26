package cachekey

import (
	"fmt"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type CacheKey struct {
	ListOptions  v1.ListOptions
	ResourcePath string
	Namespace    string
}

// String returns a string containing the fields values of the cacheKey receiver.
func (c CacheKey) String() string {
	return fmt.Sprintf("listOptions: %v, resourcePath: %s, namespace: %s", c.ListOptions.String(), c.ResourcePath, c.Namespace)
}

// ResourceVersion returns the resourceVersion from the CacheKey's ListOption's.
func (c CacheKey) ResourceVersion() string {
	return c.ListOptions.ResourceVersion
}
