// cache.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-08-22
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-08-23

// A concurrent-safe cache for applications running on a single machine. It supports
// set operation with expiration. Elements are not stored in a single pool (map) but
// distributed in many separate regions, which called shard. This design allows us
// to perform some massive operations (like cleaning expired elements) step by step.
package cache

import (
	"sync"
	"time"
)

// Cache configuration.
type Config struct {
	// The elements are not stored in a single pool but distributed in many
	// separate regions, which called shard. ShardNumber specifies how many
	// shards there are. Of course, there must exist one shard at least.
	ShardNumber int

	// Cache will clean expired elements periodically, this parameter controls
	// the frequency of cleaning operations.
	CleanInterval time.Duration

	// When an element is out of date, it will be cleand sliently. But maybe an
	// element is complicated and some additional works are needed to release its
	// resource, which is why Finalizer field exists. When an element is deleted
	// (auto or manual), this field will be applied for it.
	Finalizer func(string, interface{})
}

// Cache is a concurrent-safe cache for applications running on a single machine.
type Cache struct {
}

// Create a Cache instance.
func New(cfg *Config) (*Cache, error) {
	return nil, nil
}

// Add an element to the cache. If the element has already existed, return an error.
func (c *Cache) Add(k string, v interface{}) error {
	return nil
}

// Add an element to the cache. If the element has already existed, replacing it.
func (c *Cache) Set(k string, v interface{}) {
}

// Add an element to the cache. If the element has already existed, return an error.
// If the expiration is zero, the effect is same as using Add method. Otherwise the
// element won't be got when it has expired.
func (c *Cache) EAdd(k string, v interface{}) error {
	return nil
}

// Add an element to the cache with an expiration. If the element has already existed,
// replacing it. If the expiration is zero, the effect is same as using Set method.
// Otherwise the element won't be got when it has expired.
func (c *Cache) ESet(k string, v interface{}, d time.Duration) {
}

// Get an element from the cache. Return nil if this element doesn't exist or has
// already expired.
func (c *Cache) Get(k string) interface{} {
	return nil
}

// Check whether an element exists. If it exists, returns true. Otherwise, returns false.
func (c *Cache) Exist(k string) bool {
	return true
}

// Delete an element from the cache. If the Finalizer field of the cache has been set,
// it will be applied for the element.
func (c *Cache) Del(k string) {
}

// Close the cache. It will release all resources in the cache. You shouldn't use
// this cache anymore after this method has been called.
func (c *Cache) Close() error {
	return nil
}

// A shard contains a part of elements of the entire cache.
type shard struct {
	sync.RWMutex
	elements  map[string]element
	finalizer func(string, interface{})
}

type element struct {
	data       interface{}
	expiration int64
}

// If an element has expired, returns true. Otherwise, returns false. It also
// returns false directly if the expiration field is zero, which means this
// element has unlimited life.
func (e element) expired() bool {
	return e.expiration != 0 && time.Now().UnixNano() > e.expiration
}

const (
	offset32 = 0x811c9dc5
	prime32  = 0x1000193
)

// Takes a string and return a 32 bit FNV-1a. This function makes no memory allocations.
func fnv32a(s string) uint32 {
	var h uint32 = offset32
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}
