// cache.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-08-22
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-08-22

// A concurrent-safe cache for applications running on a single machine. It supports
// set operation with expiration. Elements are not stored in a single pool (map) but
// distributed in many separate regions, which called shard. This design allows us
// to perform some massive operations (like cleaning expired elements) step by step.
package cache

import (
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

func (c *Cache) Add(k string, v interface{}) error {
	return nil
}

// Add an element to the cache. If the element has already existed, replacing it.
func (c *Cache) Set(k string, v interface{}) {
}

func (c *Cache) EAdd(k string, v interface{}) error {
	return nil
}

// Add an element to the cache with an expiration. If the element has already existed,
// replacing it. If the expiration is zero, the effect is same as using Set method.
// Otherwise the element won't be got when it has been expired.
func (c *Cache) ESet(k string, v interface{}, d time.Duration) {
}

func (c *Cache) Get(k string) interface{} {
	return nil
}

func (c *Cache) Exist(k string) bool {
	return true
}

func (c *Cache) Del(k string) {
}

func (c *Cache) Close() error {
	return nil
}
