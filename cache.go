// cache.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-08-22
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-09-17

// A concurrent-safe cache for applications running on a single machine. It supports
// set operation with expiration. Elements are not stored in a single pool (map) but
// distributed in many separate regions, which called shard. This design allows us
// to perform some massive operations (like cleaning expired elements) progressively.
package cache

import (
	"fmt"
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
	// the frequency of cleaning operations. It can't be less than 1 min in this
	// version; otherwise, many CPU cycles are occupied by the 'clean' task.
	CleanInterval time.Duration

	// When an element is out of date, it will be cleand sliently. But maybe an
	// element is complicated and some additional works are needed to release its
	// resource, which is why Finalizer field exists. When an element is deleted
	// (auto or manual), this field will be applied for it.
	Finalizer func(string, interface{})
}

const (
	minShardNumber   = 1
	minCleanInterval = time.Minute
)

// Check whether the configuration is right. If it's invalid, return an error.
func (cfg *Config) validate() error {
	if cfg.ShardNumber < minShardNumber {
		return fmt.Errorf("the number of shards (%d) can't be less than %d",
			cfg.ShardNumber, minShardNumber)
	}

	if cfg.CleanInterval < minCleanInterval {
		return fmt.Errorf("the clean interval (%s) can't be less than %s",
			cfg.CleanInterval, minCleanInterval)
	}
	return nil
}

// Cache is a concurrent-safe cache for applications running on a single machine.
type Cache struct {
	shards   []*shard
	n        uint32
	interval time.Duration
	exit     chan chan struct{}
}

// Create a Cache instance. The configuration parameter should be valid when it's not
// nil; like the number of shards must be greater than or equal to 1 and the clean
// interval can't be less than 1 minute. If it's nil, the underlying shard number and
// clean interval will be set to 32 and 1 hour respectively by default.
func New(cfg *Config) (*Cache, error) {
	n, interval, finalizer := 32, time.Hour, (func(string, interface{}))(nil)
	if cfg != nil {
		if err := cfg.validate(); err != nil {
			return nil, err
		}
		n, interval, finalizer = cfg.ShardNumber, cfg.CleanInterval, cfg.Finalizer
	}

	c := &Cache{
		shards:   make([]*shard, n),
		n:        uint32(n),
		interval: interval,
		exit:     make(chan chan struct{}),
	}

	for i, _ := range c.shards {
		c.shards[i] = &shard{
			elements:  make(map[string]element),
			finalizer: finalizer,
			q:         &queue{},
		}
	}
	go c.clean()

	return c, nil
}

// Add an element to the cache. If the element has already existed, return an error.
func (c *Cache) Add(k string, v interface{}) error {
	return c.shards[fnv32a(k)&c.n].add(k, v, 0)
}

// Add an element to the cache. If the element has already existed, replacing it.
func (c *Cache) Set(k string, v interface{}) {
	c.shards[fnv32a(k)&c.n].set(k, v, 0)
}

// Add an element to the cache. If the element has already existed, return an error.
// If the expiration is zero, the effect is same as using Add method. Otherwise the
// element won't be got when it has expired.
func (c *Cache) EAdd(k string, v interface{}, d time.Duration) error {
	return c.shards[fnv32a(k)&c.n].add(k, v, d)
}

// Add an element to the cache with an expiration. If the element has already existed,
// replacing it. If the expiration is zero, the effect is same as using Set method.
// Otherwise the element won't be got when it has expired.
func (c *Cache) ESet(k string, v interface{}, d time.Duration) {
	c.shards[fnv32a(k)&c.n].set(k, v, d)
}

// Get an element from the cache. Return nil if this element doesn't exist or has
// already expired.
func (c *Cache) Get(k string) interface{} {
	return c.shards[fnv32a(k)&c.n].get(k)
}

// Check whether an element exists. If it exists, returns true. Otherwise, returns false.
func (c *Cache) Exist(k string) bool {
	return c.shards[fnv32a(k)&c.n].exist(k)
}

// Delete an element from the cache. If the Finalizer field of the cache has been set,
// it will be applied for the element.
func (c *Cache) Del(k string) {
	c.shards[fnv32a(k)&c.n].del(k)
}

// Close the cache. All elements in the cache will be deleted. If the Finalizer field
// of the cache has been set, it will be applied for the all elements in the cache. You
// shouldn't use this cache anymore after this method has been called.
func (c *Cache) Close() error {
	exitDone := make(chan struct{})
	c.exit <- exitDone
	<-exitDone
	return nil
}

// Clean expired elements in the cache periodically.
func (c *Cache) clean() {
	for {
		select {
		case <-time.After(c.interval):
			// It's not all shards execute clean opearation simultaneously, but
			// one by one. It's too waste time when a shard execute the clean
			// method, if all shards do this at the same time, all user requests
			// will be blocked. So I decide clean shards sequentially to reduce
			// this effect.
			for _, s := range c.shards {
				s.clean()
			}
		case exitDone := <-c.exit:
			for _, s := range c.shards {
				s.close()
			}
			close(exitDone)
			return
		}
	}
}

// A shard contains a part of elements of the entire cache.
type shard struct {
	sync.RWMutex
	elements  map[string]element
	finalizer func(string, interface{})

	// If we set the expiration for an element, the index of which will be
	// saved in this queue. The primary objective of designing this field is
	// iterating expired elements in an incremental way.
	q *queue
}

// Add an element to the shard. If the element has already existed but not expired,
// return an error.
func (s *shard) add(k string, v interface{}, d time.Duration) error {
	s.Lock()
	defer s.Unlock()

	if e, found := s.elements[k]; found && !e.expired() {
		return fmt.Errorf("element (%s) has already existed", k)
	}

	s._set(k, v, d)
	return nil
}

// Add an element to the shard. If the element has existed, replacing it. If the
// duration is zero, which means this element never expires.
func (s *shard) set(k string, v interface{}, d time.Duration) {
	s.Lock()
	s._set(k, v, d)
	s.Unlock()
}

func (s *shard) _set(k string, v interface{}, d time.Duration) {
	expiration := int64(0)
	if d > 0 {
		expiration = time.Now().Add(d).UnixNano()
		// NOTE: If the element has existed, there might be multiple indices related
		// to it in the queue. They have the same key but different expiration, but
		// only one of which is valid.
		s.q.push(index{k, expiration})
	}
	s.elements[k] = element{v, expiration}
}

// Get an element from the shard. Return nil if this element doesn't exist or
// has already expired.
func (s *shard) get(k string) interface{} {
	s.RLock()
	e, found := s.elements[k]
	s.RUnlock()

	if !found || e.expired() {
		return nil
	}
	return e.data
}

// Check whether an element exists. Return true if the element exists and hasn't expired.
func (s *shard) exist(k string) bool {
	s.RLock()
	e, found := s.elements[k]
	s.RUnlock()

	return found && !e.expired()
}

// Delete an element from the shard. If the finalizer field of the shard has been set,
// it will be applied for the element.
func (s *shard) del(k string) {
	s.Lock()
	e, found := s.elements[k]
	// NOTE: We don't need to remove the index (or indices) of this element
	// from the queue at this point.
	delete(s.elements, k)
	s.Unlock()

	if found && s.finalizer != nil {
		s.finalizer(k, e.data)
	}
}

// Clean all expired elements in the shard, return the number of elements cleaned
// in this process. NOTE: You can't run this method of a shard instance many times
// at the same time.
func (s *shard) clean() int {
	q, n := &queue{}, 0
	for b := s.pop(); b != nil; b = s.pop() {
		for _, i := range b.indices {
			expired := false

			s.Lock()
			e, found := s.elements[i.key]
			// At first, we need to ensure that there exist an element related to
			// this index. The following two cases will make the index invalid:
			// 1. The element has already been deleted manually.
			// 2. The index is dirty, which means the element has been updated. In
			//    this case, their expirations are not equal.
			if found && e.expiration == i.expiration {
				// Then we check whether the element has expired. If it has, we remove
				// it from the map. Otherwise, we must save the index to the temporary
				// queue, which for the next clean.
				if expired = e.expired(); expired {
					delete(s.elements, i.key)
					n++
				} else {
					q.push(i)
				}
			}
			s.Unlock()

			// We don't know how many time the following statement will cost, it may
			// take a lot of time. so I don't place it into the above critical region.
			if expired && s.finalizer != nil {
				s.finalizer(i.key, e.data)
			}
		}
	}

	s.Lock()
	s.q = q
	s.Unlock()

	return n
}

func (s *shard) pop() (b *block) {
	s.Lock()
	b = s.q.pop()
	s.Unlock()
	return
}

// Close the shard. In fact, the pratical effect of this operation is deleting all
// elements in the shard. If the finalizer field is not nil, it will be applied for
// each element.
func (s *shard) close() {
	s.Lock()
	defer s.Unlock()

	if s.finalizer != nil {
		for k, e := range s.elements {
			s.finalizer(k, e.data)
		}
	}

	// NOTE: We just set 'elements' and 'queue' field to an empty variable instead
	// of really deleting all items in them, GC will help us do this work.
	s.elements, s.q = make(map[string]element), &queue{}
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

// A queue which contains indices.
type queue struct {
	top, tail *block
	bn        int // blocks number.
}

func (q *queue) push(i index) {
	// The queue is empty; we need to append a block to it. In this case,
	// top and tail reference the same block.
	if q.tail == nil {
		q.tail, q.bn = &block{}, 1
		q.top = q.tail
	}

	// The last block is full; we need to append a new block to the queue
	// and update tail.
	if len(q.tail.indices) == blockCapacity {
		q.tail.next = &block{}
		q.tail = q.tail.next
		q.bn++
	}

	// Append an index to the last block.
	q.tail.indices = append(q.tail.indices, i)
}

// NOTE: We pop a black from the queue instead of an index.
func (q *queue) pop() (b *block) {
	// We can classify problems into three cases:
	//  1. There exist two blocks in the queue at least. We only need to
	//     return the block referenced by top and update top.
	//  2. There exist only one block in the queue. We need to return the
	//     block referenced by top, then set top and tail to nil. Because
	//     they reference to the same one in this case.
	//  3. There's no block in the queue. Returns nil directly.
	if q.top != nil {
		if q.top != q.tail {
			b, q.top = q.top, q.top.next
		} else {
			b, q.top, q.tail = q.top, nil, nil
		}
		q.bn--
	}
	return
}

// Get the number of indices in the queue.
func (q *queue) size() int {
	if q.tail != nil {
		return (q.bn-1)*blockCapacity + len(q.tail.indices)
	}
	return 0
}

// Compute the blocks number of the queue; it's only used in test now.
func (q *queue) _bn() int {
	bn := 0
	for top := q.top; top != nil; top = top.next {
		bn++
	}
	return bn
}

// Get the number of indices in the tail block; it's only used in test now.
func (q *queue) _tailSize() int {
	if q.tail != nil {
		return len(q.tail.indices)
	}
	return 0
}

// The maximal number of indices in one block.
const blockCapacity = 32

// block is the basic unit for cleaning out expired elements. Multiple indices are
// stored in a common block and multiple blocks are organized in linked-list format.
type block struct {
	indices []index
	next    *block
}

// We only build an index for an element which expiration field is not zero.
type index struct {
	key        string
	expiration int64
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
