// example_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-10-11
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-10-11

package cache_test

import (
	"fmt"
	"github.com/blinklv/go-cache"
	"log"
	"testing"
	"time"
)

// The following example illustrates how to use this package.
func Example() {
	c, err := cache.New(&cache.Config{
		ShardNumber:   128,
		CleanInterval: 30 * time.Minute,
	})
	if err != nil {
		log.Fatalf("create cache failed (%s)", err)
	}
	defer c.Close()

	c.ESet("hello", "world", 5*time.Second)
	fmt.Printf("I get: %v\n", c.Get("hello"))
	time.Sleep(5 * time.Second)
	fmt.Printf("I get: %v\n", c.Get("hello"))
	// Output:
	// I get: world
	// I get: <nil>
}

func ExampleNew() {
	c, _ := cache.New(&cache.Config{
		ShardNumber:   128,
		CleanInterval: 30 * time.Minute,
		Finalizer: func(key string, value interface{}) {
			log.Printf("%s:%v", key, value)
		},
	})
	c.Close()
	// Output:
}

// The following three ways of creating a cache using default configuration
// have the same effect.
func ExampleNew_default() {
	c, _ := cache.New(&cache.Config{
		ShardNumber:   cache.DefaultShardNumber,
		CleanInterval: cache.DefaultCleanInterval,
	})
	c.Close()

	c, _ = cache.New(&cache.Config{})
	c.Close()

	c, _ = cache.New(nil)
	c.Close()
	// Output:
}

// The following is a black box test for this package.
func TestCache(t *testing.T) {
	if testing.Short() {
		return
	}

	c, err := cache.New(&cache.Config{
		ShardNumber:   64,
		CleanInterval: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("create cache failed (%s)", err)
	}

	defer c.Close()

	exit := make(chan struct{})

	// write goroutine
	go func() {
		base, interval := batchESet(c, 0, 100000, 0), 1*time.Minute
		d := 2 * time.Minute
		for {
			if d != 0 {
				base = batchESet(c, base, 100000, d)
				time.Sleep(interval)
				d -= 10 * time.Second
			} else {
				close(exit)
				return
			}
		}
	}()

	// stats
	ticker := time.NewTicker(10 * time.Second)
outer:
	for {
		select {
		case <-ticker.C:
			log.Printf("cache size (%d)", c.Size())
		case <-exit:
			ticker.Stop()
			log.Printf("write goroutine quit")
			break outer
		}
	}

	for {
		size := c.Size()
		log.Printf("cache size (%d)", size)
		if size == 100000 {
			log.Printf("exit")
			return
		}
		time.Sleep(10 * time.Second)
	}
}

func batchESet(c *cache.Cache, base, n int, d time.Duration) int {
	for i := 0; i < n; i++ {
		c.ESet(fmt.Sprintf("%d", base+i), make([]byte, 1024), d)
	}
	return base + n
}
