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
