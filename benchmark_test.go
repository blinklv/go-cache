// benchmark_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-09-27
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-09-27

package cache

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// Perform performance tests for all cache's write operations: Add, Set, EAdd, ESet.
func BenchmarkCacheWrite(b *testing.B) {
	ops := []string{"add", "set", "expire-add", "expire-set"}
	shardNumbers := []int{1, 4, 16, 32, 64, 128, 256, 512, 1024}
	for _, op := range ops {
		for _, shardNumber := range shardNumbers {
			desc := fmt.Sprintf("operation(%s) shard-number(%d)", op, shardNumber)
			b.Run(desc, benchmarkCacheWrite(map[string]interface{}{
				"op":           op,
				"shard-number": shardNumber,
			}))
		}
	}
}

func benchmarkCacheWrite(m map[string]interface{}) func(*testing.B) {
	return func(b *testing.B) {
		c, _ := New(&Config{
			ShardNumber:   m["shard-number"].(int),
			CleanInterval: time.Hour,
		})
		val := make([]byte, 2048)

		b.StartTimer()
		switch m["op"] {
		case "add":
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c.Add(getStr(16), val)
				}
			})
		case "expire-add":
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c.EAdd(getStr(16), val, time.Hour)
				}
			})
		case "set":
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c.Set(getStr(16), val)
				}
			})
		case "expire-set":
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c.ESet(getStr(16), val, time.Hour)
				}
			})
		}
		b.StopTimer()
		c.Close()

	}
}

const LetterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const (
	letterIdxBits = 6
	letterIdxMask = 1<<letterIdxBits - 1
	letterIdxMax  = 63 / letterIdxBits
)

// Generate a random string of length n, its character set is 'LetterBytes'.
func getStr(n int) string {
	b := make([]byte, n)
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(LetterBytes) {
			b[i] = LetterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}
