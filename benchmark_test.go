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

func BenchmarkCacheSet(b *testing.B) {
	keyLengths := []int{16}
	valueSizes := []int{2048}
	shardNumbers := []int{1, 4, 16, 32, 64, 128, 256, 512, 1024}

	for _, keyLength := range keyLengths {
		for _, valueSize := range valueSizes {
			for _, shardNumber := range shardNumbers {
				desc := fmt.Sprintf("key-length(%d) value-size(%d) shard-number(%d)",
					keyLength, valueSize, shardNumber)
				b.Run(desc, benchmarkCacheSet(map[string]interface{}{
					"key-length":   keyLength,
					"value-size":   valueSize,
					"shard-number": shardNumber,
				}))
			}
		}
	}
}

func benchmarkCacheSet(m map[string]interface{}) func(*testing.B) {
	return func(b *testing.B) {
		c, _ := New(&Config{
			ShardNumber:   m["shard-number"].(int),
			CleanInterval: time.Hour,
		})
		val := make([]byte, m["value-size"].(int))
		keyLength := m["key-length"].(int)

		b.StartTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c.Set(getStr(keyLength), val)
			}
		})
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
