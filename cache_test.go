// cache_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-08-29
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-09-05

package cache

import (
	"fmt"
	"github.com/bmizerany/assert"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueuePush(t *testing.T) {
	elements := []struct {
		n int
	}{
		{10},
		{50},
		{100},
		{250},
		{500},
		{1000},
		{2000},
		{5000},
	}

	for _, e := range elements {
		q := &queue{}
		for i := 0; i < e.n; i++ {
			q.push(index{})
		}
		t.Logf("indices-number (%d) blocks-number (%d) actual-blocks-number (%d) tail-size (%d)",
			q.size(), q.bn, q._bn(), q._tailSize())
		assert.Equal(t, q.size(), e.n)
		assert.Equal(t, q._bn(), (e.n+blockCapacity-1)/blockCapacity)
		assert.Equal(t, q.bn, q._bn())
		assert.Equal(t, q._tailSize(), e.n%blockCapacity)
	}
}

func TestQueuePop(t *testing.T) {
	elements := []struct {
		pushNumber int
		popNumber  int
		nilNumber  int
	}{
		{pushNumber: 1000, popNumber: 25},
		{pushNumber: 1000, popNumber: 40},
		{pushNumber: 2000, popNumber: 40},
		{pushNumber: 0, popNumber: 10},
		{pushNumber: 10 * blockCapacity, popNumber: 10},
		{pushNumber: 10*blockCapacity + blockCapacity/2, popNumber: 11},
	}

	for _, e := range elements {
		q := &queue{}
		for i := 0; i < e.pushNumber; i++ {
			q.push(index{})
		}
		assert.Equal(t, q.size(), e.pushNumber)
		assert.Equal(t, q.bn, q._bn())

		for i := 0; i < e.popNumber; i++ {
			if q.pop() == nil {
				e.nilNumber++
			}
		}

		t.Logf("indices-number (%d) blocks-number (%d) nil-pop (%d)",
			q.size(), q.bn, e.nilNumber)

		bn := (e.pushNumber + blockCapacity - 1) / blockCapacity
		if bn <= e.popNumber {
			assert.Equal(t, q.bn, 0)
			assert.Equal(t, q.size(), 0)
			assert.Equal(t, e.nilNumber, e.popNumber-bn)
		} else {
			assert.Equal(t, q.bn, q._bn())
			assert.Equal(t, q.bn, bn-e.popNumber)
			assert.Equal(t, e.nilNumber, 0)
		}
	}
}

func TestShardAdd(t *testing.T) {
	elements := []struct {
		s        *shard
		ws       *workers
		keys     []string
		lifetime time.Duration
		interval time.Duration
		total    int64
		fail     int64
	}{
		{
			s:    &shard{elements: make(map[string]element), q: &queue{}},
			ws:   &workers{wn: 1, number: 256},
			keys: []string{"foo", "bar", "hello", "world"},
		},
		{
			s:    &shard{elements: make(map[string]element), q: &queue{}},
			ws:   &workers{wn: 16, number: 256},
			keys: []string{"foo", "bar", "hello", "world"},
		},
		{
			s:        &shard{elements: make(map[string]element), q: &queue{}},
			ws:       &workers{wn: 16, number: 256},
			keys:     []string{"foo", "bar", "hello", "world", "apple"},
			lifetime: time.Second,
			interval: 100 * time.Millisecond,
		},
		{
			s:        &shard{elements: make(map[string]element), q: &queue{}},
			ws:       &workers{wn: 32, number: 256},
			keys:     []string{"foo", "bar", "hello", "world", "apple", "geek"},
			lifetime: 500 * time.Millisecond,
			interval: 50 * time.Millisecond,
		},
	}

	for _, e := range elements {
		e.ws.cb = func(w *worker, i int) error {
			if e.interval != 0 {
				time.Sleep(e.interval)
			}

			atomic.AddInt64(&e.total, 1)
			key := e.keys[i%len(e.keys)]
			if e.s.add(key, key, e.lifetime) != nil {
				atomic.AddInt64(&e.fail, 1)
			}
			return nil
		}

		e.ws.initialize()
		e.ws.run()
		t.Logf("total (%d) fail (%d)", e.total, e.fail)

		assert.Equal(t, len(e.s.elements), len(e.keys))
		if e.lifetime != 0 && e.interval != 0 {
			success := (e.ws.number/int(e.lifetime/e.interval) + 1) * len(e.keys)
			min, max := int(float64(success)*0.8), int(float64(success)*1.2)
			assert.Equal(t, int(e.total-e.fail) >= min, true)
			assert.Equal(t, int(e.total-e.fail) <= max, true)
			assert.Equal(t, e.s.q.size(), int(e.total-e.fail))
		} else {
			assert.Equal(t, int(e.total-e.fail), len(e.keys))
		}
	}
}

func TestShardSet(t *testing.T) {
	elements := []struct {
		s          *shard
		ws         *workers
		bg         *boolgen
		notExpired int64
		expired    int64
	}{
		{
			s:  &shard{elements: make(map[string]element), q: &queue{}},
			ws: &workers{wn: 1, number: 256},
			bg: newBoolgen(),
		},
		{
			s:  &shard{elements: make(map[string]element), q: &queue{}},
			ws: &workers{wn: 4, number: 1024},
			bg: newBoolgen(),
		},
		{
			s:  &shard{elements: make(map[string]element), q: &queue{}},
			ws: &workers{wn: 32, number: 2048},
			bg: newBoolgen(),
		},
		{
			s:  &shard{elements: make(map[string]element), q: &queue{}},
			ws: &workers{wn: 128, number: 8192},
			bg: newBoolgen(),
		},
	}

	for _, e := range elements {
		e.ws.cb = func(w *worker, i int) error {
			if e.bg.Bool() {
				e.s.set(fmt.Sprintf("%d-%d", w.id, i), i, 0)
				atomic.AddInt64(&e.notExpired, 1)
			} else {
				e.s.set(fmt.Sprintf("%d-%d", w.id, i), i, 30*time.Second)
				atomic.AddInt64(&e.expired, 1)
			}
			return nil
		}

		e.ws.initialize()
		e.ws.run()

		actualNotExpired, actualExpired := len(e.s.elements)-e.s.q.size(), e.s.q.size()
		t.Logf("not-expired/actual-not-expired (%d/%d) expired/actual-expired (%d/%d)",
			e.notExpired, actualNotExpired, e.expired, actualExpired)

		assert.Equal(t, actualNotExpired, int(e.notExpired))
		assert.Equal(t, actualExpired, int(e.expired))
	}
}

func TestShardGet(t *testing.T) {
	elements := []struct {
		s        *shard
		ws       *workers
		n        int
		fail     int64
		lifetime time.Duration
		interval time.Duration
	}{
		{
			s:  &shard{elements: make(map[string]element), q: &queue{}},
			ws: &workers{wn: 1, number: 256},
			n:  128,
		},
		{
			s:  &shard{elements: make(map[string]element), q: &queue{}},
			ws: &workers{wn: 4, number: 512},
			n:  256,
		},
		{
			s:  &shard{elements: make(map[string]element), q: &queue{}},
			ws: &workers{wn: 32, number: 1024},
			n:  100,
		},
		{
			s:        &shard{elements: make(map[string]element), q: &queue{}},
			ws:       &workers{wn: 32, number: 1024},
			n:        1024,
			lifetime: 100 * time.Millisecond,
			interval: 10 * time.Millisecond,
		},
		{
			s:        &shard{elements: make(map[string]element), q: &queue{}},
			ws:       &workers{wn: 32, number: 1024},
			n:        330,
			lifetime: 100 * time.Millisecond,
			interval: 10 * time.Millisecond,
		},
	}

	for _, e := range elements {
		for i := 0; i < e.n; i++ {
			k := fmt.Sprintf("%d", i)
			assert.Equal(t, e.s.add(k, k, e.lifetime), nil)
		}

		e.ws.cb = func(w *worker, i int) error {
			if e.interval != 0 {
				time.Sleep(e.interval)
			}

			k := fmt.Sprintf("%d", i)
			x := e.s.get(k)
			if v, ok := x.(string); !ok || v != k {
				atomic.AddInt64(&e.fail, 1)
			}
			return nil
		}

		e.ws.initialize()
		e.ws.run()

		total := e.ws.wn * e.ws.number
		t.Logf("total (%d) fail (%d) success (%d)", total, e.fail, total-int(e.fail))
		if e.lifetime == 0 {
			assert.Equal(t, e.ws.number-int(e.fail)/e.ws.wn, e.n)
		} else {
			assert.Equal(t, e.ws.number-int(e.fail)/e.ws.wn < e.n, true)
		}
	}
}

type worker struct {
	id     int
	number int
	cb     func(*worker, int) error
}

func (w *worker) run(wg *sync.WaitGroup) error {
	defer wg.Done()
	for i := 0; i < w.number; i++ {
		if err := w.cb(w, i); err != nil {
			return err
		}
	}
	return nil
}

type workers struct {
	wn     int
	number int
	cb     func(*worker, int) error

	ws []*worker
	wg *sync.WaitGroup
}

func (ws *workers) initialize() {
	ws.ws = make([]*worker, ws.wn)
	ws.wg = &sync.WaitGroup{}

	for i := 0; i < ws.wn; i++ {
		ws.ws[i] = &worker{i, ws.number, ws.cb}
	}
}

func (ws *workers) run() {
	for _, w := range ws.ws {
		w := w
		ws.wg.Add(1)
		go w.run(ws.wg)
	}
	ws.wg.Wait()
}

// The original design of the following struct is from StackOverflow:
// https://stackoverflow.com/questions/45030618/generate-a-random-bool-in-go?answertab=active#tab-top
type boolgen struct {
	src       rand.Source
	cache     int64
	remaining int
}

func newBoolgen() *boolgen {
	return &boolgen{src: rand.NewSource(time.Now().UnixNano())}
}

func (b *boolgen) Bool() bool {
	if b.remaining == 0 {
		b.cache, b.remaining = b.src.Int63(), 63
	}

	result := b.cache&0x01 == 1
	b.cache >>= 1
	b.remaining--

	return result
}
