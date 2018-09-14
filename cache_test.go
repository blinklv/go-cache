// cache_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-08-29
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-09-14

package cache

import (
	"fmt"
	"github.com/bmizerany/assert"
	"math/rand"
	"strconv"
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

func TestShardGetAndExist(t *testing.T) {
	elements := []struct {
		s        *shard
		ws       *workers
		n        int
		getFail  int64
		notExist int64
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
				atomic.AddInt64(&e.getFail, 1)
			}

			if !e.s.exist(k) {
				atomic.AddInt64(&e.notExist, 1)
			}

			return nil
		}

		e.ws.initialize()
		e.ws.run()

		total := e.ws.wn * e.ws.number
		t.Logf("total (%d) get-fail/not-exist (%d/%d) success (%d)",
			total, e.getFail, e.notExist, total-int(e.getFail))

		assert.Equal(t, e.getFail, e.notExist)
		if e.lifetime == 0 {
			assert.Equal(t, e.ws.number-int(e.getFail)/e.ws.wn, e.n)
		} else {
			assert.Equal(t, e.ws.number-int(e.getFail)/e.ws.wn < e.n, true)
		}
	}
}

func TestShardDel(t *testing.T) {
	elements := []struct {
		s  *shard
		ws *workers
		n  int
	}{
		{
			s:  &shard{elements: make(map[string]element), q: &queue{}},
			ws: &workers{wn: 1, number: 256},
			n:  128,
		},
		{
			s:  &shard{elements: make(map[string]element), q: &queue{}},
			ws: &workers{wn: 4, number: 256},
			n:  127,
		},
		{
			s:  &shard{elements: make(map[string]element), q: &queue{}},
			ws: &workers{wn: 16, number: 1024},
			n:  512,
		},
		{
			s:  &shard{elements: make(map[string]element), q: &queue{}},
			ws: &workers{wn: 32, number: 512},
			n:  1001,
		},
	}

	for _, e := range elements {
		for i := 0; i < e.n; i++ {
			k := fmt.Sprintf("%d", i)
			assert.Equal(t, e.s.add(k, k, 0), nil)
		}

		var dn, fn, en int64

		e.s.finalizer = func(k string, v interface{}) {
			ki, _ := strconv.Atoi(k)
			vi, _ := strconv.Atoi(v.(string))
			if ki == vi && ki%2 == 1 {
				atomic.AddInt64(&fn, 1)
			}
		}

		e.ws.cb = func(w *worker, i int) error {
			if i%2 == 1 {
				e.s.del(fmt.Sprintf("%d", i))
				atomic.AddInt64(&dn, 1)
			}
			return nil
		}

		e.ws.initialize()
		e.ws.run()

		for i := 0; i < e.n; i++ {
			if e.s.exist(fmt.Sprintf("%d", i)) {
				en++
			}
		}

		t.Logf("rest (%d) delete (%d) finalize (%d)", en, dn, fn)
		assert.Equal(t, int(en+fn), e.n)
	}
}

func TestShardClean(t *testing.T) {
	elements := []struct {
		s        *shard
		parts    int
		n        int
		lifetime time.Duration
	}{
		{
			s:        &shard{elements: make(map[string]element), q: &queue{}},
			parts:    4,
			n:        1024,
			lifetime: 2 * time.Second,
		},
		{
			s:        &shard{elements: make(map[string]element), q: &queue{}},
			parts:    8,
			n:        512,
			lifetime: 2 * time.Second,
		},
		{
			s:        &shard{elements: make(map[string]element), q: &queue{}},
			parts:    16,
			n:        2048,
			lifetime: 5 * time.Second,
		},
	}

	for _, e := range elements {
		for part := 0; part < e.parts; part++ {
			for beg, end := part*e.n, (part+1)*e.n; beg < end; beg++ {
				k := fmt.Sprintf("%d", beg)
				e.s.add(k, beg, time.Duration(part)*e.lifetime)
				e.s.set(k, beg, time.Duration(part)*e.lifetime)
			}
		}

		assert.Equal(t, e.s.q.size(), 2*(len(e.s.elements)-e.n))

		for part := 1; part < e.parts; part++ {
			time.Sleep(e.lifetime)
			cleaned := e.s.clean()

			t.Logf("rest (%d) indices (%d) cleaned (%d)",
				len(e.s.elements), e.s.q.size(), cleaned)

			assert.Equal(t, cleaned, e.n)
			assert.Equal(t, e.s.q.size(), len(e.s.elements)-e.n)
			assert.Equal(t, len(e.s.elements), (e.parts-part)*e.n)
		}

		for part := 0; part < e.parts; part++ {
			for beg, end := part*e.n, (part+1)*e.n; beg < end; beg++ {
				k := fmt.Sprintf("%d", beg)
				assert.Equal(t, part == 0 && e.s.exist(k) || part != 0 && !e.s.exist(k), true)
			}
		}
	}
}

func TestShardClose(t *testing.T) {
	elements := []struct {
		s        *shard
		n        int
		lifetime time.Duration
	}{
		{
			s:        &shard{elements: make(map[string]element), q: &queue{}},
			n:        1024,
			lifetime: time.Minute,
		},
		{
			s:        &shard{elements: make(map[string]element), q: &queue{}},
			n:        2048,
			lifetime: time.Minute,
		},
		{
			s:        &shard{elements: make(map[string]element), q: &queue{}},
			n:        4096,
			lifetime: 0,
		},
	}

	for _, e := range elements {
		for i := 0; i < e.n; i++ {
			e.s.set(fmt.Sprintf("%d", i), i, e.lifetime)
		}

		size, qsize := len(e.s.elements), e.s.q.size()
		assert.Equal(t, len(e.s.elements), e.n)
		if e.lifetime != 0 {
			assert.Equal(t, e.s.q.size(), e.n)
		}

		fn := 0
		e.s.finalizer = func(k string, v interface{}) {
			fn++
		}

		e.s.close()

		t.Logf("size/original-size (%d/%d) queue-size/original-queue-size (%d/%d) finalize-count (%d)",
			size, len(e.s.elements), qsize, e.s.q.size(), fn)
		assert.Equal(t, fn, e.n)
		assert.Equal(t, len(e.s.elements), 0)
		assert.Equal(t, e.s.q.size(), 0)
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
