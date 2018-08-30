// cache_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-08-29
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-08-30

package cache

import (
	"github.com/bmizerany/assert"
	"testing"
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
