// mergejoin_mem_test contains tests for merge join when the data structures are
// all held in memory.

package join

import (
	"testing"
)

// natural join foobar[foo, bar] with barbaz[bar, baz] resulting in
// foobarbaz[foo, bar, baz]

type foobar struct {
	foo int
	bar int
}

type barbaz struct {
	bar int
	baz int
}

type foobarbaz struct {
	foo int
	bar int
	baz int
}

type joinExpr struct {
	left  []foobar
	right []barbaz
	res   []foobarbaz
}

// test implementation of MergeJoin for the foo,bar * bar, baz -> foo, bar, baz
// join.
func (e *joinExpr) MergeJoin(lSize, rSize chan int, updateLeft, updateRight, done chan struct{}) (compare func(i, j int) TupComp, combine func(i, j int)) {
	if len(e.left) == 0 || len(e.right) == 0 {
		// zero join
		close(lSize)
		close(rSize)
		return
	}

	var (
		leftBlock  []foobar
		rightBlock []barbaz
	)
	// start the left block reader
	go func() {

		sz := 1
		start := 0

		for i := 1; i < len(e.left); i++ {
			if sz == 0 || e.left[i].bar == e.left[start].bar {
				sz++
				continue
			}

			// wait to be told to update the slices

			<-updateLeft
			// update the left slice
			leftBlock = e.left[start:i]
			// send the new size and then reset
			select {
			case lSize <- sz:
				sz = 1
				start = i
			case <-done:
				// early cancellation
				return
			}
		}

		// send the last block

		// wait to be told to update the slices
		<-updateLeft
		// update the left slice
		leftBlock = e.left[start:]
		// send the new size and then reset
		select {
		case lSize <- sz:
		case <-done:
			// early cancellation
			return
		}
		<-updateLeft
		close(lSize)
	}()

	// start the right block reader
	go func() {

		sz := 1
		start := 0

		for i := 1; i < len(e.right); i++ {
			if sz == 0 || e.right[i].bar == e.right[start].bar {
				sz++
				continue
			}

			// wait to be told to update the slices
			<-updateRight
			// update the left slice
			rightBlock = e.right[start:i]
			// send the new size and then reset

			select {
			case rSize <- sz:
				sz = 1
				start = i
			case <-done:
				// early cancellation
				return
			}
		}

		// send the last block
		// wait to be told to update the slices
		<-updateRight
		// update the left slice
		rightBlock = e.right[start:]
		// send the new size and then reset
		select {
		case rSize <- sz:
		case <-done:
			// early cancellation
			return
		}
		<-updateRight
		close(rSize)
	}()

	compare = func(i, j int) TupComp {

		if leftBlock[i].bar == rightBlock[j].bar {
			return EQ
		}
		if leftBlock[i].bar < rightBlock[j].bar {
			return LT
		}
		return GT
	}
	combine = func(i, j int) {
		e.res = append(e.res, foobarbaz{leftBlock[i].foo, leftBlock[i].bar, rightBlock[j].baz})
	}
	return
}

func TestMemMergeJoin(t *testing.T) {

	l := []foobar{
		{1, 1},
		{1, 2},
		{2, 3},
		{3, 3},
		{1, 4},
		{2, 5},
	}

	r := []barbaz{
		{1, 1},
		{1, 2},
		{2, 2},
		{3, 1},
		{3, 4},
		{4, 4},
	}
	e := &joinExpr{left: l, right: r, res: []foobarbaz{}}
	Join(e)
	expectTable := []foobarbaz{
		{1, 1, 1},
		{1, 1, 2},
		{1, 2, 2},
		{2, 3, 1},
		{2, 3, 4},
		{3, 3, 1},
		{3, 3, 4},
		{1, 4, 4},
	}
	if resLen := len(e.res); resLen != len(expectTable) {
		t.Errorf("Join() results have len => %v, want %v", e.res, len(expectTable))
	}
	for i := range expectTable {
		if e.res[i] != expectTable[i] {
			t.Errorf("Join() result %d tuple => %v, want %v", i, e.res[i], expectTable[i])
		}
	}
}
