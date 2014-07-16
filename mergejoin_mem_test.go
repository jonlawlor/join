// mergejoin_mem_test contains tests for merge join when the data structures are
// all held in memory.

package relpipes

import (
	"sort"
	"testing"
)

// natural join fooBar[foo, bar] with barBaz[bar, baz] resulting in
// fooBarBaz[foo, bar, baz]
type joinExprSlice struct {
	left  []fooBar
	right []barBaz
	res   []fooBarBaz
}

// test implementation of MergeJoin for the foo,bar * bar, baz -> foo, bar, baz
// join.
func (e *joinExprSlice) MergeJoin(lSize, rSize chan int, advanceLeft, advanceRight, done chan struct{}) (compare func() TupComp, combine func(i, j int)) {
	if len(e.left) == 0 || len(e.right) == 0 {
		// zero join
		close(lSize)
		close(rSize)
		return
	}

	var (
		leftBlock  []fooBar
		rightBlock []barBaz
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

			<-advanceLeft
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
		<-advanceLeft
		// update the left slice
		leftBlock = e.left[start:]
		// send the new size and then reset
		select {
		case lSize <- sz:
		case <-done:
			// early cancellation
			return
		}
		<-advanceLeft
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
			<-advanceRight
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
		<-advanceRight
		// update the left slice
		rightBlock = e.right[start:]
		// send the new size and then reset
		select {
		case rSize <- sz:
		case <-done:
			// early cancellation
			return
		}
		<-advanceRight
		close(rSize)
	}()

	compare = func() TupComp {

		if leftBlock[0].bar == rightBlock[0].bar {
			return EQ
		}
		if leftBlock[0].bar < rightBlock[0].bar {
			return LT
		}
		return GT
	}
	combine = func(i, j int) {
		e.res = append(e.res, fooBarBaz{leftBlock[i].foo, leftBlock[i].bar, rightBlock[j].baz})
	}
	return
}

func TestMemMergeJoin(t *testing.T) {

	l := []fooBar{
		{1, 1},
		{1, 2},
		{2, 3},
		{3, 3},
		{1, 4},
		{2, 5},
	}

	r := []barBaz{
		{1, 1},
		{1, 2},
		{2, 2},
		{3, 1},
		{3, 4},
		{4, 4},
	}
	e := &joinExprSlice{left: l, right: r}
	Join(e)
	expectTable := []fooBarBaz{
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
		t.Errorf("Join() results have len => %v, want %v", len(e.res), len(expectTable))
	}
	for i := range expectTable {
		if e.res[i] != expectTable[i] {
			t.Errorf("Join() result %d tuple => %v, want %v", i, e.res[i], expectTable[i])
		}
	}

	// test the various record counts used in the benchmarks
	var TT = []struct {
		j *joinExprSlice
		N int
	}{
		{makeMergeJoinMem(10, 10, 3, 10, 10, 3), 19},
		{makeMergeJoinMem(32, 32, 9, 32, 32, 9), 30},
		{makeMergeJoinMem(100, 100, 30, 100, 100, 30), 90},
		{makeMergeJoinMem(316, 316, 90, 316, 316, 90), 291},
		{makeMergeJoinMem(1000, 1000, 300, 1000, 1000, 300), 1105},
		{makeMergeJoinMem(3162, 3162, 900, 3162, 3162, 900), 3247},
		{makeMergeJoinMem(10000, 10000, 3000, 10000, 10000, 3000), 9880},
	}
	for i, tt := range TT {
		if l := len(tt.j.res); l != tt.N {
			t.Errorf("%d Table length was => %d, want %d", i, l, tt.N)
		}
	}
}

// create a merge join for testing
func makeMergeJoinMem(leftN, leftFoo, leftBar, rightN, rightBar, rightBaz int) *joinExprSlice {
	l := makeFooBar(leftN, leftFoo, leftBar)
	r := makeBarBaz(rightN, rightBar, rightBaz)
	sort.Sort(fooBarByBar(l))
	sort.Sort(barBazByBar(r))
	j := &joinExprSlice{left: l, right: r}
	Join(j)
	return j
}

func BenchmarkMergeJoinMem10x10(b *testing.B) { // 19 results
	runMergeJoinMemBenchmark(b, 10, 10, 3, 10, 10, 3)
}
func BenchmarkMergeJoinMem32x32(b *testing.B) { // 30 results
	runMergeJoinMemBenchmark(b, 32, 32, 9, 32, 32, 9)
}
func BenchmarkMergeJoinMem100x100(b *testing.B) { // 90 results
	runMergeJoinMemBenchmark(b, 100, 100, 30, 100, 100, 30)
}
func BenchmarkMergeJoinMem316x316(b *testing.B) { // 291 results
	runMergeJoinMemBenchmark(b, 316, 316, 90, 316, 316, 90)
}
func BenchmarkMergeJoinMem1000x1000(b *testing.B) { // 1105 results
	runMergeJoinMemBenchmark(b, 1000, 1000, 300, 1000, 1000, 300)
}
func BenchmarkMergeJoinMem3162x3162(b *testing.B) { // 9880 results
	runMergeJoinMemBenchmark(b, 3162, 3162, 900, 3162, 3162, 900)
}
func BenchmarkMergeJoinMem10000x10000(b *testing.B) { // as many as 1e8
	runMergeJoinMemBenchmark(b, 10000, 10000, 3000, 10000, 10000, 3000)
}

func BenchmarkMergeJoinMemUnsorted10x10(b *testing.B) { // as many as 1e2
	runMergeJoinMemUnsortedBenchmark(b, 10, 10, 3, 10, 10, 3)
}
func BenchmarkMergeJoinMemUnsorted32x32(b *testing.B) { // as many as 1e3
	runMergeJoinMemUnsortedBenchmark(b, 32, 32, 9, 32, 32, 9)
}
func BenchmarkMergeJoinMemUnsorted100x100(b *testing.B) { // as many as 1e4
	runMergeJoinMemUnsortedBenchmark(b, 100, 100, 30, 100, 100, 30)
}
func BenchmarkMergeJoinMemUnsorted316x316(b *testing.B) { // as many as 1e5
	runMergeJoinMemUnsortedBenchmark(b, 316, 316, 90, 316, 316, 90)
}
func BenchmarkMergeJoinMemUnsorted1000x1000(b *testing.B) { // as many as 1e6
	runMergeJoinMemUnsortedBenchmark(b, 1000, 1000, 300, 1000, 1000, 300)
}
func BenchmarkMergeJoinMemUnsorted3162x3162(b *testing.B) { // as many as 1e7
	runMergeJoinMemUnsortedBenchmark(b, 3162, 3162, 900, 3162, 3162, 900)
}
func BenchmarkMergeJoinMemUnsorted10000x10000(b *testing.B) { // as many as 1e8
	runMergeJoinMemUnsortedBenchmark(b, 10000, 10000, 3000, 10000, 10000, 3000)
}

// in this case, the sorting time is not included
func runMergeJoinMemBenchmark(b *testing.B, leftN, leftFoo, leftBar, rightN, rightBar, rightBaz int) {
	l := makeFooBar(leftN, leftFoo, leftBar)
	r := makeBarBaz(rightN, rightBar, rightBaz)
	sort.Sort(fooBarByBar(l))
	sort.Sort(barBazByBar(r))
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ex := &joinExprSlice{left: l, right: r}
		b.StartTimer()
		Join(ex)
	}
}

// in this case, the sorting time is included
func runMergeJoinMemUnsortedBenchmark(b *testing.B, leftN, leftFoo, leftBar, rightN, rightBar, rightBaz int) {
	l := makeFooBar(leftN, leftFoo, leftBar)
	r := makeBarBaz(rightN, rightBar, rightBaz)
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lft := l[:]
		rgt := r[:]
		ex := &joinExprSlice{left: lft, right: rgt}
		b.StartTimer()
		sort.Sort(fooBarByBar(l))
		sort.Sort(barBazByBar(r))
		Join(ex)
	}
}
