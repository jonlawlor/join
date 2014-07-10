// mergejoin_mem_test contains tests for merge join when the data structures are
// all held in memory.

package join

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
func (e *joinExprSlice) MergeJoin(lSize, rSize chan int, updateLeft, updateRight, done chan struct{}) (compare func(i, j int) TupComp, combine func(i, j int)) {
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
}

func BenchmarkMergeJoinMem10x10(b *testing.B) { // as many as 1e2
	runMergeJoinMemBenchmark(b, 10, 10, 3, 10, 10, 3)
}
func BenchmarkMergeJoinMem32x32(b *testing.B) { // as many as 1e3
	runMergeJoinMemBenchmark(b, 32, 32, 9, 32, 32, 9)
}
func BenchmarkMergeJoinMem100x100(b *testing.B) { // as many as 1e4
	runMergeJoinMemBenchmark(b, 100, 100, 30, 100, 100, 30)
}
func BenchmarkMergeJoinMem316x316(b *testing.B) { // as many as 1e5
	runMergeJoinMemBenchmark(b, 316, 316, 90, 316, 316, 90)
}
func BenchmarkMergeJoinMem1000x1000(b *testing.B) { // as many as 1e6
	runMergeJoinMemBenchmark(b, 1000, 1000, 300, 1000, 1000, 300)
}
func BenchmarkMergeJoinMem3162x3162(b *testing.B) { // as many as 1e7
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
