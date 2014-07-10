// mergejoin_mem_test contains tests for merge join when the data structures are
// all held in memory.

package join

import (
	"sort"
	"testing"
)

// natural join fooBar[foo, bar] with barBaz[bar, baz] resulting in
// fooBarBaz[foo, bar, baz]
type joinExprChan struct {
	left  chan fooBar
	right chan barBaz
	res   chan fooBarBaz
}

// test implementation of MergeJoin for the foo,bar * bar, baz -> foo, bar, baz
// join.
func (e *joinExprChan) MergeJoin(lSize, rSize chan int, updateLeft, updateRight, done chan struct{}) (compare func(i, j int) TupComp, combine func(i, j int)) {
	var (
		leftBlock  []fooBar
		rightBlock []barBaz
	)
	// start the left block reader
	go func() {

		var recs []fooBar
		rec, ok := <-e.left
		if !ok {
			// there was nothing to join on
			close(lSize)
			return
		}

		recs = append(recs, rec)
		sz := 1
		for rec := range e.left {
			if sz == 0 || rec.bar == recs[0].bar {
				recs = append(recs, rec)
				sz++
				continue
			}
			// wait to be told to update the slices
			<-updateLeft

			// update the left slice
			leftBlock = recs
			// send the new size and then reset
			select {
			case lSize <- sz:
				sz = 1
				recs = []fooBar{rec}
			case <-done:
				// early cancellation
				return
			}
		}

		// send the last block

		// wait to be told to update the slices
		<-updateLeft
		// update the left slice
		leftBlock = recs
		// send the new size and then reset

		select {
		case lSize <- sz:
		case <-done:
			// early cancellation
			return
		}
		<-updateLeft
		close(lSize)
		<-updateRight
		close(e.res)

	}()

	// start the right block reader
	go func() {

		var recs []barBaz

		rec, ok := <-e.right
		if !ok {
			// there was nothing to join on
			close(rSize)
			return
		}
		recs = append(recs, rec)

		sz := 1
		for rec := range e.right {
			if sz == 0 || rec.bar == recs[0].bar {
				recs = append(recs, rec)
				sz++
				continue
			}

			// wait to be told to update the slices
			<-updateRight

			// update the left slice
			rightBlock = recs
			// send the new size and then reset
			select {
			case rSize <- sz:
				sz = 1
				recs = []barBaz{rec}
			case <-done:
				// early cancellation
				return
			}
		}

		// send the last block

		// wait to be told to update the slices
		<-updateRight
		// update the left slice
		rightBlock = recs
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
		e.res <- fooBarBaz{leftBlock[i].foo, leftBlock[i].bar, rightBlock[j].baz}
	}
	return
}

func TestChanMergeJoin(t *testing.T) {

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

	e := &joinExprChan{left: fooBarChan(l, 1), right: barBazChan(r, 1), res: make(chan fooBarBaz, 1)}
	go Join(e)

	var resTable []fooBarBaz

	for rec := range e.res {
		resTable = append(resTable, rec)
	}

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
	if resLen := len(resTable); resLen != len(expectTable) {
		t.Errorf("Join() results have len => %v, want %v", resLen, len(expectTable))
	}
	for i := range expectTable {
		if resTable[i] != expectTable[i] {
			t.Errorf("Join() result %d tuple => %v, want %v", i, resTable[i], expectTable[i])
		}
	}

	// test the various record counts used in the benchmarks
	var TT = []struct{
		j *joinExprChan
		N int} {
		{makeMergeJoinChan(10, 10, 3, 10, 10, 3),19},
		{makeMergeJoinChan(32, 32, 9, 32, 32, 9),30},
		{makeMergeJoinChan(100, 100, 30, 100, 100, 30),90},
		{makeMergeJoinChan(316, 316, 90, 316, 316, 90),291},
		{makeMergeJoinChan(1000, 1000, 300, 1000, 1000, 300),1105},
		{makeMergeJoinChan(3162, 3162, 900, 3162, 3162, 900),3247},
		{makeMergeJoinChan(10000, 10000, 3000, 10000, 10000, 3000),9880},
	}
	for i, tt := range(TT) {
		if l := CountResChan(tt.j.res); l != tt.N {
			t.Errorf("%d Table length was => %d, want %d", i, l, tt.N)
		}
	}
}


// create a merge join for testing
func makeMergeJoinChan(leftN, leftFoo, leftBar, rightN, rightBar, rightBaz int) *joinExprChan {
	l := makeFooBar(leftN, leftFoo, leftBar)
	r := makeBarBaz(rightN, rightBar, rightBaz)
	sort.Sort(fooBarByBar(l))
	sort.Sort(barBazByBar(r))
	j := &joinExprChan{left: fooBarChan(l, 1), right: barBazChan(r, 1), res: make(chan fooBarBaz, 1)}
	go Join(j)
	return j
}



func BenchmarkMergeJoinChan10x10(b *testing.B) { // 19 results
	runMergeJoinChanBenchmark(b, 10, 10, 3, 10, 10, 3)
}
func BenchmarkMergeJoinChan32x32(b *testing.B) { // 30 results
	runMergeJoinChanBenchmark(b, 32, 32, 9, 32, 32, 9)
}
func BenchmarkMergeJoinChan100x100(b *testing.B) { // 90 results
	runMergeJoinChanBenchmark(b, 100, 100, 30, 100, 100, 30)
}
func BenchmarkMergeJoinChan316x316(b *testing.B) { // 291 results
	runMergeJoinChanBenchmark(b, 316, 316, 90, 316, 316, 90)
}
func BenchmarkMergeJoinChan1000x1000(b *testing.B) { // 1105 results
	runMergeJoinChanBenchmark(b, 1000, 1000, 300, 1000, 1000, 300)
}
func BenchmarkMergeJoinChan3162x3162(b *testing.B) { // 3247 results
	runMergeJoinChanBenchmark(b, 3162, 3162, 900, 3162, 3162, 900)
}
func BenchmarkMergeJoinChan10000x10000(b *testing.B) { // as many as 9880 results
	runMergeJoinChanBenchmark(b, 10000, 10000, 3000, 10000, 10000, 3000)
}

func EmptyResChan(res chan fooBarBaz) {
	for _ = range res {
	}
}

func CountResChan(res chan fooBarBaz) (i int) {
	for _ = range res {
		i++
	}
	return
}


func runMergeJoinChanBenchmark(b *testing.B, leftN, leftFoo, leftBar, rightN, rightBar, rightBaz int) {
	l := makeFooBar(leftN, leftFoo, leftBar)
	r := makeBarBaz(rightN, rightBar, rightBaz)
	sort.Sort(fooBarByBar(l))
	sort.Sort(barBazByBar(r))
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ex := &joinExprChan{left: fooBarChan(l, 1), right: barBazChan(r, 1), res: make(chan fooBarBaz, 1)}
		b.StartTimer()
		go Join(ex)
		EmptyResChan(ex.res)
	}
}
