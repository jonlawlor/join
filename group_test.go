package relpipes

import (
	"runtime"
	"sort"
	"testing"
	"sync"
	)
// a set of tests and benchmarks for group

// note that if you are adapting this to your own use, you should probably
// chose a different name for the tuple (other than fooBar!) as it is just
// a placeholder.

type predicate func(t1, t2 *fooBar) bool

func p(t1, t2 *fooBar) bool { return t1.foo == t2.foo }

// we can only perform grouping on equality when we're using a hash
type subFooBar struct{ foo int }

func proj(t *fooBar) subFooBar { return subFooBar{t.foo} }

// groupOrdered takes two channels, one for input and one for
// results, and passes tuples that belong to the same group
// to the appropriate output channel.  It also takes a
// predicate, which when evaluated on two input tuples returns
// a true if they belong to the same group, otherwise false.
// It also takes n, which is the buffer size that should be
// used for the inner result channels.
func groupOrdered(in <-chan fooBar, res chan (chan fooBar), p predicate, n int) {
	go func() {

		// initialization is a bit trickier here.
		// We can't compare the first tuple to any
		// other tuples, but that's fine because
		// it means we can sent it immeidately.
		inner := make(chan fooBar, n)
		prev, ok := <-in
		// On the other hand, in might have been
		// closed and never sent any values, so we
		// have to check for that case.
		if ok {
			res <- inner
			inner <- prev
		} else {
			close(res)
			return
		}

		// read each input,
		for v := range in {
			// if it is has not already been
			// encountered, send it on the results
			// channel and then update the prev
			// variable.  If it has not been
			// encountered, then close the results
			// channel and create a new one
			if p(&v, &prev) {
				inner <- v
			} else {
				close(inner)
				inner = make(chan fooBar, n)
				res <- inner
				inner <- v
				prev = v
			}
		}
		// close the resulting channels
		close(inner)
		close(res)
	}()
	return
}

// ordered benchmarks

func emptyFooBarGroup(ch chan (chan fooBar)) {
	for inner := range ch {
		for _ = range inner {
			// do nothing, just recv
		}
	}
}

func benchGroupOrdered(b *testing.B, tupN, fooN, barN int) {
	in := makeFooBar(tupN, fooN, barN)
	sort.Sort(fooBarByBar(in))
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := make(chan (chan fooBar), 1)

		b.StartTimer()
		groupOrdered(fooBarChan(in, 1), res, p, 2)

		emptyFooBarGroup(res)
		b.StopTimer()
	}
}

// benchmarks for group with cardinality ~3
func BenchmarkGroupOrdered_10_3_1(b *testing.B) {
	benchGroupOrdered(b, 10, 3, 1)
}
func BenchmarkGroupOrdered_100_3_1(b *testing.B) {
	benchGroupOrdered(b, 100, 3, 1)
}
func BenchmarkGroupOrdered_1000_3_1(b *testing.B) {
	benchGroupOrdered(b, 1000, 3, 1)
}
func BenchmarkGroupOrdered_10000_3_1(b *testing.B) {
	benchGroupOrdered(b, 10000, 3, 1)
}
func BenchmarkGroupOrdered_100000_3_1(b *testing.B) {
	benchGroupOrdered(b, 100000, 3, 1)
}
func BenchmarkGroupOrdered_1000000_3_1(b *testing.B) {
	benchGroupOrdered(b, 1000000, 3, 1)
}

// benchmarks for group with cardinality ~1000
func BenchmarkGroupOrdered_10_1000_1(b *testing.B) {
	benchGroupOrdered(b, 10, 1000, 1)
}
func BenchmarkGroupOrdered_100_1000_1(b *testing.B) {
	benchGroupOrdered(b, 100, 1000, 1)
}
func BenchmarkGroupOrdered_1000_1000_1(b *testing.B) {
	benchGroupOrdered(b, 1000, 1000, 1)
}
func BenchmarkGroupOrdered_10000_1000_1(b *testing.B) {
	benchGroupOrdered(b, 10000, 1000, 1)
}
func BenchmarkGroupOrdered_100000_1000_1(b *testing.B) {
	benchGroupOrdered(b, 100000, 1000, 1)
}
func BenchmarkGroupOrdered_1000000_1000_1(b *testing.B) {
	benchGroupOrdered(b, 1000000, 1000, 1)
}

// benchmarks for group with cardinality ~100000
func BenchmarkGroupOrdered_10_100000_1(b *testing.B) {
	benchGroupOrdered(b, 10, 100000, 1)
}
func BenchmarkGroupOrdered_100_100000_1(b *testing.B) {
	benchGroupOrdered(b, 100, 100000, 1)
}
func BenchmarkGroupOrdered_1000_100000_1(b *testing.B) {
	benchGroupOrdered(b, 1000, 100000, 1)
}
func BenchmarkGroupOrdered_10000_100000_1(b *testing.B) {
	benchGroupOrdered(b, 10000, 100000, 1)
}
func BenchmarkGroupOrdered_100000_100000_1(b *testing.B) {
	benchGroupOrdered(b, 100000, 100000, 1)
}
func BenchmarkGroupOrdered_1000000_100000_1(b *testing.B) {
	benchGroupOrdered(b, 1000000, 100000, 1)
}



// groupUnordered takes two channels, one for input and one for
// results, and passes tuples that belong to the same group
// to the appropriate output channel. It also takes n, which is
// the buffer size that should be used for the inner result channels.
func groupUnordered(in <-chan fooBar, res chan (chan fooBar), n int) {

	go func() {
		m := make(map[subFooBar]chan fooBar)
		var wg sync.WaitGroup

		// read each input,
		for v := range in {
			// if it is has not already been
			// encountered, create a new goroutine
			// to handle it
			sub := proj(&v)
			if _, exist := m[sub]; !exist {
				wg.Add(1)
				g := make(chan fooBar)
				m[sub] = g
				go makeGroup(g, res, &wg, n)
			}
			m[sub] <- v
		}
		// close the grouping channels
		for _, ch := range m {
			close(ch)
		}
		// once the groups are all finished,
		// close the result group channel
		wg.Wait()
		close(res)
	}()
	return
}

// makeGroup handles an individual group.  It will never block on
// a recv from groupChan
func makeGroup(in chan fooBar, res chan (chan fooBar), wg *sync.WaitGroup, n int) {

	groupChan := make(chan fooBar, n)
	sendChan := groupChan
	sentTup := true
	tupBuf := make([]fooBar, 0) // a stack of tuples
	var v fooBar

	for {
		if bufLen := len(tupBuf); sentTup && bufLen > 0 {
			// there is a need to send a new value
			// pop one of the tuples out of the buffer
			v, tupBuf = tupBuf[bufLen-1], tupBuf[:bufLen-1]
			sentTup = false
			sendChan = groupChan
		} else {
			if sentTup {
				if in == nil {
					// there is nothing left to send
					break
				}
				sendChan = nil
			}
		}
		select {
		case res <- groupChan:
			res = nil
		case sendChan <- v:
			sentTup = true
		case tup, ok := <-in:
			if !ok {
				// input channel is closed
				in = nil
				break
			}
			tupBuf = append(tupBuf, tup)
		}
	}
	wg.Done()
	close(groupChan)
}

// benchmark

func benchGroupUnordered(b *testing.B, tupN, fooN, barN int) {
	mc := runtime.GOMAXPROCS(2)
	in := makeFooBar(tupN, fooN, barN)
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := make(chan (chan fooBar), 1)

		b.StartTimer()
		groupUnordered(fooBarChan(in, 1), res, 2)

		emptyFooBarGroup(res)
		b.StopTimer()
	}
	runtime.GOMAXPROCS(mc)

}

// benchmarks for group with cardinality ~3
func BenchmarkGroupUnordered_10_3_1(b *testing.B) {
	benchGroupUnordered(b, 10, 3, 1)
}
func BenchmarkGroupUnordered_100_3_1(b *testing.B) {
	benchGroupUnordered(b, 100, 3, 1)
}
func BenchmarkGroupUnordered_1000_3_1(b *testing.B) {
	benchGroupUnordered(b, 1000, 3, 1)
}
func BenchmarkGroupUnordered_10000_3_1(b *testing.B) {
	benchGroupUnordered(b, 10000, 3, 1)
}
func BenchmarkGroupUnordered_100000_3_1(b *testing.B) {
	benchGroupUnordered(b, 100000, 3, 1)
}
func BenchmarkGroupUnordered_1000000_3_1(b *testing.B) {
	benchGroupUnordered(b, 1000000, 3, 1)
}

// benchmarks for group with cardinality ~1000
func BenchmarkGroupUnordered_10_1000_1(b *testing.B) {
	benchGroupUnordered(b, 10, 1000, 1)
}
func BenchmarkGroupUnordered_100_1000_1(b *testing.B) {
	benchGroupUnordered(b, 100, 1000, 1)
}
func BenchmarkGroupUnordered_1000_1000_1(b *testing.B) {
	benchGroupUnordered(b, 1000, 1000, 1)
}
func BenchmarkGroupUnordered_10000_1000_1(b *testing.B) {
	benchGroupUnordered(b, 10000, 1000, 1)
}
func BenchmarkGroupUnordered_100000_1000_1(b *testing.B) {
	benchGroupUnordered(b, 100000, 1000, 1)
}
func BenchmarkGroupUnordered_1000000_1000_1(b *testing.B) {
	benchGroupUnordered(b, 1000000, 1000, 1)
}

// benchmarks for group with cardinality ~100000
func BenchmarkGroupUnordered_10_100000_1(b *testing.B) {
	benchGroupUnordered(b, 10, 100000, 1)
}
func BenchmarkGroupUnordered_100_100000_1(b *testing.B) {
	benchGroupUnordered(b, 100, 100000, 1)
}
func BenchmarkGroupUnordered_1000_100000_1(b *testing.B) {
	benchGroupUnordered(b, 1000, 100000, 1)
}
func BenchmarkGroupUnordered_10000_100000_1(b *testing.B) {
	benchGroupUnordered(b, 10000, 100000, 1)
}
func BenchmarkGroupUnordered_100000_100000_1(b *testing.B) {
	benchGroupUnordered(b, 100000, 100000, 1)
}
func BenchmarkGroupUnordered_1000000_100000_1(b *testing.B) {
	benchGroupUnordered(b, 1000000, 100000, 1)
}
