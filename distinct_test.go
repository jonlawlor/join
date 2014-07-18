package relpipes

import (
	"runtime"
	"sort"
	"sync"
	"testing"
)

// a set of tests and benchmarks for some implementations of distinct

// note that if you are adapting this to your own use, you should probably
// chose a different name for the tuple (other than fooBar!) as it is just
// a placeholder.

// simplest version

// distinct takes two channels, one for input and one for results,
// and only passes unique values to the results channel.  Once the
// input channel is closed and all values have been sent to the
// results channel, the results channel is closed.
func distinct(in <-chan fooBar, res chan<- fooBar) {
	mem := make(map[fooBar]struct{})

	go func() {
		// read each input,
		for v := range in {
			// if it is has not already been
			// encountered, make a new entry for it
			// and send it to the result chan.
			if _, dup := mem[v]; !dup {
				mem[v] = struct{}{}
				res <- v
			}
		}
		close(res)
	}()
	return
}

func emptyFooBar(ch chan fooBar) {
	for _ = range ch {
		// do nothing, just recv
	}
}

func benchDistinct(b *testing.B, tupN, fooN, barN int) {
	in := makeFooBar(tupN, fooN, barN)
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := make(chan fooBar, 1)

		b.StartTimer()
		distinct(fooBarChan(in, 1), res)

		emptyFooBar(res)
		b.StopTimer()
	}
}

// benchmarks for distinct with cardinality ~3
func BenchmarkDistinct_10_3_1(b *testing.B) {
	benchDistinct(b, 10, 3, 1)
}
func BenchmarkDistinct_100_3_1(b *testing.B) {
	benchDistinct(b, 100, 3, 1)
}
func BenchmarkDistinct_1000_3_1(b *testing.B) {
	benchDistinct(b, 1000, 3, 1)
}
func BenchmarkDistinct_10000_3_1(b *testing.B) {
	benchDistinct(b, 10000, 3, 1)
}
func BenchmarkDistinct_100000_3_1(b *testing.B) {
	benchDistinct(b, 100000, 3, 1)
}
func BenchmarkDistinct_1000000_3_1(b *testing.B) {
	benchDistinct(b, 1000000, 3, 1)
}

// benchmarks for distinct with cardinality ~1000
func BenchmarkDistinct_10_1000_1(b *testing.B) {
	benchDistinct(b, 10, 1000, 1)
}
func BenchmarkDistinct_100_1000_1(b *testing.B) {
	benchDistinct(b, 100, 1000, 1)
}
func BenchmarkDistinct_1000_1000_1(b *testing.B) {
	benchDistinct(b, 1000, 1000, 1)
}
func BenchmarkDistinct_10000_1000_1(b *testing.B) {
	benchDistinct(b, 10000, 1000, 1)
}
func BenchmarkDistinct_100000_1000_1(b *testing.B) {
	benchDistinct(b, 100000, 1000, 1)
}
func BenchmarkDistinct_1000000_1000_1(b *testing.B) {
	benchDistinct(b, 1000000, 1000, 1)
}

// benchmarks for distinct with cardinality ~100000
func BenchmarkDistinct_10_100000_1(b *testing.B) {
	benchDistinct(b, 10, 100000, 1)
}
func BenchmarkDistinct_100_100000_1(b *testing.B) {
	benchDistinct(b, 100, 100000, 1)
}
func BenchmarkDistinct_1000_100000_1(b *testing.B) {
	benchDistinct(b, 1000, 100000, 1)
}
func BenchmarkDistinct_10000_100000_1(b *testing.B) {
	benchDistinct(b, 10000, 100000, 1)
}
func BenchmarkDistinct_100000_100000_1(b *testing.B) {
	benchDistinct(b, 100000, 100000, 1)
}
func BenchmarkDistinct_1000000_100000_1(b *testing.B) {
	benchDistinct(b, 1000000, 100000, 1)
}

// concurrent (unordered) version

// distinctParallel takes two channels, one for input and one for
// results, and only passes unique values to the results channel.
// Once the input channel is closed and all values have been sent
// to the results channel, the results channel is closed.  It also
// takes n, the number of goroutines that are executed
// concurrently.
func distinctParallel(in <-chan fooBar, res chan<- fooBar, n int) {

	// sharedMem adds Lock() and Unlock() to the memory of sent
	// tuples.
	sharedMem := struct {
		sync.Mutex
		mem map[fooBar]struct{}
	}{mem: make(map[fooBar]struct{})}

	// We have to wait for all goroutines to finish before
	// closing the results channel.
	var wg sync.WaitGroup
	wg.Add(n)
	go func() {
		wg.Wait()
		close(res)
	}()

	for i := 0; i < n; i++ {
		go func() {
			// read each input,
			for v := range in {
				// if it is has not already been
				// encountered, make a new entry
				// forit and send it to the
				// result chan
				// note that both reads and
				// writes require locking the
				// shared map
				sharedMem.Lock()
				if _, dup := sharedMem.mem[v]; !dup {
					sharedMem.mem[v] = struct{}{}
					sharedMem.Unlock()
					res <- v
				} else {
					sharedMem.Unlock()
				}
			}
			wg.Done()
		}()
	}
	return
}

func benchDistinctParallel(b *testing.B, tupN, fooN, barN int) {
	mc := runtime.GOMAXPROCS(2)
	in := makeFooBar(tupN, fooN, barN)
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := make(chan fooBar, 1)

		b.StartTimer()
		distinctParallel(fooBarChan(in, 1), res, 2)

		emptyFooBar(res)
		b.StopTimer()
	}
	runtime.GOMAXPROCS(mc)
}

// benchmarks for distinct with cardinality ~3
func BenchmarkDistinctParallel_10_3_1(b *testing.B) {
	benchDistinctParallel(b, 10, 3, 1)
}
func BenchmarkDistinctParallel_100_3_1(b *testing.B) {
	benchDistinctParallel(b, 100, 3, 1)
}
func BenchmarkDistinctParallel_1000_3_1(b *testing.B) {
	benchDistinctParallel(b, 1000, 3, 1)
}
func BenchmarkDistinctParallel_10000_3_1(b *testing.B) {
	benchDistinctParallel(b, 10000, 3, 1)
}
func BenchmarkDistinctParallel_100000_3_1(b *testing.B) {
	benchDistinctParallel(b, 100000, 3, 1)
}
func BenchmarkDistinctParallel_1000000_3_1(b *testing.B) {
	benchDistinctParallel(b, 1000000, 3, 1)
}

// benchmarks for distinct with cardinality ~1000
func BenchmarkDistinctParallel_10_1000_1(b *testing.B) {
	benchDistinctParallel(b, 10, 1000, 1)
}
func BenchmarkDistinctParallel_100_1000_1(b *testing.B) {
	benchDistinctParallel(b, 100, 1000, 1)
}
func BenchmarkDistinctParallel_1000_1000_1(b *testing.B) {
	benchDistinctParallel(b, 1000, 1000, 1)
}
func BenchmarkDistinctParallel_10000_1000_1(b *testing.B) {
	benchDistinctParallel(b, 10000, 1000, 1)
}
func BenchmarkDistinctParallel_100000_1000_1(b *testing.B) {
	benchDistinctParallel(b, 100000, 1000, 1)
}
func BenchmarkDistinctParallel_1000000_1000_1(b *testing.B) {
	benchDistinctParallel(b, 1000000, 1000, 1)
}

// benchmarks for distinct with cardinality ~100000
func BenchmarkDistinctParallel_10_100000_1(b *testing.B) {
	benchDistinctParallel(b, 10, 100000, 1)
}
func BenchmarkDistinctParallel_100_100000_1(b *testing.B) {
	benchDistinctParallel(b, 100, 100000, 1)
}
func BenchmarkDistinctParallel_1000_100000_1(b *testing.B) {
	benchDistinctParallel(b, 1000, 100000, 1)
}
func BenchmarkDistinctParallel_10000_100000_1(b *testing.B) {
	benchDistinctParallel(b, 10000, 100000, 1)
}
func BenchmarkDistinctParallel_100000_100000_1(b *testing.B) {
	benchDistinctParallel(b, 100000, 100000, 1)
}
func BenchmarkDistinctParallel_1000000_100000_1(b *testing.B) {
	benchDistinctParallel(b, 1000000, 100000, 1)
}

// ordered version

// distinctOrdered takes two channels, one for input and one for
// results, and only passes unique values to the results channel.
// Once the input channel is closed and all values have been sent
// to the results channel, the results channel is closed.  The
// input channel can produce values in ascending or descending
// order.  The result will be produced in the same order.
func distinctOrdered(in <-chan fooBar, res chan<- fooBar) {
	var prev fooBar
	go func() {
		// read each input,
		for v := range in {
			// if it is has not already been
			// encountered, send it on the results
			// channel and then update the prev
			// variable.
			if v != prev {
				res <- v
				prev = v
			}
		}
		close(res)
	}()
	return
}

// benchmarks

func benchDistinctOrdered(b *testing.B, tupN, fooN, barN int) {
	in := makeFooBar(tupN, fooN, barN)
	sort.Sort(fooBarByBar(in))
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := make(chan fooBar, 1)

		b.StartTimer()
		distinctOrdered(fooBarChan(in, 1), res)

		emptyFooBar(res)
		b.StopTimer()
	}
}

// benchmarks for distinct with cardinality ~3
func BenchmarkDistinctOrdered_10_3_1(b *testing.B) {
	benchDistinctOrdered(b, 10, 3, 1)
}
func BenchmarkDistinctOrdered_100_3_1(b *testing.B) {
	benchDistinctOrdered(b, 100, 3, 1)
}
func BenchmarkDistinctOrdered_1000_3_1(b *testing.B) {
	benchDistinctOrdered(b, 1000, 3, 1)
}
func BenchmarkDistinctOrdered_10000_3_1(b *testing.B) {
	benchDistinctOrdered(b, 10000, 3, 1)
}
func BenchmarkDistinctOrdered_100000_3_1(b *testing.B) {
	benchDistinctOrdered(b, 100000, 3, 1)
}
func BenchmarkDistinctOrdered_1000000_3_1(b *testing.B) {
	benchDistinctOrdered(b, 1000000, 3, 1)
}

// benchmarks for distinct with cardinality ~1000
func BenchmarkDistinctOrdered_10_1000_1(b *testing.B) {
	benchDistinctOrdered(b, 10, 1000, 1)
}
func BenchmarkDistinctOrdered_100_1000_1(b *testing.B) {
	benchDistinctOrdered(b, 100, 1000, 1)
}
func BenchmarkDistinctOrdered_1000_1000_1(b *testing.B) {
	benchDistinctOrdered(b, 1000, 1000, 1)
}
func BenchmarkDistinctOrdered_10000_1000_1(b *testing.B) {
	benchDistinctOrdered(b, 10000, 1000, 1)
}
func BenchmarkDistinctOrdered_100000_1000_1(b *testing.B) {
	benchDistinctOrdered(b, 100000, 1000, 1)
}
func BenchmarkDistinctOrdered_1000000_1000_1(b *testing.B) {
	benchDistinctOrdered(b, 1000000, 1000, 1)
}

// benchmarks for distinct with cardinality ~100000
func BenchmarkDistinctOrdered_10_100000_1(b *testing.B) {
	benchDistinctOrdered(b, 10, 100000, 1)
}
func BenchmarkDistinctOrdered_100_100000_1(b *testing.B) {
	benchDistinctOrdered(b, 100, 100000, 1)
}
func BenchmarkDistinctOrdered_1000_100000_1(b *testing.B) {
	benchDistinctOrdered(b, 1000, 100000, 1)
}
func BenchmarkDistinctOrdered_10000_100000_1(b *testing.B) {
	benchDistinctOrdered(b, 10000, 100000, 1)
}
func BenchmarkDistinctOrdered_100000_100000_1(b *testing.B) {
	benchDistinctOrdered(b, 100000, 100000, 1)
}
func BenchmarkDistinctOrdered_1000000_100000_1(b *testing.B) {
	benchDistinctOrdered(b, 1000000, 100000, 1)
}
