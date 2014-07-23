package relpipes

import (
	"sync"
	"sort"
	"testing"
	"runtime"
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
// to the appropriate output channel.
func groupUnordered(in <-chan fooBar, res chan (chan fooBar)) {
	go func() {
		// recv the first value from the input, which will determine
		// the first group that we'll send.  If it is already closed
		// then we can close the result.
		v, ok := <- in
		if !ok {
			close(res)
			return
		}

		// figure out the subdomain for the first (and only active)
		// group.
		groupSub := proj(&v)

		// send the first group and tuple
		g := make(chan fooBar,1)
		res <- g
		g <- v

		// map containing the inactive groups
		mem := make(map[subFooBar][]fooBar)

		for v = range in {
			newSub := proj(&v)
			if newSub == groupSub {
				// it belongs to the active group
				g <- v
				continue
			}
			// otherwise it is a new tuple that has to be saved
			if sl, exists := mem[newSub]; !exists {
				// we've encountered a new group, so initialize
				// the map's value
				mem[newSub] = []fooBar{v}
			} else {
				mem[newSub] = append(sl, v)
			}
		}
		// once we've read all of the tuples, we have to go through
		// the memory and send the results
		close(g)
		for _, sl := range mem {
			g = make(chan fooBar,1)
			res <- g
			for _, v = range sl {
				g <- v
			}
			close(g)
		}
		close(res)
	}()
	return
}

// benchmark

func benchGroupUnordered(b *testing.B, tupN, fooN, barN int) {
	in := makeFooBar(tupN, fooN, barN)
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := make(chan (chan fooBar), 1)

		b.StartTimer()
		groupUnordered(fooBarChan(in, 1), res)

		emptyFooBarGroup(res)
		b.StopTimer()
	}
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

// groupUnordered takes two channels, one for input and one for
// results, and passes tuples that belong to the same group
// to the appropriate output channel.
func groupUnorderedParallel(in <-chan fooBar, res chan (chan fooBar), n int) {
	// We split the input into n goroutines by performing
	// a modulo on the input tuples, and using that to
	// determine which stripe to send the tuple to.
	stripes := make([]chan fooBar, n)

	// We have to wait for all goroutines to finish before
	// closing the results channel.
	var wg sync.WaitGroup
	wg.Add(n)
	go func() {
		wg.Wait()
		close(res)
	}()

	for i := 0; i < n; i++ {
		stripes[i] = make(chan fooBar, 1)
		go func(stripe <-chan fooBar) {
			// recv the first value from the input, which will determine
			// the first group that we'll send.  If it is already closed
			// then we can close the result.
			v, ok := <- in
			if !ok {
				wg.Done()
				return
			}

			// figure out the subdomain for the first (and only active)
			// group.
			groupSub := proj(&v)

			// send the first group and tuple
			g := make(chan fooBar,1)
			res <- g
			g <- v

			// map containing the inactive groups
			mem := make(map[subFooBar][]fooBar)

			for v = range in {
				newSub := proj(&v)
				if newSub == groupSub {
					// it belongs to the active group
					g <- v
					continue
				}
				// otherwise it is a new tuple that has to be saved
				if sl, exists := mem[newSub]; !exists {
					// we've encountered a new group, so initialize
					// the map's value
					mem[newSub] = []fooBar{v}
					} else {
						mem[newSub] = append(sl, v)
					}
				}
				// once we've read all of the tuples, we have to go through
				// the memory and send the results
				close(g)
				for _, sl := range mem {
					g = make(chan fooBar,1)
					res <- g
					for _, v = range sl {
						g <- v
					}
					close(g)
				}
				wg.Done()
			}(stripes[i])
		}

	// use modulo to send inputs to the appropriate
	// channel
	go func() {
		for v := range in {
			stripes[v.foo%n] <- v
		}
		// close intermediate channels
		for i := 0; i < n; i++ {
			close(stripes[i])
		}
	}()
	return
}

// benchmarks

func benchGroupUnorderedParallel(b *testing.B, tupN, fooN, barN int) {
	mc := runtime.GOMAXPROCS(2)
	in := makeFooBar(tupN, fooN, barN)
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := make(chan (chan fooBar), 2)

		b.StartTimer()
		groupUnorderedParallel(fooBarChan(in, 1), res, 2)

		emptyFooBarGroup(res)
		b.StopTimer()
	}
	runtime.GOMAXPROCS(mc)
}

// benchmarks for group with cardinality ~3
func BenchmarkGroupUnorderedParallel_10_3_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 10, 3, 1)
}
func BenchmarkGroupUnorderedParallel_100_3_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 100, 3, 1)
}
func BenchmarkGroupUnorderedParallel_1000_3_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 1000, 3, 1)
}
func BenchmarkGroupUnorderedParallel_10000_3_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 10000, 3, 1)
}
func BenchmarkGroupUnorderedParallel_100000_3_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 100000, 3, 1)
}
func BenchmarkGroupUnorderedParallel_1000000_3_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 1000000, 3, 1)
}

// benchmarks for group with cardinality ~1000
func BenchmarkGroupUnorderedParallel_10_1000_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 10, 1000, 1)
}
func BenchmarkGroupUnorderedParallel_100_1000_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 100, 1000, 1)
}
func BenchmarkGroupUnorderedParallel_1000_1000_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 1000, 1000, 1)
}
func BenchmarkGroupUnorderedParallel_10000_1000_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 10000, 1000, 1)
}
func BenchmarkGroupUnorderedParallel_100000_1000_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 100000, 1000, 1)
}
func BenchmarkGroupUnorderedParallel_1000000_1000_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 1000000, 1000, 1)
}

// benchmarks for group with cardinality ~100000
func BenchmarkGroupUnorderedParallel_10_100000_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 10, 100000, 1)
}
func BenchmarkGroupUnorderedParallel_100_100000_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 100, 100000, 1)
}
func BenchmarkGroupUnorderedParallel_1000_100000_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 1000, 100000, 1)
}
func BenchmarkGroupUnorderedParallel_10000_100000_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 10000, 100000, 1)
}
func BenchmarkGroupUnorderedParallel_100000_100000_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 100000, 100000, 1)
}
func BenchmarkGroupUnorderedParallel_1000000_100000_1(b *testing.B) {
	benchGroupUnorderedParallel(b, 1000000, 100000, 1)
}
