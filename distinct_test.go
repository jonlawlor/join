package relpipes

import "sync"

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
