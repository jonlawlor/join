package relpipes

import "sync"

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
func groupOrdered(in <-chan fooBar, res chan chan fooBar, p predicate, n int) {
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

// groupUnordered takes two channels, one for input and one for
// results, and passes tuples that belong to the same group
// to the appropriate output channel. It also takes n, which is
// the buffer size that should be used for the inner result channels.
func groupUnordered(in <-chan fooBar, res chan chan fooBar, n int) {

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
func makeGroup(in chan fooBar, res chan chan fooBar, wg *sync.WaitGroup, n int) {

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
