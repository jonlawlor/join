// mergejoin contains implementations of a merge-join in Go

package join


// TupComp represents 3 valued comparison between two tuples.  It can either be
// LT (which means that the left tuple is less than the right), EQ (which means
// that they are equal), or GT (which means that the left tuple is greater than
// the right tuple)
type TupComp int

const (
	// LT left tuple is less than the right tuple
	LT TupComp = iota

	// EQ both tuples are equal
	EQ TupComp = iota

	// GT left tuple is greater than the right tuple
	GT TupComp = iota
)

// MergeJoiner is the interface for merge join
type MergeJoiner interface {
	// MergeJoin takes two (buffered!) channels lSize and rSize which indicate
	// the size of the most recently read block from the left and right table.
	// advanceLeft and Right are used to signal when a block is should be read,
	// so that the MergeJoin can swap out any memory to begin reading the new block.
	// done is used for early cancellation.  compare is a function which compares
	// the left block's i tuple to the right block's j tuple, and returns a
	// TupComp.  Combine combines those tuples.
	//
	// This interface is ugly, and I would like to simplify it.  It might be
	// possible to change all of the channels to have type chan struct{}, and
	// remove the i and j from from combining.  However, the interface would be
	// more general at that point and I'm not sure if that is a good thing or a
	// bad one.  I'm not even sure that having an interface is useful, and it
	//  probably results in worse performance.  However, it has helped during
	// testing.
	//
	MergeJoin(lSize, rSize chan int, advanceLeft, advanceRight, done chan struct{}) (compare func() TupComp, combine func(i, j int))
}

// Join is an implementation of merge join.  A merge join requires that both
// input tables are sorted on the join attribute.  It runs in O(n+m) time where
// n and m are the length of the two inputs.  It produces results very quickly
// in comparison to other join methods as well (such as hash join).  It should
// be used whenever the inputs are already sorted.
//
//
func Join(e MergeJoiner) {
	// Construct the block size channels.  They are unbuffered, so that the block
	// producer can determine when join is finished reading from the blocks.
	lSize := make(chan int, 1)
	rSize := make(chan int, 1)
	advanceLeft := make(chan struct{}, 0)
	advanceRight := make(chan struct{}, 0)

	done := make(chan struct{}, 0) // signal to terminate execution early

	// compare and combine are always going to be closures
	compare, combine := e.MergeJoin(lSize, rSize, advanceLeft, advanceRight, done)

	// start reading from the blocks

	advanceLeft <- struct{}{}
	advanceRight <- struct{}{}
	lSz, ok := <-lSize
	if !ok {
		close(done)
		return
	}
	rSz, ok := <-rSize
	if !ok {
		close(done)
		return
	}
	for {
		switch compare() {
		case LT:
			advanceLeft <- struct{}{}
			lSz, ok = <-lSize
			if !ok {
				// close the other sync channels
				close(advanceRight)
				close(advanceLeft)
				close(done)
				return
			}
		case GT:

			advanceRight <- struct{}{}
			rSz, ok = <-rSize
			if !ok {
				// close the other sync channels
				close(advanceRight)
				close(advanceLeft)
				close(done)
				return
			}
		case EQ:

			// combine the results
			for i := 0; i < lSz; i++ {
				for j := 0; j < rSz; j++ {
					combine(i, j)
				}
			}

			// now advance the left relation
			advanceLeft <- struct{}{}
			lSz, ok = <-lSize
			if !ok {
				// close the other sync channels
				close(advanceRight)
				close(done)
				return
			}
		}
	}
}
