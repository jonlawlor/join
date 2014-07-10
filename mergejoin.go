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

// MergeJoiner is the interface for merge join in memory
type MergeJoiner interface {
	MergeJoin(lSize, rSize chan int, updateLeft, updateRight, done chan struct{}) (compare func(i, j int) TupComp, combine func(i, j int))
}

// Join is an implementation of merge join
func Join(e MergeJoiner) {
	// Construct the block size channels.  They are unbuffered, so that the block
	// producer can determine when join is finished reading from the blocks.
	lSize := make(chan int, 0)
	rSize := make(chan int, 0)
	updateLeft := make(chan struct{}, 0)
	updateRight := make(chan struct{}, 0)

	done := make(chan struct{}, 0) // signal to terminate execution early
	compare, combine := e.MergeJoin(lSize, rSize, updateLeft, updateRight, done)

	// start reading from the blocks

	updateLeft <- struct{}{}
	updateRight <- struct{}{}
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
		switch compare(0, 0) {
		case LT:
			updateLeft <- struct{}{}
			lSz, ok = <-lSize
			if !ok {
				// close the other sync channels
				close(updateRight)
				close(updateLeft)
				close(done)
				return
			}
		case GT:

			updateRight <- struct{}{}
			rSz, ok = <-rSize
			if !ok {
				// close the other sync channels
				close(updateRight)
				close(updateLeft)
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
			updateLeft <- struct{}{}
			lSz, ok = <-lSize
			if !ok {
				// close the other sync channels
				close(updateRight)
				close(done)
				return
			}
		}
	}
}
