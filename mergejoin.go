// mergejoin contains implementations of a merge-join in Go

package join

// implementation without concurrency, where everything is already in mem

import "fmt"

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

// MemoryMergeJoiner is the interface for merge join in memory
type MemoryMergeJoiner interface {
	LeftLen() int  // length of the left table
	RightLen() int // length of the right table

	LeftEQ(i, j int) bool  // compare the values in the left table[i] to table[j]
	RightEQ(i, j int) bool // compare the values in the right table[i] to table[j]

	LeftRightComp(i, j int) TupComp // tells if the tuple in the left table[i] is LT, EQ, or GT the right table[j]

	Combine(i, j int) // combines the two tuples and has a side effect to make that available to the join caller
}

func advance(start int, tableLen int, Comp func(i int, j int) bool) (end int) {
	// advance returns the end of the block that begins with start, and where each
	// tuple has the same attributes as the previous one, subject to the
	// comparison function Comp.
	end = start + 1

	for end <= tableLen && Comp(start, end) {
		end++
	}
	return
}

// MergeJoin is an implementation of merge join for an in memory relation
func MergeJoin(e MemoryMergeJoiner) {
	lLen := e.LeftLen()
	if lLen == 0 {
		return // there is nothing to join
	}
	rLen := e.RightLen()

	if rLen == 0 {
		return // there is nothing to join
	}

	var (
		lStart int // [start, end) of the slice that determines the left block
		rStart int // [start, end) of the slice that determines the right block
	)

	lEnd := advance(lStart, lLen, e.LeftEQ)
	rEnd := advance(rStart, rLen, e.RightEQ)

	for {
		c := e.LeftRightComp(lStart, rStart)
		switch c {
		case LT:
			// left block is less than the right block, so advance the left
			if lEnd == lLen {
				return
			}
			lStart = lEnd
			lEnd = advance(lStart, lLen, e.LeftEQ)

		case EQ:
			// blocks match, so combine their tuples
			for i := lStart; i < lEnd; i++ {
				for j := rStart; j < rEnd; j++ {
					e.Combine(i, j)
				}
			}

		case GT:
			// left block is greater than the right block, so advance the right
			if rEnd == rEnd {
				return
			}

			rStart = rEnd
			rEnd = advance(rStart, rLen, e.RightEQ)

		}
	}
}

type foobar struct {
	foo int
	bar int
}

type barbaz struct {
	bar int
	baz int
}

type foobarbaz struct {
	foo int
	bar int
	baz int
}

type joinExpr struct {
	left  []foobar
	right []barbaz
	res   []foobarbaz
}

func (e *joinExpr) LeftLen() int  { return len(e.left) }
func (e *joinExpr) RightLen() int { return len(e.right) }

func (e *joinExpr) LeftEQ(i, j int) bool  { return e.left[i].bar == e.left[j].bar }
func (e *joinExpr) RightEQ(i, j int) bool { return e.right[i].bar == e.right[j].bar }
func (e *joinExpr) LRLess(i, j int) bool  { return e.left[i].bar < e.right[j].bar }
func (e *joinExpr) LREqual(i, j int) bool { return e.left[i].bar == e.right[j].bar }

func (e *joinExpr) Combine(i, j int) {
	e.res = append(e.res, foobarbaz{e.left[i].foo, e.left[i].bar, e.right[i].baz})
}

func main() {

	l := []foobar{
		{1, 1},
		{1, 2},
		{2, 3},
		{3, 3},
		{1, 4},
		{2, 5},
	}

	r := []barbaz{
		{1, 1},
		{1, 2},
		{2, 2},
		{3, 1},
		{3, 4},
		{4, 4},
	}

	j := &joinExpr{left: l, right: r, res: []foobarbaz{}}

	fmt.Println("Hello, playground", j)
}
