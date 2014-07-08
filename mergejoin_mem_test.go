// mergejoin_mem_test contains tests for merge join when the data structures are
// all held in memory.


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
