// some functions to create example data

package join

import "math/rand"

type fooBar struct {
	foo int
	bar int
}

// implement the sort interface (sorting on Bar)
type fooBarByBar []fooBar

func (e fooBarByBar) Len() int           { return len(e) }
func (e fooBarByBar) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }
func (e fooBarByBar) Less(i, j int) bool { return e[i].bar < e[j].bar }

type barBaz struct {
	bar int
	baz int
}

// implement the sort interface (sorting on Bar)
type barBazByBar []barBaz

func (e barBazByBar) Len() int           { return len(e) }
func (e barBazByBar) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }
func (e barBazByBar) Less(i, j int) bool { return e[i].bar < e[j].bar }

// result relation
type fooBarBaz struct {
	foo int
	bar int
	baz int
}

// makeFooBar makes an example unsorted []fooBar where each foo is less than
// maxFoo and each bar is less than maxBar
func makeFooBar(n, maxFoo, maxBar int) (res []fooBar) {
	// this always produces the same output
	r := rand.New(rand.NewSource(99))
	for i := 0; i < n; i++ {
		res = append(res, fooBar{r.Intn(maxFoo), r.Intn(maxBar)})
	}
	return
}

// makeBarBaz makes an example unsorted []barBaz where each bar is less than
// maxBar and each baz is less than maxBaz
func makeBarBaz(n, maxBar, maxBaz int) (res []barBaz) {
	// this always produces the same output
	r := rand.New(rand.NewSource(99))
	for i := 0; i < n; i++ {
		res = append(res, barBaz{r.Intn(maxBar), r.Intn(maxBaz)})
	}
	return
}

// functions to turn fooBar and barBaz into channels that produce their tuples

// fooBarChan turns a slice of fooBar into a channel that returns each element
func fooBarChan(table []fooBar, bufSz int) chan fooBar {
	res := make(chan fooBar, bufSz)
	go func() {
		for i := range table {
			res <- table[i]
		}
		close(res)
	}()
	return res
}

// barBazChan turns a slice of barBaz into a channel that returns each element
func barBazChan(table []barBaz, bufSz int) chan barBaz {
	res := make(chan barBaz, bufSz)
	go func() {
		for i := range table {
			res <- table[i]
		}
		close(res)

	}()
	return res
}
