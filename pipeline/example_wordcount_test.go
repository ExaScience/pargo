package pipeline_test

import (
	"bufio"
	"fmt"
	"io"
	"runtime"
	"strings"

	"github.com/exascience/pargo/pipeline"
	"github.com/exascience/pargo/sort"
	"github.com/exascience/pargo/sync"
)

type Word string

func (w Word) Hash() (hash uint64) {
	// DJBX33A
	hash = 5381
	for _, b := range w {
		hash = ((hash << 5) + hash) + uint64(b)
	}
	return
}

func WordCount(r io.Reader) *sync.Map {
	result := sync.NewMap(16 * runtime.GOMAXPROCS(0))
	scanner := pipeline.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var p pipeline.Pipeline
	p.Source(scanner)
	p.Add(
		pipeline.Par(pipeline.Receive(
			func(_ int, data interface{}) interface{} {
				var uniqueWords []string
				for _, s := range data.([]string) {
					newValue, _ := result.Modify(Word(s), func(value interface{}, ok bool) (newValue interface{}, store bool) {
						if ok {
							newValue = value.(int) + 1
						} else {
							newValue = 1
						}
						store = true
						return
					})
					if newValue.(int) == 1 {
						uniqueWords = append(uniqueWords, s)
					}
				}
				return uniqueWords
			},
		)),
		pipeline.Ord(pipeline.ReceiveAndFinalize(
			func(_ int, data interface{}) interface{} {
				// print unique words as encountered first at the source
				for _, s := range data.([]string) {
					fmt.Print(s, " ")
				}
				return data
			},
			func() { fmt.Println(".") },
		)),
	)
	p.Run()
	return result
}

func Example_wordCount() {
	r := strings.NewReader("The big black bug bit the big black bear but the big black bear bit the big black bug back")
	counts := WordCount(r)
	words := make(sort.StringSlice, 0)
	counts.Range(func(key, _ interface{}) bool {
		words = append(words, string(key.(Word)))
		return true
	})
	sort.Sort(words)
	for _, word := range words {
		count, _ := counts.Load(Word(word))
		fmt.Println(word, count.(int))
	}

	// Output:
	// The big black bug bit the bear but back .
	// The 1
	// back 1
	// bear 2
	// big 4
	// bit 2
	// black 4
	// bug 2
	// but 1
	// the 3
}
