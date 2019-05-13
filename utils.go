package topn

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

type urlCount struct {
	url string // url address
	cnt int // url count
}

type minHeap struct {
	cap int // capacity
	nodes []urlCount // nodes
}

// Initialize a heap.
func newMinHeap(capacity int) *minHeap {
	nodes := make([]urlCount, capacity, capacity)
	h := &minHeap{cap: capacity, nodes: nodes, }
	return h
}

func urlCountCmp(a, b urlCount) bool {
	if a.cnt == b.cnt {
		return a.url < b.url
	}
	return a.cnt > b.cnt
}

// Push operation for min heap.
func (h *minHeap) Push(node urlCount)  {
	if !urlCountCmp(node, h.nodes[0]) {
		// do nothing.
		return
	}

	h.nodes[0] = node
	i := 0

	for {
		left, right := i * 2 + 1, i * 2 + 2
		if left >= h.cap {
			break
		}
		tmp := left
		if right < h.cap {
			// select the min node to swap
			if urlCountCmp(h.nodes[left], h.nodes[right]) {
				tmp = right
			}
		}
		if urlCountCmp(node, h.nodes[tmp]) {
			// swap
			h.nodes[i], h.nodes[tmp] = h.nodes[tmp], h.nodes[i]
			i = tmp
		} else {
			break
		}
	}
}

// return node in heap which index is idx.
func (h *minHeap) getNode(idx int) (urlCount, error) {
	if idx < 0 || idx >= h.cap || h.nodes[idx].cnt == 0 {
		return urlCount{}, fmt.Errorf("index invalid")
	}
	return h.nodes[idx], nil
}

// Write heap node to file.
func writeHeapToFile(h *minHeap, filename string) {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	defer file.Close()
	if err != nil {
		log.Fatal(err.Error())
	}
	urlCntMap := make(map[string]int)
	for i := 0; i < h.cap; i++ {
		pair, err := h.getNode(i)
		if err != nil {
			continue
		}
		urlCntMap[pair.url] = pair.cnt
	}
	genResult(filename, urlCntMap, h.cap)
}

// Append a map to file.
func writeMapToFile(m map[string]int, filename string) {
	file, buf := CreateFileAndBuf(filename)
	for k, v := range m {
		buf.Write([]byte(k + "," + strconv.Itoa(v) + "\n"))
	}
	SafeClose(file, buf)
}

// TopN returns topN urls in the urlCntMap.
func TopN(urlCntMap map[string]int, n int) ([]string, []int) {
	ucs := make([]*urlCount, 0, len(urlCntMap))
	for k, v := range urlCntMap {
		ucs = append(ucs, &urlCount{k, v})
	}
	sort.Slice(ucs, func(i, j int) bool {
		if ucs[i].cnt == ucs[j].cnt {
			return ucs[i].url < ucs[j].url
		}
		return ucs[i].cnt > ucs[j].cnt
	})
	urls := make([]string, 0, n)
	cnts := make([]int, 0, n)
	for i, u := range ucs {
		if i == n {
			break
		}
		urls = append(urls, u.url)
		cnts = append(cnts, u.cnt)
	}
	return urls, cnts
}

// CheckFile checks if these two files are same.
func CheckFile(expected, got string) (string, bool) {
	c1, err := ioutil.ReadFile(expected)
	if err != nil {
		panic(err)
	}
	c2, err := ioutil.ReadFile(got)
	if err != nil {
		panic(err)
	}
	s1 := strings.TrimSpace(string(c1))
	s2 := strings.TrimSpace(string(c2))
	if s1 == s2 {
		return "", true
	}

	errMsg := fmt.Sprintf("expected:\n%s\n, but got:\n%s\n", c1, c2)
	return errMsg, false
}

// CreateFileAndBuf opens or creates a specific file for writing.
func CreateFileAndBuf(fpath string) (*os.File, *bufio.Writer) {
	dir := path.Dir(fpath)
	os.MkdirAll(dir, 0777)
	f, err := os.OpenFile(fpath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	return f, bufio.NewWriterSize(f, 1<<20)
}

// OpenFileAndBuf opens a specific file for reading.
func OpenFileAndBuf(fpath string) (*os.File, *bufio.Reader) {
	f, err := os.OpenFile(fpath, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	return f, bufio.NewReader(f)
}

// WriteToBuf write strs to this buffer.
func WriteToBuf(buf *bufio.Writer, strs ...string) {
	for _, str := range strs {
		if _, err := buf.WriteString(str); err != nil {
			panic(err)
		}
	}
}

// SafeClose flushes this buffer and closes this file.
func SafeClose(f *os.File, buf *bufio.Writer) {
	if buf != nil {
		if err := buf.Flush(); err != nil {
			panic(err)
		}
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
}

// FileOrDirExist tests if this file or dir exist in a simple way.
func FileOrDirExist(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}
