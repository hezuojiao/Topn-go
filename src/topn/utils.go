package topn

import (
	//"fmt"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
)

// Url pair struct.
type urlPair struct {
	Addr string  // url address
	Freq int64  // url frequency
}

// Min heap struct.
type minHeap struct {
	Capacity int     // capacity
	Nodes []urlPair // node
}


// Initialize a heap.
func newMinHeap(capacity int) *minHeap {
	nodes := make([]urlPair, capacity, capacity)
	h := &minHeap{Capacity: capacity, Nodes: nodes, }
	return h
}

// Push operation for min heap.
func (h *minHeap) Push(node urlPair)  {
	if node.Freq <= h.Nodes[0].Freq {
		// do nothing.
		return
	}
	h.Nodes[0] = node
	i := 0
	for {
		left, right := i * 2 + 1, i * 2 + 2
		if left >= h.Capacity {
			break
		}

		tmp := left
		if right < h.Capacity {
			// select the min node to swap
			if h.Nodes[right].Freq < h.Nodes[left].Freq {
				tmp = right
			}
		}
		if h.Nodes[tmp].Freq < node.Freq {
			// swap
			h.Nodes[i], h.Nodes[tmp] = h.Nodes[tmp], h.Nodes[i]
			i = tmp
		} else {
			break
		}
	}
}

// return node in heap which index is idx.
func (h *minHeap) getNode(idx int) (urlPair, error) {
	if idx < 0 || idx >= h.Capacity || h.Nodes[idx].Freq == 0 {
		return urlPair{}, fmt.Errorf("Index invalid")
	}
	return h.Nodes[idx], nil
}

// Write heap node to file.
func writeHeapToFile(h *minHeap, filename string) error {
	os.Remove(filename)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	defer file.Close()
	if err != nil {
		return fmt.Errorf(err.Error())
	}
	for i:= 0; i < h.Capacity; i++ {
		pair, err := h.getNode(i)
		if err != nil {
			fmt.Errorf(err.Error())
			continue
		}
		file.Write([]byte(pair.Addr + ": " + strconv.FormatInt(pair.Freq, 10) + "\n"))
	}
	return nil
}


// simple hash function copied from 6.824
func hashFunc(url string) int {
	h := fnv.New32a()
	h.Write([]byte(url))
	return int(h.Sum32() & 0x7fffffff)
}


// Function to create / remove temporary files.
func createTmpFiles(tmpPath string, hashSize int) error {
	// remove old temporary files
	if err := removeTmpFiles(tmpPath); err != nil {
		return fmt.Errorf(err.Error())
	}

	if err := os.Mkdir(tmpPath, os.ModePerm); err != nil {
		return fmt.Errorf(err.Error())
	}
	for i := 0; i < hashSize; i++ {
		_, err := os.Create(tmpPath +"/" + strconv.Itoa(i) + ".url")
		if err != nil {
			return fmt.Errorf(err.Error())
		}
	}
	return nil
}

func removeTmpFiles(tmpPath string) error {
	if err := os.RemoveAll(tmpPath); err != nil {
		return fmt.Errorf(err.Error())
	}
	return nil
}

func writeMapToFile(m map[string]int64, filename string) error {

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	defer file.Close()
	if err != nil {
		return fmt.Errorf(err.Error())
	}

	for k, v := range m {
		file.Write([]byte(k + "," + strconv.FormatInt(v, 10) + "\n"))
	}
	return nil
}