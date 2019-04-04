package topn

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

func TestMinHeap(t *testing.T) {
	// Test NewMinHeap
	passFlag := true
	heap := newMinHeap(10)
	if heap.Capacity != 10 {
		passFlag = false
	}
	for i := 0; i < 10 ; i++ {
		_, err := heap.getNode(i)
		if err == nil {
			passFlag = false
		}
	}

	if passFlag {
		t.Log("NewMinHeap Pass!")
	} else {
		t.Error("MewMinHeap Failed!")
	}

	// Test Push
	passFlag = true
	for i := 0; i < 20; i++ {
		heap.Push(urlPair{strconv.Itoa(i), int64(i)})
	}
	for i := 0; i < 20; i++ {
		pair, err := heap.getNode(i)
		if (i < 10 && pair.Freq < 10) || (i >= 10 && err == nil) {
			passFlag = false
		}
	}
	heap.Push(urlPair{"google.com", int64(15)})
	heap.Push(urlPair{"pingcap.com", int64(100)})
	heap.Push(urlPair{"1.1.1.1", int64(1)})


	for i := 0; i < 10; i++ {
		pair, _ := heap.getNode(i)
		if pair.Freq < 12 {
			passFlag = false
		}
	}

	if passFlag {
		t.Log("Push Pass!")
	} else {
		t.Error("Push Failed!")
	}

	err := writeHeapToFile(heap, "./test.txt")
	if err == nil {
		t.Log("Writer Pass!")
	} else {
		t.Error("Writer Failed!")
	}

	os.Remove("./test.txt")
}

func TestHashFunc(t *testing.T) {
	fmt.Printf("HashCode : %d\n", hashFunc("google.com") % 10)
	fmt.Printf("HashCode : %d\n", hashFunc("pingcap.com") % 10)
	for i := 0; i < 20; i++ {
		fmt.Printf("HashCode : %d\n", hashFunc(strconv.Itoa(i + 10000) + ".com") % 10)
	}
}


func TestTmpFilesOp(t *testing.T) {
	tmpPath := "./tmp"
	if err := createTmpFiles(tmpPath, 10); err != nil {
		t.Error("CreateTmpFiles Failed!")
	}
	if err := removeTmpFiles(tmpPath); err != nil {
		t.Error("RemoveTmpFiles Failed!")
	}
	t.Log("TmpFilesOP Pass!")

}