package topn

import (
	"bufio"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
)


// This function will create min heap for each temporary files.
// We use heap to store the most n-th frequent url in each file.
// Then use another min heap to find the top-n answer, and write to file.
func doReduce(
	outFile string,  // Name of top-n answer file.
	tmpPath string,  // Path of temporary files.
	hashSize int,    // Number of hash buckets.
	workerNum int,   // Number of workers to do url heap sort in parallel.
	n int,           // Top n.
) {
	var (
		heaps = make([]*minHeap, hashSize)     // heaps for each temporary file sort.
		workerChan = make(chan int, workerNum) // worker chan.
		wg sync.WaitGroup
	)

	for i := 0; i < hashSize; i++ {
		heaps[i] = newMinHeap(n)  // Initialize the heap' capacity with n.
	}

	for i := 0; i < workerNum; i++ {
		workerChan <- i //
	}

	for i := 0; i < hashSize; i++ {
		wg.Add(1)
		go buildHeapFromFile(i, tmpPath, heaps, workerChan, &wg) // do heap sort.
	}

	wg.Wait()
	close(workerChan)

	resHeap := newMinHeap(n)
	for i := 0; i < hashSize; i++ {
		for j := 0; j < n; j++ {
			node, err := heaps[i].getNode(j)
			if err != nil {
				continue
			}
			resHeap.Push(node)
		}
	}
	writeHeapToFile(resHeap, outFile)

	//debug.
	// remove temporary files.
	err := removeTmpFiles(tmpPath)
	if err != nil {
		log.Fatal(err.Error())
	}
}

// Function build min heap from temporary file.
// Heap contains top-n answers for each file.
func buildHeapFromFile(id int, tmpPath string, heaps []*minHeap, workerChan chan int, wg *sync.WaitGroup) {
	defer wg.Done()

	workerId := <- workerChan
	fileName := tmpPath + "/" + strconv.Itoa(id) + ".url"
	//fmt.Printf("%s\n", fileName)
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		log.Fatal(err.Error())
	}
	buf := bufio.NewReader(file)

	freqMap := make(map[string]int64)
	for {
		url, _, err := buf.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		freqMap[string(url)]++
	}


	for addr, freq := range freqMap {
		heaps[id].Push(urlPair{addr, freq})
	}

	workerChan <- workerId
}