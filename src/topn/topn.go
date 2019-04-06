package topn

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Function combines map and reduce.
func Run(
	inFile string,
	outFile string,
	tmpFile string,
	hashSize int,
	bufferSize int,
	workerNum int,
	n int,
) {

	// Map
	fmt.Println("Map phase start...")
	doMap(inFile, tmpFile, hashSize, bufferSize)
	fmt.Println("Map phase done.")

	// Reduce
	fmt.Println("Reduce phase start...")
	doReduce(outFile, tmpFile, hashSize, workerNum, n)
	fmt.Println("Reduce phase done.")

}


// Function to split a huge input file to small files that can be placed in memory.
// We use a buffer map to store url pairs, and combine the url pair when they have
// the same address. It is useful for program optimization.
func doMap(
	inFile string,  // Name of input data.
	tmpPath string, // Path of temporary files.
	hashSize int,   // Number of hash buckets.
	bufferSize int, // Size of buffer map.
){

	// Create temporary files
	if err := createTmpFiles(tmpPath, hashSize); err != nil {
		log.Fatal("Unable to create temporary files", err.Error())
		return
	}

	// Read buffer map
	var bufferMap = make([]map[string]int64, hashSize)
	for i := 0; i < hashSize; i++ {
		bufferMap[i] = make(map[string]int64)
	}

	// Read input file.
	file, err := os.Open(inFile)
	defer file.Close()
	if err != nil {
		log.Fatal(err.Error())
	}

	buf := bufio.NewReader(file)
	for {
		line, _, err := buf.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err.Error())
		}

		url := string(line)
		fileNo := hashFunc(url) % hashSize
		bufferMap[fileNo][url]++

		// If map'size reached buffer size, map'data will be written to temporary file,
		// then clear the corresponding map.
		if len(bufferMap[fileNo]) >= bufferSize {
			writeMapToFile(bufferMap[fileNo],tmpPath + "/" + strconv.Itoa(fileNo) + ".url")
			bufferMap[fileNo] = make(map[string]int64)
		}
	}

	// write rest data in map.
	for i := 0; i < hashSize; i++ {
		writeMapToFile(bufferMap[i], tmpPath + "/" + strconv.Itoa(i) + ".url")
	}
}



// This function will create min heap for each temporary files.
// We use min heap to store the most n-th frequent url in each file.
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
		line, _, err := buf.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		pair := strings.Split(string(line), ",")
		url  := pair[0]
		freq, _ := strconv.Atoi(pair[1])
		freqMap[url] += int64(freq)
	}


	for addr, freq := range freqMap {
		heaps[id].Push(urlPair{addr, freq})
	}

	workerChan <- workerId
}