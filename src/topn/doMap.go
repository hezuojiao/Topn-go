package topn

import (
	"bufio"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
)

// Function to split a huge input file to small files that can be placed in memory.
// The relation between reader and writers like producer and consumers.
func doMap(
	inFile string,  // Name of input data.
	tmpPath string, // Path of temporary files.
	hashSize int,   // Number of hash buckets.
	bufferSize int, // Size of read buffer chan.
){

	// Create temporary files
	if err := createTmpFiles(tmpPath, hashSize); err != nil {
		log.Fatal("Unable to create temporary files", err.Error())
		return
	}

	var (
		urlChans = make([]chan string, hashSize) // Chan to receive data from reader.
		quits    = make([]chan bool, hashSize)   // Chan to inform consumers quitting.
		wg sync.WaitGroup
	)

	for i := 0; i < hashSize; i++ {
		urlChans[i] = make(chan string, bufferSize)
		quits[i] = make(chan bool)
	}

	wg.Add(1)
	go urlProducer(inFile, urlChans, quits, &wg)
	for i := 0; i < hashSize; i++ {
		wg.Add(1)
		go urlConsumer(tmpPath, i, urlChans, quits, &wg)
	}
	wg.Wait()
}


// The reader function. Read file line by line.
// when getting one url, it will be sent to corresponding chan.
func urlProducer(filePath string, urlChans []chan string, quits []chan bool, wg *sync.WaitGroup)  {
	defer wg.Done()
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		log.Fatal(err.Error())
	}

	buf := bufio.NewReader(file)
	hashSize := len(urlChans)

	for {
		line, _, err := buf.ReadLine()
		if err != nil {
			if err == io.EOF {
				//time.Sleep(60 * time.Millisecond)
				for i := 0; i < len(quits); i++ {
					quits[i] <- true
				}
				return
			}
			log.Fatal(err.Error())
		}
		url := string(line)
		chanNo := hashFunc(url) % hashSize
		urlChans[chanNo] <- url
	}
}

// The consumer.
// Listen to corresponding chan, if there are data, receive it, and append it to file.
func urlConsumer(tmpPath string, id int, urlChans []chan string, quits []chan bool, wg *sync.WaitGroup)  {
	defer wg.Done()
	fileName := tmpPath + "/" + strconv.Itoa(id) + ".url"
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	defer file.Close()
	if err != nil {
		log.Fatal(err.Error())
	}

	// Listen to corresponding chan.
	for {
		select {
		case url := <- urlChans[id]:
			file.Write([]byte(url + "\n"))
		case <- quits[id]:
			// Close chans
			close(quits[id])
			close(urlChans[id])

			// Handle rest of data
			for url := range urlChans[id] {
				file.Write([]byte(url + "\n"))
			}
			return
		default:
		}
	}
}
