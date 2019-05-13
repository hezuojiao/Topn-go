package topn

import (
	"hash/fnv"
	"io"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type reduceTask struct {
	path 	string
	heap 	*minHeap
	taskId	int
	wg 		sync.WaitGroup
}

type Scheduler struct {
	nWorkers	int
	wg			sync.WaitGroup
	taskCh 		chan *reduceTask
	exit 		chan struct{}
}

var singleton = &Scheduler{
	nWorkers: runtime.NumCPU(),
	taskCh:   make(chan *reduceTask),
	exit:     make(chan struct{}),
}

func GetScheduler() *Scheduler {
	return singleton
}

func (s *Scheduler) NWorkers() int { return s.nWorkers }

func (s *Scheduler) Start() {
	for i := 0; i < s.nWorkers; i++ {
		s.wg.Add(1)
		go s.worker()
	}
}

func (s *Scheduler) worker()  {
	defer s.wg.Done()
	for {
		select {
		case t := <-s.taskCh:
			file, buf := OpenFileAndBuf(t.path)
			urlCnt := make(map[string]int)
			for {
				line, _, err := buf.ReadLine()
				if err == io.EOF {
					break
				} else if err != nil {
					log.Fatal(err)
				}
				pair := strings.Split(string(line), ",")
				url := pair[0]
				cnt, _ := strconv.Atoi(pair[1])
				urlCnt[url] += cnt
			}

			for url, cnt := range urlCnt {
				t.heap.Push(urlCount{url, cnt})
			}
			SafeClose(file, nil)
			if err := os.RemoveAll(t.path); err != nil {
				log.Fatal(err)
			}
			t.wg.Done()
		case <-s.exit:
			return
		}
	}
}

func (s *Scheduler) Shutdown()  {
	close(s.exit)
	s.wg.Wait()
}

func (s *Scheduler) run(dataDir string, nReduce, n int) {
	bufferSize := (int(GB) / s.NWorkers()) / nReduce

	// map phase
	s.doMap(dataDir, bufferSize, nReduce)

	// reduce phase
	s.Start()
	s.doReduce(dataDir, nReduce, n)
	s.Shutdown()
}

func (s *Scheduler) doMap(dataDir string, bufferSize, nReduce int) {
	var (
		bufferMap = make([]map[string]int, nReduce)
		fileLock  = make([]sync.Mutex, nReduce)
		wg 			sync.WaitGroup
	)
	for i := 0; i < nReduce; i++ {
		bufferMap[i] = make(map[string]int, bufferSize)
	}

	file, buf := OpenFileAndBuf(path.Join(dataDir, "inputFile"))

	for {
		line, _, err := buf.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		url := string(line)
		fileNo := hashFunc(url) % nReduce
		bufferMap[fileNo][url]++

		if len(bufferMap[fileNo]) >= bufferSize {
			wg.Add(1)
			go func(m map[string]int, id int, wg *sync.WaitGroup) {
				defer wg.Done()
				fileLock[id].Lock()
				writeMapToFile(m, tmpFileName(dataDir, id))
				fileLock[id].Unlock()
			}(bufferMap[fileNo], fileNo, &wg)
			bufferMap[fileNo] = make(map[string]int, bufferSize)
		}
	}
	wg.Wait()
	// write rest data in map.
	for i := 0; i < nReduce; i++ {
		writeMapToFile(bufferMap[i], tmpFileName(dataDir, i))
	}
	SafeClose(file, nil)
}

func (s *Scheduler) doReduce(dataDir string, nReduce, n int) {
	var (
		heaps = make([]*minHeap, nReduce)
		tasks = make([]*reduceTask, nReduce)
	)
	for i := 0; i < nReduce; i++ {
		heaps[i] = newMinHeap(n)
		tasks[i] = &reduceTask{
			path:	tmpFileName(dataDir, i),
			heap:	heaps[i],
			taskId:	i,
		}
		tasks[i].wg.Add(1)
		go func(i int) { s.taskCh <- tasks[i] }(i)
	}

	for _, t := range tasks {
		t.wg.Wait()
	}

	resultHeap := newMinHeap(n)
	for i := 0; i < nReduce; i++ {
		for j := 0; j < n; j++ {
			node, err := heaps[i].getNode(j)
			if err != nil {
				continue
			}
			resultHeap.Push(node)
		}
	}

	writeHeapToFile(resultHeap, resultFileName(dataDir, n))
}

func hashFunc(url string) int {
	h := fnv.New32a()
	h.Write([]byte(url))
	return int(h.Sum32() & 0x7fffffff)
}

func tmpFileName(dataDir string, fileNo int) string {
	return path.Join(dataDir, "mrtmp-" + strconv.Itoa(fileNo))
}

func resultFileName(dataDir string, n int) string {
	return path.Join(dataDir, "top-" + strconv.Itoa(n) + "-result")
}
