package topn

import (
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"testing"
	"time"
)

const (
	dataDir 			= "/tmp/topn_data"
	dataSize DataSize 	= 1 * GB
	nReduce 			= 10
	n					= 100
)

func dataPrefix(ds DataSize) string {
	return path.Join(dataDir, fmt.Sprintf("inputfile-%s", ds))
}

func TestGenData(t *testing.T) {
	prefix := dataPrefix(dataSize)
	genData(prefix, dataSize, n)
}

func TestCleanData(t *testing.T) {
	if err := os.RemoveAll(dataDir); err != nil {
		log.Fatal(err)
	}
}

func TestTopN(t *testing.T)  {
	scheduler := GetScheduler()
	prefix := dataPrefix(dataSize)
	if !FileOrDirExist(prefix) {
		genData(prefix, dataSize, n)
	}
	runtime.GC()

	begin := time.Now()
	scheduler.run(prefix, nReduce, n)
	cost := time.Since(begin)

	// check result
	tpath := resultFileName(prefix, n)
	rpath := path.Join(prefix, "result")
	if !FileOrDirExist(tpath) {
		panic("top-n result file doesn't exist")
	}

	if errMsg, ok := CheckFile(rpath, tpath); !ok {
		t.Fatalf("FAIL, dataSize=%v, cost=%v\n%v\n",  dataSize, cost, errMsg)
	} else {
		fmt.Printf("PASS, dataSize=%v, cost=%v\n", dataSize, cost)
	}
}
