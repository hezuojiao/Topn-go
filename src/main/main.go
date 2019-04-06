package main

import (
	"fmt"
	"os"
	"time"
	"topn"
)

// Some parameter can be tune.
var (
	tmpPath 	= "./tmp"	// Path of temporary file. Default path[./tmp]
	hashSize	= 500       // Number of hash buckets. For 100GB file, can be split to 500 files,
							// each file size is about 100GB / 500 = 200MB. Actually, we do combine operation before
							// write map to file. So, each file size is less than 50MB.
	bufferSize 	= 100000	// Size of read buffer chan. The average url length is 100B, so 1GB / (50B * hashSize)
							// = 20000 is appropriate.
	workerNum 	= 10		// Number of workers to do url heap sort in parallel.
	n 			= 100		// top-n. Default n is 100.
)


func main()  {
	// Command should be "./main infile outfile".
	if len(os.Args) == 3 {
		infile := string(os.Args[1])
		outfile := string(os.Args[2])

		t := time.Now()
		// Run.
		topn.Run(infile, outfile, tmpPath, hashSize, bufferSize, workerNum, n)
		fmt.Println("Topn run elapsed :", time.Since(t))
	}
}