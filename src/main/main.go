package main

import (
	"fmt"
	"os"
	"time"
	"topn"
)

// There are some parameter need to be determined according to the file size.
var (
	tmpPath 	= "./tmp" // Path of temporary file. Default path[../
	hashSize	= 100          // Number of hash buckets. For 100GB file, can be split to 1024 files,
								// each size is about 100GB / 1024 = 100MB.
	bufferSize 	= 51200		// Size of read buffer chan. The average url length is 10B, so 512MB / 10B = 51200000 is
								// appropriate.
	workerNum 	= 10			// Number of workers to do url heap sort in parallel. 1GB / 100MB = 10.
	n 			= 100			// top-n. Default n is 100.
)



func main()  {
	if len(os.Args) == 3 {
		infile := string(os.Args[1])
		outfile := string(os.Args[2])

		t := time.Now()
		// Run.
		topn.TopnRun(infile, outfile, tmpPath, hashSize, bufferSize, workerNum, n)
		fmt.Println("Topn run elapsed :", time.Since(t))
	}
}