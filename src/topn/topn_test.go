package topn

import (
	"testing"
)


const (
	inFile  	= "./data/data_0.1g.txt"
	outFile		= "./data/resTopn.txt"
	tmpPath 	= "./data/tmp"
	hashSize	= 20
	bufferSize 	= 100000
	workerNum 	= 4
	n 			= 100
)

// Test doMap function.
// To verify the correctness, we compare the line counts by
// use shell commands "wc -l tmp/*.url" and "wc -l infile.txt".
func TestDoMap(t *testing.T) {

	doMap(inFile, tmpPath, hashSize, bufferSize)
	//removeTmpFiles(tmpPath)
}

// Test topnRun function.
func TestTopnRun(t *testing.T)  {
	//doReduce(outFile, tmpPath, hashSize, workerNum, n)
	TopnRun(inFile, outFile, tmpPath, hashSize, bufferSize, workerNum, n)
}
