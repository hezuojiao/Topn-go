package topn

import (
	"testing"
)


var (
	inFile  	= "./data/data_0.01g.txt"
	outFile		= "./data/resTopn.txt"
	tmpPath 	= "./tmp"
	hashSize	= 20
	bufferSize 	= 100000
	workerNum 	= 4
	n 			= 100
)

// Test doMap function.
func TestDoMap(t *testing.T) {

	doMap(inFile, tmpPath, hashSize, bufferSize)
	//removeTmpFiles(tmpPath)
}

// Test Run function.
func TestRun(t *testing.T)  {
	//doReduce(outFile, tmpPath, hashSize, workerNum, n)
	Run(inFile, outFile, tmpPath, hashSize, bufferSize, workerNum, n)
}
