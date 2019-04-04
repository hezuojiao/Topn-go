package topn

// Function combines map and reduce.
func TopnRun(
	inFile string,
	outFile string,
	tmpFile string,
	hashSize int,
	bufferSize int,
	workerNum int,
	n int,
) {

	// Map
	doMap(inFile, tmpFile, hashSize, bufferSize)

	// Reduce
	doReduce(outFile, tmpFile, hashSize, workerNum, n)

}
