.PHONY: all

all: test_topn gendata clean

test_topn:
	go test -v -run=TestTopN -timeout 1h

gendata:
	go test -v -run=TestGenData

clean:
	go test -v -run=TestCleanData
