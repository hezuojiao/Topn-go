# Topn-go
Topn implemented by Go.

`Topn-go` is used to find the top `n` most frequently occurring urls from a large file. It can process huge files with 
limited memory efficiently.

## Main idea
### Preliminary implementation
To handle a large file, we can firstly split it into small files, then deal with them one by one easily. So the 
pipeline is like this:
1) Partition a large file into small files that can be placed in memory. (Map phase)
2) Merge and sort urls in small files, and find the top n candidates from each file.
Then sort all candidates and write to file. (Reduce phase)

The `Reduce` phase is a classic parallel problem. Since the sort operation on small file are independent of each other,
we run them in parallel naturally. I leverage `min-heap` to store the top n candidates from each file, then merge 
them in another min-heap and write it to file.

So the biggest problem is how to partition file efficiently in `Map` phase. In the preliminary implementation, i think
of the relation between file reader and writers like producer and consumers. The producer reads file line by line, then 
send the url to corresponding `chan` in Go-lang. The consumers use `select` to listen its chan, receive url, and append 
it to file. It looks a reasonable pattern in this scenario. However, it is inefficient due to the feature of Hard Disk 
Drive(HDD).

For each read and write to the disk, the latency highly depends on the track location. In the `Producer-Consumer` pattern,
consumers are writing url simultaneously. Every time the data is received, a write operation is triggered. Lots of time 
is wasted in seeking tracks. So the efficiency is very low.

### Optimization
Inspired by `Combiner` in map-reduce model, we can apply Combiner function that does partial merging of url before it 
write to the disk. I maintain a buffer map to to keep url pairs, and combine the url pair if they have the same address.
When map size reached a certain value, pairs in map will be flush to corresponding file.
Experimental results show that partial combining significantly reduces redundant disk IO and speeds up the MapReduce 
operation.

## Run and Test
How to run topn-go:
```
make test_topn
```


How to clean up all test data:
```
make clean
```

How to generate test data:
```
make gendata
```


MIT License