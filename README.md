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

The implementation can be find in [here](https://github.com/hezuojiao/Topn-go/tree/f0be0184b6b122ceac95c20593029b46f48d5e6a).

### Optimization
Inspired by `Combiner` in map-reduce model, we can apply Combiner function that does partial merging of url before it 
write to the disk. I maintain a buffer map to to keep url pairs, and combine the url pair if they have the same address.
When map size reached a certain value, pairs in map will be flush to corresponding file.
Experimental results show that partial combining significantly reduces redundant disk IO and speeds up the MapReduce 
operation.

## Run and Test
### Step 1
Firstly, you need to clone the repository and do some initialization work, by running the commands below.
```
git clone https://github.com/hezuojiao/Topn-go
cd Topn-go
export "GOPATH=$PWD"  # go needs $GOPATH to be set to the project's working directory
mkdir "$GOPATH/data"  # path to store data.
```

### Step 2 (optional)
If you don't have data, you can generate it by running `script` we provided.
```$xslt
python3 script/dataGenerator.py --size 100 # file size[GB]
```
Then, you will get two files, `data_100.0g.txt` is url file and `ans_100.0g.txt` is a file corresponding top n answer.

### Step 3
Run the program.
```$xslt
go build "$GOPATH/src/main"
./main data/data_100.0g.txt data/res.txt
```
if you want run `main` with your own data, you can use the commands below.
```$xslt
./main input_data_file_path output_data_file_path
```

Then you will see messages below.
```$xslt
Map phase start...
Map phase done.
Reduce phase start...
Reduce phase done.
Topn run elapsed : 8.602890588s
```


MIT License