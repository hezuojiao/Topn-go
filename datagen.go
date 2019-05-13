package topn

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path"
	"sort"
)

type DataSize int

const (
	Byte = 1
	KB   = 1 << 10
	MB   = 1 << 20
	GB   = 1 << 30
)

func (d DataSize) String() string {
	if d < KB {
		return fmt.Sprintf("%dbyte", d)
	} else if d < MB {
		return fmt.Sprintf("%dKB", d/KB)
	} else if d < GB {
		return fmt.Sprintf("%dMB", d/MB)
	}
	return fmt.Sprintf("%dGB", d/GB)
}

func genData(dataFileDir string, dataSize DataSize, n int) {
	if FileOrDirExist(dataFileDir) {
		if err := os.RemoveAll(dataFileDir); err != nil {
			log.Fatal(err)
		}
	}
	fpath := path.Join(dataFileDir, "inputFile")
	rpath := path.Join(dataFileDir, "result")
	gens := AllCaseGenFs()
	urlCount := make(map[string]int)
	fmt.Printf("generate data file, dataSize=%v", dataSize)
	for _, gen := range gens {
		tmpUrlCnt := gen(fpath, int(dataSize), len(gens))
		for k, v := range tmpUrlCnt {
			urlCount[k] += v
		}
	}
	genResult(rpath, urlCount, n)
}

// CaseGenF represents test case generate function
type CaseGenF func(filePath string, dataSize , totalCaseNum int) map[string]int

// AllCaseGenFs returns all CaseGenFs used to test.
func AllCaseGenFs() []CaseGenF {
	var gs []CaseGenF
	gs = append(gs, genUniformCases()...)
	gs = append(gs, genSingleCases()...)
	gs = append(gs, genPercentCases()...)
	return gs
}

func genUniformCases() []CaseGenF {
	cardinalities := []int{1, 100, 10000, 1000000}
	gs := make([]CaseGenF, 0, len(cardinalities))
	for _, card := range cardinalities {
		gs = append(gs, func(filePath string, dataSize, totalCaseNum int) map[string]int {
			urls, avgLen := randomNURL(card)
			eachRecords := (dataSize / totalCaseNum) / avgLen
			urlCount := make(map[string]int, len(urls))
			f, buf := CreateFileAndBuf(filePath)
			for i := 0; i < eachRecords; i++ {
				str := urls[rand.Int()%len(urls)]
				urlCount[str]++
				WriteToBuf(buf, str, "\n")
			}
			SafeClose(f, buf)
			return urlCount
		})
	}
	return gs
}

func genSingleCases() []CaseGenF {
	cardinalities := []int{10, 100}
	gs := make([]CaseGenF, 0, len(cardinalities))
	for _, card := range cardinalities {
		gs = append(gs, func(filePath string, dataSize, totalCaseNum int) map[string]int {
			urls, avgLen := randomNURL(card)
			eachRecords := (dataSize / totalCaseNum) / avgLen
			urlCount := make(map[string]int, 1)
			f, buf := CreateFileAndBuf(filePath)
			for i := 0; i < eachRecords; i++ {
				str := urls[0]
				urlCount[str]++
				WriteToBuf(buf, str, "\n")
			}
			SafeClose(f, buf)
			return urlCount
		})
	}
	return gs
}

func genPercentCases() []CaseGenF {
	ps := []struct {
		l int
		p []float64
	}{
		{11, []float64{0.9, 0.09, 0.009, 0.0009, 0.00009, 0.000009}},
		{10000, []float64{0.9, 0.09, 0.009, 0.0009, 0.00009, 0.000009}},
		{100000, []float64{0.9, 0.09, 0.009, 0.0009, 0.00009, 0.000009}},
		{10000, []float64{0.5, 0.4}},
		{10000, []float64{0.3, 0.3, 0.3}},
	}
	gs := make([]CaseGenF, 0, len(ps))
	for _, p := range ps {
		gs = append(gs, func(filePath string, dataSize, totalCaseNum int) map[string]int {

			// make up percents list
			percents := make([]float64, 0, p.l)
			percents = append(percents, p.p...)
			var sum float64
			for _, p := range p.p {
				sum += p
			}
			if sum > 1 || len(p.p) > p.l {
				panic("invalid prefix")
			}
			x := (1 - sum) / float64(p.l-len(p.p))
			for i := 0; i < p.l-len(p.p); i++ {
				percents = append(percents, x)
			}

			// generate data
			urls, avgLen := randomNURL(len(percents))
			eachRecords := (dataSize / totalCaseNum) / avgLen
			urlCount := make(map[string]int, len(urls))

			accumulate := make([]float64, len(percents) + 1)
			accumulate[0] = 0
			for i := range percents {
				accumulate[i + 1] = accumulate[i] + percents[i]
			}

			f, buf := CreateFileAndBuf(filePath)
			for i := 0; i < eachRecords; i++ {
				x := rand.Float64()
				idx := sort.SearchFloat64s(accumulate, x)
				if idx != 0 {
					idx--
				}
				str := urls[idx]
				urlCount[str]++
				WriteToBuf(buf, str, "\n")
			}
			SafeClose(f, buf)
			return urlCount
		})
	}
	return gs
}

func genResult(rpath string, urlCount map[string]int, n int) {
	us, cs := TopN(urlCount, n)
	f, buf := CreateFileAndBuf(rpath)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	SafeClose(f, buf)
}

func randomNURL(n int) ([]string, int) {
	length := 0
	urls := make([]string, 0, n)
	for i := 0; i < n; i++ {
		url := wrapLikeURL(fmt.Sprintf("%d", i))
		length += len(url)
		urls = append(urls, url)
	}
	return urls, length / len(urls)
}

var urlPrefixes = []string{
	"github.com/username/topn-go/issues",
	"github.com/username/topn-go/pull",
	"github.com/username/topn-go",
}

func wrapLikeURL(suffix string) string {
	return path.Join(urlPrefixes[rand.Intn(len(urlPrefixes))], suffix)
}
