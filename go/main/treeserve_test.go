package main

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestMain(t *testing.T) {
	filename := "/tmp/test.dat.gz"
	fmt.Println(1, os.Stdout, os.Stderr)

	test := generateTestData(10000000, 2)

	err := writeFile(test, filename)
	if err != nil {
		t.Errorf(err.Error())
	}

	//cmd := exec.Command("go build -o ./test")
	outfile, err := os.Create("/tmp/test_out.txt")
	if err != nil {
		t.Errorf(err.Error())
	}
	defer outfile.Close()
	/*
		app := "./test"

		//arg1 := "-debug=true"
		arg0 := "-inputPath=" + filename

		cmd := exec.Command(app, arg0)
		cmd.Stdout = outfile
		cmd.Stderr = outfile
		err = cmd.Run()

		if err != nil {
			fmt.Println(err.Error())
			return
		}
	*/
}

// get the json from the url and parse to interface
func TestGetJson(t *testing.T) {
	aURL := "http://localhost:8000/tree?depth=2&path=/lustre/test"
	res, err := http.Get(aURL)

	if err != nil {
		fmt.Println(err.Error())
	}

	// if response is 200, get the last request - Head will follow redirects upto 10 times (create your own http.Client to configure this)

	fmt.Println(res.Status)
	j, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		fmt.Println(err.Error())
	}

	fmt.Println(jsonPrettyPrint(string(j)))

}

func jsonPrettyPrint(in string) string {
	var out bytes.Buffer
	err := json.Indent(&out, []byte(in), "", "\t")
	if err != nil {
		return in
	}
	return out.String()
}

// generate a test file in the same format as the treeserve input files
// each level has three files randomly selected from a set of filetypes, two directories and 2 links
// the sizes are simple to allow easy manual checking of output
/*base64 encoding of the path (to handle unprintable characters in paths)
* size of the object
* owner
* group
* atime
* mtime
* ctime
* object type (dir, normal file, symlink etc.)
* inode #
* number of hardlinks
* device id*/
func generateTestData(baseTime, levels int) (lines []string) {

	rootDir := "/lustre/test/"
	currentDir := rootDir + ""
	nextDir := ""
	line := ""
	r := rand.New(rand.NewSource(time.Now().Unix()))

	line = getRootData(rootDir, baseTime)
	lines = append(lines, line)

	for i := 1; i <= levels; i++ {

		// at each level add the files, directories and links to a tab separated string
		for j := 0; j < 3; j++ {
			line, _ = getLineData(currentDir, "f", baseTime, i, j, r)
			lines = append(lines, line)

		}
		// directories ... two, one empty and one becomes next current plus 2 links
		for j := 3; j < 5; j++ {

			line, _ = getLineData(currentDir, "l", baseTime, i, j, r)
			lines = append(lines, line)

		}
		for j := 5; j < 7; j++ {

			line, nextDir = getLineData(currentDir, "d", baseTime, i, j, r)
			//	fmt.Println("***", nextDir)
			lines = append(lines, line)

		}

		//	fmt.Println("end of level ", i, currentDir, nextDir)
		currentDir = nextDir

	}

	return

}

func writeFile(data []string, filename string) (err error) {

	f, err := os.Create(filename)
	if err != nil {
		return
	}
	defer f.Close()

	zw := gzip.NewWriter(f)

	// Setting the Header fields is optional.
	//zw.Name = filename
	//zw.Comment = "test data for treeserve"
	//zw.ModTime = time.Now()

	for i := range data {
		//	fmt.Println("line ", i)
		n, err := zw.Write([]byte(data[i]))
		if n <= 0 {
			return fmt.Errorf("Bytes written was %d", n)
		}
		if err != nil {
			return err
		}
	}

	if err := zw.Close(); err != nil {
		log.Fatal(err)
	}

	return
}

func getLineData(currentDir, nodetype string, baseTime, level, count int, r *rand.Rand) (line, nextDir string) {

	filetypes := []string{"bam", "sam", "cram", "bai", "sai", "crai", "txt", "zip", "gz", "tmp"}
	users := []string{"10", "11"}
	groups := []string{"100", "101"}

	nextDir = currentDir
	//fmt.Println("in:", nextDir)

	delim := "\t"
	term := "\n"

	n := ""
	if nodetype == "f" {
		ext := filetypes[r.Intn(len(filetypes))]
		n = currentDir + "file" + strconv.Itoa(level) + strconv.Itoa(count) + "." + ext
	} else {
		n = currentDir + "node" + strconv.Itoa(level) + strconv.Itoa(count)
	}
	if nodetype == "d" {
		nextDir = currentDir + "node" + strconv.Itoa(level) + "0/"
	}
	//fmt.Println(n)
	nextName := base64.StdEncoding.EncodeToString([]byte(n))
	nextGroup := groups[r.Intn(len(groups))]
	nextUser := users[r.Intn(len(users))]
	nextATime := strconv.Itoa(baseTime - 10000)
	nextMTime := strconv.Itoa(baseTime - 20000)
	nextCTime := strconv.Itoa(baseTime - 30000)

	line = nextName
	line += delim + "100"
	line += delim + nextUser
	line += delim + nextGroup
	line += delim + nextATime
	line += delim + nextMTime
	line += delim + nextCTime

	line += delim + nodetype

	line += delim + "0"
	line += delim + "0"
	line += delim + n
	line += term

	//	fmt.Println(line)
	//fmt.Println("return:", nextDir)

	return
}

func getRootData(rootDir string, baseTime int) (line string) {

	delim := "\t"
	term := "\n"

	nextName := base64.StdEncoding.EncodeToString([]byte(rootDir))
	nextGroup := "0"
	nextUser := "0"
	nextATime := strconv.Itoa(baseTime - 10000)
	nextMTime := strconv.Itoa(baseTime - 20000)
	nextCTime := strconv.Itoa(baseTime - 30000)

	line = nextName
	line += delim + "100"
	line += delim + nextUser
	line += delim + nextGroup
	line += delim + nextATime
	line += delim + nextMTime
	line += delim + nextCTime

	line += delim + "d"

	line += delim + "0"
	line += delim + "0"
	line += delim + "0"
	line += term

	return
}
