package treeserve

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/handlers"
)

// Aggregate values are converted from bytes and seconds to Tebibytes and year on output
const secondsInYear = 60 * 60 * 24 * 365
const costPerTibYear = 150.0
const bytesInTebibytes = 1024 * 1024 * 1024 * 1024

var sizeConv = big.NewFloat(bytesInTebibytes)
var timeConv = big.NewFloat(secondsInYear / costPerTibYear)

// The files are made using getent group and getent passwd
// the maps use the files to map GID and UID to the names
var userMap map[string]string
var groupMap map[string]string
var groupfile = "/home/sjc/testdata/g"
var userfile = "/home/sjc/testdata/p"

// recursive tree structure, non binary tree, data at each level
type dirTree struct {
	key       *Md5Key    // not output in json
	ChildDirs []*dirTree `json:"child_dirs,omitempty"`
	Data      webAggData `json:"data,omitempty"`
	Name      string     `json:"name"`
	Path      string     `json:"path"`
}

// v2 of the original C++ added this
type fullTree struct {
	Date string  `json:"date"`
	Tree dirTree `json:"tree"`
}

// The aggregate data for a node (groups, users and tags are dynamic so map not struct)
// map levels are group/user/tag
type webAggData struct {
	Ctime map[string]map[string]map[string]string `json:"ctime"`
	Count map[string]map[string]map[string]string `json:"count"`
	Atime map[string]map[string]map[string]string `json:"atime"`
	Mtime map[string]map[string]map[string]string `json:"mtime"`
	Size  map[string]map[string]map[string]string `json:"size"`
}

// Aggregates is one set of cost values, which will apply to one set of categories
type Aggregates struct {
	Group string `json:"group"`
	User  string `json:"user"`
	Tag   string `json:"tag"`

	Size         *big.Int `json:"size"`
	Count        *big.Int `json:"count"`
	CreationCost *big.Int `json:"ccost"`
	AccessCost   *big.Int `json:"acost"`
	ModifyCost   *big.Int `json:"mcost"`
}

//Webserver listens for requests of the form
// xxxxx/maxdepth=1&path=/lustre/scratch115/projects
// and returns nodes in json
func (ts *TreeServe) Webserver(groupFile, userFile string, port string) {

	groupMap, userMap = buildUserGroupMaps(groupFile, userFile)
	logInfo("Built Maps")

	//port := "8000"
	//ip := "127.0.0.1:"
	http.HandleFunc("/", hello)
	http.HandleFunc("/tree", ts.tree)
	http.HandleFunc("/raw", ts.raw)
	logInfo("Set handlers")
	//http.ListenAndServe(":"+port, nil)
	err := http.ListenAndServe(":"+port, handlers.LoggingHandler(os.Stdout, http.DefaultServeMux))
	//err := http.ListenAndServe(":9000", nil)
	//	logInfo(err.Error())
	LogError(err)

	logInfo(fmt.Sprintf("Webserver started on port %s", port))

}

func hello(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "Listening out for requests")
}

// tree handles requests of the form <url>/api/v2?maxdepth=1&path=/lustre/scratch115/projects
// and returns the data in json format, or a 404 error
func (ts *TreeServe) tree(w http.ResponseWriter, r *http.Request) {

	path, depth := queryParameters(r)
	logInfo(fmt.Sprintf("Getting path %s, depth %d", path, depth))

	nodeKey := ts.getPathKey(path)
	logInfo("gotPathKey")

	j := []byte{}

	logInfo("Calling build tree")
	t, err := ts.buildTree(nodeKey, 0, depth)

	if err == nil {
		ft := fullTree{Date: time.Now().String(), Tree: t}
		j, err = json.Marshal(ft)

	}
	if err != nil {

		LogError(err)

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, "Could not retrieve tree")

	} else {
		w.Header().Set("Content-Type", "application/json; charset=utf-8") // normal header
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.WriteHeader(http.StatusOK)

		io.WriteString(w, string(j))
	}

}

// buildTree does a recursive tree build passing in level and depth so it will stop appropriately
// Returning a few levels from the chosen directory means that recursion is not too expensive here.
func (ts *TreeServe) buildTree(rootKey *Md5Key, level int, depth int) (t dirTree, err error) {
	logInfo(fmt.Sprintf("buildTree level %d depth %d", level, depth))

	if level > depth {
		return
	}

	temp, err := ts.GetTreeNode(rootKey)

	if err != nil {
		LogError(err)
		return
	}
	logInfo("got treenode " + temp.Name)

	t = dirTree{key: rootKey}
	t.Path = strings.TrimSuffix(temp.Name, "/")
	_, file := filepath.Split(t.Path)
	t.Name = file

	logInfo("About to retrieve aggregates")
	stats, err := ts.retrieveAggregates(rootKey)
	if err != nil {
		LogError(err)
		return
	}
	if len(stats) == 0 {
		logInfo(" Blank stats at " + t.Path)
	}
	//logInfo("About to organise aggregates")
	a, err := organiseAggregates(stats)
	if err != nil {
		LogError(err)
		return
	}
	t.Data = a
	//logInfo("About to retrieve children")
	child, err := ts.children(rootKey)
	if err != nil {
		LogError(err)
		return
	}

	// the tree of local file data *.*
	immediateChildStats := []*AggregateStats{}
	// include this directory
	aa, err := ts.CalculateAggregateStats(rootKey)
	LogError(err)
	immediateChildStats = append(immediateChildStats, aa)

	// recursion and build file summary
	for j := range child {
		temp, err := ts.GetTreeNode(child[j])
		LogError(err)

		if temp.Stats.FileType != 'f' {

			t2, err := ts.buildTree(child[j], level+1, depth) /// recursion ...make next level tree for each child
			if err != nil {
				LogError(err)
			}
			if t2.Path != "" {
				t.addChild(&t2)
			}

		} else {
			// collect files for the *.*, and not at lowest level
			if level < depth {
				a, err := ts.CalculateAggregateStats(child[j])
				LogError(err)
				immediateChildStats = append(immediateChildStats, a)
			}
		}

	}
	if level < depth { // only files in the *.*, and not at lowest level
		immediateChildStats, _ = combineAggregateStats(immediateChildStats)
		summaryTree, ok := getSummaryTree(t.Path, immediateChildStats)
		if ok {
			t.addChild(&summaryTree)
		}
	}

	return
}

// getSummaryTree makes an entry with path *.* that contains stats for the node itself and it's children.
// No *.* is added for empty directories
func getSummaryTree(path string, imm []*AggregateStats) (t dirTree, ok bool) {
	ok = true
	agg := AggregatesFromAggregateStats(imm)
	if len(agg) > 0 { // don't add *.* if the directory has no contents
		w, err := organiseAggregates(agg)
		LogError(err)
		t = dirTree{Name: "*.*", Path: path + "/*.*", Data: w}
	} else {
		ok = false
	}

	return
}

// build up the set of stats of grandchildren of a node by appending the data for children of a child
func (ts *TreeServe) appendChildStats(g []Aggregates, key *Md5Key) (newg []Aggregates) {
	copy(newg, g)
	// for the tree of local file data combine grandchild aggregates
	grandChildren, err := ts.children(key)
	if err != nil {
		LogError(err)
	}

	for j2 := range grandChildren {
		next, err := ts.retrieveAggregates(grandChildren[j2])
		LogError(err)
		if len(next) != 0 {
			newg = append(newg, next...)
		}
	}
	return
}

/// organiseAggregates returns a three level map to to get the correct json from the array of Aggregate stats
// because some of the keys are dynamic (users, groups and tags) so can't be just a struct.
/*  The idea is:
a.Atime[g][u][t] = 3
a.Ctime[g][u][t] = 4
a.Mtime[g][u][t] = 5
a.Count[g][u][t] = 6
a.Size[g][u][t] = 7
// but can't add to an empty map so work out which map levels exist
*/
func organiseAggregates(stats []Aggregates) (a webAggData, err error) {

	errorCount := 0
	for i := range stats {
		statsItem := stats[i]

		//fmt.Println(statsItem.Group, statsItem.User, statsItem.Tag)

		if statsItem.Count.Cmp(big.NewInt(0)) == 0 {
			errorCount++
			//	fmt.Println(statsItem.Count, statsItem.Size, statsItem)
			continue // don't add empty sets of aggregates
		}

		g := lookupGID(statsItem.Group)
		u := lookupUID(statsItem.User)
		tag := statsItem.Tag

		//Access Cost
		b := statsItem.AccessCost
		updateMap(true, &a.Atime, b, g, u, tag)

		// Modify Cost"count ", ag.Count,
		b = statsItem.ModifyCost
		updateMap(true, &a.Mtime, b, g, u, tag)

		// Create Cost
		b = statsItem.CreationCost
		updateMap(true, &a.Ctime, b, g, u, tag)

		// Size
		b = statsItem.Size
		updateMap(false, &a.Size, b, g, u, tag)

		// Count
		b = statsItem.Count
		//fmt.Println("Adding:", b.Text(10), g, u, tag)
		updateMap(false, &a.Count, b, g, u, tag)

	}

	if errorCount > 0 {
		err = fmt.Errorf("%d examples of zero counts in organiseAggregates", errorCount)
	}
	return
}

//--------------------------------------------------------------

// addChild adds a child dirTree to a dirTree
func (t *dirTree) addChild(child *dirTree) {
	t.ChildDirs = append(t.ChildDirs, child)
}

// get path and depth from request, or use defaults
// /lustre/scratch115/realdata/mdt0 is an example
func queryParameters(r *http.Request) (path string, depth int) {
	url := r.URL
	vals := url.Query()

	//defaults
	path = "/lustre"
	depth = 2
	var err error

	if val, ok := vals["path"]; ok {
		path = val[0]
	}
	if val, ok := vals["depth"]; ok {
		depth, err = strconv.Atoi(val[0])
		LogError(err)
	}

	path = filepath.Clean(path)
	return

}

// updateMap takes a new set of category tags and a value and updates the three level map (this is needed so that json.Marshal outputs the correct format)
func updateMap(scaleMap bool, theMap *map[string]map[string]map[string]string, theValue *big.Int, g string, u string, tag string) {
	if len(*theMap) == 0 {

		mt := make(map[string]string)
		if scaleMap {
			mt[tag] = convertstatsForOutput(theValue)
		} else {
			mt[tag] = theValue.Text(10)
		}

		mu := make(map[string]map[string]string)
		mu[u] = mt
		mg := make(map[string]map[string]map[string]string)
		mg[g] = mu

		*theMap = mg

	} else if _, ok := (*theMap)[g]; !ok { // g doesn't exist in map

		mt := make(map[string]string)
		if scaleMap {
			mt[tag] = convertstatsForOutput(theValue)
		} else {
			mt[tag] = theValue.Text(10)
		}

		mu := make(map[string]map[string]string)
		mu[u] = mt
		(*theMap)[g] = mu

	} else if _, ok := (*theMap)[g]; ok { // g exists in map
		if _, ok2 := (*theMap)[g][u]; !ok2 { // but u doesn't}

			mt := make(map[string]string)
			if scaleMap {
				mt[tag] = convertstatsForOutput(theValue)
			} else {
				mt[tag] = theValue.Text(10)
			}

			(*theMap)[g][u] = mt
		} else {
			// key tag does not exist in map

			if scaleMap {

				(*theMap)[g][u][tag] = convertstatsForOutput(theValue)
			} else {
				(*theMap)[g][u][tag] = theValue.Text(10)
			}

		}

	}

}

// retrieveAggregates takes a node key and returns the array of stats associated with it
// Used for output after the database has been built up. Returns an error if the node has no stats associated
// which may be the case for the parent of the root node but nothing else
func (ts *TreeServe) retrieveAggregates(nodekey *Md5Key) (data []Aggregates, err error) {
	logInfo("")
	data = []Aggregates{}
	// all keys mapping this node to sets of aggregate stats
	aggregateKeys, err := ts.StatMappingsDB.GetKeySet(nodekey)
	if err != nil {
		LogError(err)
		return
	}
	if len(aggregateKeys) == 0 {
		LogError(fmt.Errorf("No stats found for node"))
		return
	}

	logInfo(fmt.Sprintf("got %d aggregate Keys", len(aggregateKeys)))
	for i := range aggregateKeys {
		logInfo(fmt.Sprintf("loop %d key %b", i, aggregateKeys[i]))
		x := aggregateKeys[i].(*Md5Key)
		logInfo(fmt.Sprintf("loop %d key %b", i, x))

		vals, err := ts.AggregateStatsDB.Get(x)

		nums := vals.(*AggregateNums)
		temp, err := ts.StatMappingDB.Get(x)
		s := temp.(*StatMapping)
		LogError(err)

		ag := Aggregates{}

		ag.Group = s.Group
		ag.User = s.User
		ag.Tag = s.Tag

		ag.Size = nums.Size
		ag.Count = nums.Count
		ag.AccessCost = nums.AccessCost
		ag.ModifyCost = nums.ModifyCost
		ag.CreationCost = nums.CreateCost
		logInfo(fmt.Sprintf("****%+v", ag))
		data = append(data, ag)

	}
	return
}

// error logging with file and line number
func LogError(err error) {
	if err == nil {
		return
	}
	buf := os.Stderr
	_, f, l, _ := runtime.Caller(1)
	logger := log.New(buf, "ERROR: "+f+" "+strconv.Itoa(l)+" ", log.LstdFlags)
	logger.Println(err)
}

func logInfo(str string) {
	/*
		buf := os.Stdout
		_, f, l, _ := runtime.Caller(1)
		logger := log.New(buf, "INFO: "+f+" "+strconv.Itoa(l)+" ", log.LstdFlags)
		logger.Println(str) */
}

// The original version had times in years and sizes in tebibytes (2^40 bytes)
// This one uses Big package to keep sizes in bytes and times in seconds
// The conversion factor was #150 per tebibyte year. If the value is
// below threshold, make it zero
func convertstatsForOutput(b *big.Int) (s string) {
	//threshold := float64(0.00001)

	b2 := big.NewFloat(0).SetInt(b)

	ans := big.NewFloat(0)

	ans.Quo(b2, sizeConv)
	ans.Quo(ans, timeConv)

	s = ans.Text('e', 6)

	return
}

// addAggregates adds two sets of aggregate data after checking that the statmappings match
func addAggregates(a, b Aggregates) (c Aggregates, err error) {

	if a.Group != b.Group {
		err = fmt.Errorf("addAggregates ... groups don't match (%s, %s)", a.Group, b.Group)
	} else {
		c.Group = a.Group
	}
	if a.User != b.User {
		err = fmt.Errorf("addAggregates ... users don't match (%s, %s)", a.User, b.User)
	} else {
		c.User = a.User
	}
	if a.Tag != b.Tag {
		err = fmt.Errorf("addAggregates ... tags don't match (%s, %s)", a.Tag, b.Tag)
	} else {
		c.Tag = a.Tag
	}

	temp := big.NewInt(0)

	temp.Add(a.Count, b.Count)
	c.Count = temp

	temp2 := big.NewInt(0)
	temp2.Add(a.Size, b.Size)
	c.Size = temp2

	temp3 := big.NewInt(0)
	temp3.Add(a.AccessCost, b.AccessCost)
	c.AccessCost = temp3

	temp4 := big.NewInt(0)
	temp4.Add(a.CreationCost, b.CreationCost)
	c.CreationCost = temp4

	temp5 := big.NewInt(0)
	temp5.Add(a.ModifyCost, b.ModifyCost)
	c.ModifyCost = temp5
	return

}

// subtract aggregates subtracts one set of aggregates from another after checking that the statmappings match
/*
func subtractAggregates(a, b Aggregates) (c Aggregates, err error) {

	if a.Group != b.Group {
		err = fmt.Errorf("addAggregates ... groups don't match (%s, %s)", a.Group, b.Group)
	} else {
		c.Group = a.Group
	}
	if a.User != b.User {
		err = fmt.Errorf("addAggregates ... users don't match (%s, %s)", a.User, b.User)
	} else {
		c.User = a.User
	}
	if a.Tag != b.Tag {
		err = fmt.Errorf("addAggregates ... tags don't match (%s, %s)", a.Tag, b.Tag)
	} else {
		c.Tag = a.Tag
	}

	temp := NewBigint()

	temp.Subtract(a.Count, b.Count)
	c.Count = temp

	temp2 := NewBigint()
	temp2.Add(a.Size, b.Size)
	c.Size = temp2

	temp3 := NewBigint()
	temp3.Subtract(a.AccessCost, b.AccessCost)
	c.AccessCost = temp3

	temp4 := NewBigint()
	temp4.Subtract(a.CreationCost, b.CreationCost)
	c.CreationCost = temp4

	temp5 := NewBigint()
	temp5.Subtract(a.ModifyCost, b.ModifyCost)
	c.ModifyCost = temp5
	return

}*/

// mapFromAggregates returns a map with key Group:User:Tag and aggregate values added for same key
/*
func mapFromAggregateArray(in []Aggregates) (out map[string]Aggregates) {
	out = make(map[string]Aggregates)
	for i := range in {
		key := in[i].Group + ":" + in[i].User + ":" + in[i].Tag
		if val, ok := out[key]; !ok {
			out[key] = in[i]
		} else {
			val2, err := addAggregates(val, in[i])
			if err != nil {
				LogError(err)
			}
			out[key] = val2
		}
	}
	return
}*/

// arrayFromAggregateMap returns an array of the values in the map
/*
func arrayFromAggregateMap(in map[string]Aggregates) (out []Aggregates) {

	for _, v := range in {
		out = append(out, v)
	}
	return
}*/

// subtractAggregateMap subtracts a child map from a parent map and returns an error if a child key
// is not present in the parent. If count becomes zero the entry is deleted
/*
func subtractAggregateMap(parent, child map[string]Aggregates) (out map[string]Aggregates, err error) {
	out = parent
	for k, v := range child {
		//fmt.Println("Child ", k, v)
		var val Aggregates
		var ok bool
		if val, ok = out[k]; !ok {
			err = fmt.Errorf("Child key %s not found in parent map ", k)
			return
		}
		//fmt.Println("Parent ", val, ok)
		temp, err := subtractAggregates(val, v)
		if err != nil {
			return out, err
		}
		//fmt.Println("subtracted ", temp.Count.Text(10))
		if temp.Count.isNegative() { // includes zero
			//fmt.Println("here")
			delete(out, k)
		} else {
			out[k] = temp
		}

		//fmt.Printf("%T   %+v, %s ", out, out, k)
	}
	return
}*/

// buildMap builds a map from a file where each line in the file has two fields among others in a string separated by another string
// at known positions (general for building user and group maps from linux getent data)
// Format several fields, colon separated, groupname or username first, groupid or userid third
// Used to get group and user names from ids using the getent format from group and passwd name:x:id:
func buildMap(inputfile string, sep string, posKey, posValue int) (theMap map[string]string) {
	theMap = make(map[string]string)
	// open the file
	if file, err := os.Open(inputfile); err == nil {

		// make sure it gets closed
		defer file.Close()

		// create a new scanner and read the file line by line
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			a := strings.Split(scanner.Text(), sep)
			if len(a) > max(posKey, posValue) {
				theMap[a[2]] = a[0]
			}
		}

		if err = scanner.Err(); err != nil {
			LogError(err)
		}

	} else {
		// if the file is not found, log it but not a disaster
		LogError(err)
	}
	return
}

// get the group name from the map if it exists
func lookupGID(id string) (val string) {
	var ok bool
	if val, ok = groupMap[id]; !ok {

		val = id
	}

	return
}

// get the user name from the map if it exists
func lookupUID(id string) (val string) {
	var ok bool
	if val, ok = userMap[id]; !ok {

		val = id
	}

	return
}

// max returns max of two ints
func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// to compare two floats allowing for size and rounding errors
func relDif(a, b float64) float64 {
	// conversion of Knuth algorithm from C to Go

	c := math.Abs(a)
	d := math.Abs(b)

	d = math.Max(c, d)

	e := math.Abs(a-b) / d

	return e

}

// change the format of a set of aggregates
func AggregatesFromAggregateStats(a []*AggregateStats) (b []Aggregates) {

	for i := range a {
		statMappings := a[i].StatMappings.Values()
		nextCount := a[i].Count
		nextSize := a[i].Size
		nextACost := a[i].AccessCost
		nextCCost := a[i].CreateCost
		nextMCost := a[i].ModifyCost
		for j := range statMappings {
			nextGroup := statMappings[j].Group
			nextUser := statMappings[j].User
			nextTag := statMappings[j].Tag

			nextEntry := Aggregates{Group: nextGroup, User: nextUser, Tag: nextTag, Count: nextCount, Size: nextSize, AccessCost: nextACost, ModifyCost: nextMCost, CreationCost: nextCCost}
			b = append(b, nextEntry)
		}
	}

	return

}

// use the files of local groups and users, built in the startup shell script to translate codes to names
// if file not found, map is empty and webserver uses codes not names
func buildUserGroupMaps(groupFile string, userFile string) (groups map[string]string, users map[string]string) {

	userCodePos := 2
	userNamePos := 0
	groupCodePos := 2
	groupNamePos := 0
	users = buildMap(userFile, ":", userCodePos, userNamePos)
	groups = buildMap(groupFile, ":", groupCodePos, groupNamePos)

	return
}
