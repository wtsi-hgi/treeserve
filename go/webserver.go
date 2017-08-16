package treeserve

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Aggregate values are converted from bytes and secodns to Tebibytes and year on output
const secondsInYear = 60 * 60 * 24 * 365
const costPerTibYear = 150.0

var userMap map[string]string
var groupMap map[string]string
var groupfile = "/tmp/groups"
var userfile = "/tmp/users"

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

	Size         *Bigint `json:"size"`
	Count        *Bigint `json:"count"`
	CreationCost *Bigint `json:"ccost"`
	AccessCost   *Bigint `json:"acost"`
	ModifyCost   *Bigint `json:"mcost"`
}

/*
type NodeData struct {
	NodePath string
	NodeType string
}*/

//Webserver listens for requests of the form
// xxxxx/maxdepth=1&path=/lustre/scratch115/projects
// and returns nodes in json
func (ts *TreeServe) Webserver() {
	userMap = buildMap(userfile, ":", 2, 0)
	groupMap = buildMap(groupfile, ":", 2, 0)
	port := "8000"
	http.HandleFunc("/", hello)
	http.HandleFunc("/tree", ts.tree)
	http.HandleFunc("/raw", ts.raw)
	http.ListenAndServe(":"+port, nil)
	logInfo("Webserver started")

}

func hello(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "Listening on port 8000")
}

// tree handles requests of the form <url>/api/v2?maxdepth=1&path=/lustre/scratch115/projects
// and returns the data or a 404 error
func (ts *TreeServe) tree(w http.ResponseWriter, r *http.Request) {

	path, depth := queryParameters(r)

	nodeKey := ts.getPathKey(path)

	j := []byte{}

	t, err := ts.buildTree(nodeKey, 0, depth)

	if err == nil {
		ft := fullTree{Date: time.Now().String(), Tree: t}
		j, err = json.Marshal(ft)

	}
	if err != nil {

		logError(err)

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

	if level >= depth {
		return
	}

	temp, err := ts.GetTreeNode(rootKey)
	if err != nil {
		logError(err)
		return
	}

	t = dirTree{key: rootKey}
	t.Path = strings.TrimSuffix(temp.Name, "/")
	_, file := filepath.Split(t.Path)
	t.Name = file

	stats, err := ts.retrieveAggregates(rootKey)
	if err != nil {
		return
	}
	if len(stats) == 0 {
		logInfo(" Blank stats at " + t.Path)
	}

	a, err := organiseAggregates(stats)
	if err != nil {
		return
	}
	t.Data = a

	child, err := ts.children(rootKey)
	if err != nil {
		return
	}

	// for the tree of local file data, found by subtracting grandchild data from the node data
	grandchildstats := []Aggregates{}

	// recursion and build file summary
	for j := range child {
		logInfo(fmt.Sprintf("level %d depth %d child %d", level, depth, j))

		temp, err := ts.GetTreeNode(child[j])
		if err != nil {
			logError(err)
		}
		logInfo(fmt.Sprintf("Name %s, Type %s", temp.Name, string(temp.Stats.FileType)))
		if temp.Stats.FileType != 'f' {
			grandchildstats = ts.addGrandChildStats(grandchildstats, child[j])
			t2, err := ts.buildTree(child[j], level+1, depth) /// make next level tree for each child
			if err != nil {
				logError(err)
			} else if t2.Path != "" {
				t.addChild(&t2)
			}
		}

	}

	summaryTree := getSummaryTree(t.Path, stats, grandchildstats)

	t.addChild(&summaryTree)

	return
}

func getSummaryTree(path string, stats []Aggregates, grandchildstats []Aggregates) (t dirTree) {
	// combine grandchild stats (have one entry where categories are the same) and subtract from node stats
	temp, err := subtractAggregateMap(mapFromAggregateArray(stats), mapFromAggregateArray(grandchildstats))
	if err != nil {
		logError(err)

	} else {
		agg := arrayFromAggregateMap(temp)

		w, err := organiseAggregates(agg)
		if err != nil {
			logError(err)
		}

		t = dirTree{Name: "*.*", Path: path + "/*.*", Data: w}
	}

	return

}

// build up the set of stats of grandchildren of a node
func (ts *TreeServe) addGrandChildStats(g []Aggregates, key *Md5Key) (newg []Aggregates) {
	copy(newg, g)
	// for the tree of local file data combine grandchild aggregates
	grandChildren, err := ts.children(key)
	if err != nil {
		logError(err)
	}

	for j2 := range grandChildren {
		next, err := ts.retrieveAggregates(grandChildren[j2])
		if err != nil {
			logError(err)
		}
		if len(next) != 0 {
			newg = append(newg, next...)
		}

	}
	fmt.Println("***", g, newg)
	return
}

/// organiseAggregates returns a map to to get the correct json from the array of Aggregate stats
// for the node this is the format.
// because some of the keys are dynamic (users, groups and tags) so can't be
// just a struct.
/*  The idea is:
a.Atime[g][u][t] = 3
a.Ctime[g][u][t] = 4
a.Mtime[g][u][t] = 5
a.Count[g][u][t] = 6
a.Size[g][u][t] = 7
// but can't add to an empty map so work out which map levels exist
*/
func organiseAggregates(stats []Aggregates) (a webAggData, err error) {

	if err != nil {
		logError(err)
		return
	}

	for i := range stats {

		statsItem := stats[i]

		g := statsItem.Group
		u := statsItem.User
		tag := statsItem.Tag

		z := NewBigint()
		if statsItem.Count.Equals(z) {
			break // don't add empty sets of aggregates
		}
		if g == "0" || g == "root" {
			fmt.Println(" Zero Group")
			break // don't add empty sets of aggregates
		}
		if u == "0" {
			fmt.Println("Zero User")
			break // don't add empty sets of aggregates
		}

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
		updateMap(false, &a.Count, b, g, u, tag)

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
		if err != nil {
			logError(err)
		}
	}

	path = filepath.Clean(path)
	return

}

// updateMap takes a new set of category tags and a value and updates the three level map (this is needed so that json.Marshal outputs the correct format)
func updateMap(scaleMap bool, theMap *map[string]map[string]map[string]string, theValue *Bigint, g string, u string, tag string) {
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
	data = []Aggregates{}
	// all keys mapping this node to sets of aggregate stats
	aggregateKeys, err := ts.StatMappingsDB.GetKeySet(nodekey)
	if err != nil {
		logError(err)
		return
	}
	if len(aggregateKeys) == 0 {
		logError(errors.New("No stats found for node"))
		return
	}

	for i := range aggregateKeys {

		x := aggregateKeys[i].(*Md5Key)

		ag := Aggregates{}

		vals, err := ts.StatMappingDB.Get(x)
		if err != nil {
			logError(err)
		}
		ag.Group = lookupGID(vals.(*StatMapping).Group)
		ag.User = lookupUID(vals.(*StatMapping).User)
		ag.Tag = vals.(*StatMapping).Tag

		if ag.Tag == "type_\u0000" {
			fmt.Println("Empty tag")
		}

		temp, err := ts.AggregateSizeDB.Get(x)
		if err != nil {
			logError(err)
		}
		size := temp.(*Bigint)
		ag.Size = size

		temp, err = ts.AggregateCountDB.Get(x)
		if err != nil {
			logError(err)
		}

		count := temp.(*Bigint)
		ag.Count = count

		temp, err = ts.AggregateAccessCostDB.Get(x)
		if err != nil {
			logError(err)
		}
		ag.AccessCost = temp.(*Bigint)

		temp, err = ts.AggregateModifyCostDB.Get(x)
		if err != nil {
			logError(err)
		}
		ag.ModifyCost = temp.(*Bigint)

		temp, err = ts.AggregateCreateCostDB.Get(x)
		if err != nil {
			logError(err)
		}
		ag.CreationCost = temp.(*Bigint)

		z := NewBigint()
		if !count.Equals(z) && ag.Tag != "type_\u0000" {
			// This is a hack as there should not be zero count aggregates in the database.
			// find out where they come from (think just parent of root but shouldn't be there)
			data = append(data, ag)
		} else {
			ns, _ := ts.GetTreeNode(nodekey)
			logError(errors.New("Zero count for node " + ns.Name))
		}

	}

	return

}

// error logging with file and line number
func logError(err error) {

	buf := os.Stderr
	_, f, l, _ := runtime.Caller(1)
	logger := log.New(buf, "ERROR: "+f+" "+strconv.Itoa(l)+" ", log.LstdFlags)
	logger.Println(err)

}

func logInfo(str string) {

	buf := os.Stdout
	_, f, l, _ := runtime.Caller(1)
	logger := log.New(buf, "INFO: "+f+" "+strconv.Itoa(l)+" ", log.LstdFlags)
	logger.Println(str)

}

// The original version had times in years and sizes in tebibytes (2^40 bytes)
// This one uses Big package to keep sizes in bytes and times in seconds
// The conversion factor was #150 per tebibyte year
func convertstatsForOutput(b *Bigint) (s string) {
	sizeConv := NewBigint()
	sizeConv.SetInt64(1024 * 1024 * 1024 * 1024) // tebibytes
	timeConv := NewBigint()
	timeConv.SetInt64(secondsInYear / 150) // divisible
	// we need b divided size conv and seconds in a year and multiplied by 150
	overallConv := NewBigint()
	overallConv.Mul(sizeConv, timeConv)

	s = Divide(b, overallConv)
	return
}

func addAggregates(a, b Aggregates) (c Aggregates, err error) {

	if a.Group != b.Group {
		err = errors.New("addAggregates ... groups don't match")
	} else {
		c.Group = a.Group
	}
	if a.User != b.User {
		err = errors.New("addAggregates ... users don't match")
	} else {
		c.User = a.User
	}
	if a.Tag != b.Tag {
		err = errors.New("addAggregates ... tags don't match")
	} else {
		c.Tag = a.Tag
	}

	temp := NewBigint()

	temp.Add(a.Count, b.Count)
	c.Count = temp

	temp2 := NewBigint()
	temp2.Add(a.Size, b.Size)
	c.Size = temp2

	temp3 := NewBigint()
	temp3.Add(a.AccessCost, b.AccessCost)
	c.AccessCost = temp3

	temp4 := NewBigint()
	temp4.Add(a.CreationCost, b.CreationCost)
	c.CreationCost = temp4

	temp5 := NewBigint()
	temp5.Add(a.ModifyCost, b.ModifyCost)
	c.ModifyCost = temp5
	return

}

// subtract aggregates subtracts one set of aggregates from another provided the mappings are the same, else error
func subtractAggregates(a, b Aggregates) (c Aggregates, err error) {

	if a.Group != b.Group {
		err = errors.New("addAggregates ... groups don't match")
	} else {
		c.Group = a.Group
	}
	if a.User != b.User {
		err = errors.New("addAggregates ... users don't match")
	} else {
		c.User = a.User
	}
	if a.Tag != b.Tag {
		err = errors.New("addAggregates ... tags don't match")
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

}

// mapFromAggregates returns a map with key Group:User:Tag and aggregate values added for same key
func mapFromAggregateArray(in []Aggregates) (out map[string]Aggregates) {
	out = make(map[string]Aggregates)
	for i := range in {
		key := in[i].Group + ":" + in[i].User + ":" + in[i].Tag
		if val, ok := out[key]; !ok {
			out[key] = in[i]
		} else {
			val2, err := addAggregates(val, in[i])
			if err != nil {
				logError(err)
			}
			out[key] = val2
		}
	}
	return
}

// arrayFromAggregateMap returns an array of the values in the map
func arrayFromAggregateMap(in map[string]Aggregates) (out []Aggregates) {

	for _, v := range in {
		out = append(out, v)
	}
	return
}

// subtractAggregateMap subtracts a child map from a parent map and returns an error if a child key
// is not present in the parent. If count becomes zero the entry is not returned
func subtractAggregateMap(parent, child map[string]Aggregates) (out map[string]Aggregates, err error) {
	out = parent
	for k, v := range child {
		if val, ok := out[k]; ok {

			temp, err := subtractAggregates(val, v)
			if err != nil {
				return out, err
			}
			if temp.Count.isNegative() { // includes zero
				delete(out, k)
			} else {
				out[k] = temp
			}

		} else {
			err := errors.New("Child key not found in parent map " + k)
			return out, err
		}

	}

	return

}

// build a map from a file where each line in the file has two fields among others in a string separated by another string
// at known positions (general for building user and group maps from linux getent data)
// Format several fields, colon separated, groupname or username first, groupid or userid third
// Used to get group and user names from ids
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

		// check for errors
		if err = scanner.Err(); err != nil {
			logError(err)
		}

	} else {
		// if the file is not found, log it but not a disaster
		logError(err)

	}

	return

}

func lookupGID(id string) (val string) {
	var ok bool
	if val, ok = groupMap[id]; !ok {

		val = id
	}

	return
}

func lookupUID(id string) (val string) {
	var ok bool
	if val, ok = userMap[id]; !ok {

		val = id
	}

	return
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

/*
func checkError(err error, level int) {
	if err == nil {
		return
	}
	switch level {
	case 0:
		logError(err)
	case 1:
		logError(err)
	case 2:
		logError(err)
	}
}*/
