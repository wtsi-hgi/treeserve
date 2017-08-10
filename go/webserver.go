package treeserve

import (
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
	port := "8000"
	http.HandleFunc("/", hello)
	http.HandleFunc("/tree", ts.tree)
	http.ListenAndServe(":"+port, nil)
	logInfo("Webserver started")

}

func hello(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "Listening on port 8000")
}

// tree handles requests of the form <url>/api/v2?maxdepth=1&path=/lustre/scratch115/projects
// and returns the data or a 404 error
func (ts *TreeServe) tree(w http.ResponseWriter, r *http.Request) {

	path, depth := getQueryParameters(r)

	nodeKey := ts.getPathKey(path)
	//fmt.Println(path, nodeKey)
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
// Returning a few levels from the chosen directory means that recursion is not to expensive here.
func (ts *TreeServe) buildTree(rootKey *Md5Key, level int, depth int) (t dirTree, err error) {
	fmt.Println("buildTree ", level, depth)
	t = dirTree{key: rootKey}

	if level >= depth {
		return
	}

	temp, err := ts.GetTreeNode(rootKey)
	if err != nil {
		logError(err)
		return
	}

	t.Path = strings.TrimSuffix(temp.Name, "/")

	_, file := filepath.Split(t.Path)
	t.Name = file

	costs, err := ts.GetCosts(rootKey)
	if err != nil {
		logError(err)
		return
	}

	a, err := organiseCosts(costs)
	if err != nil {
		logError(err)
		return
	}
	t.Data = a

	children, err := ts.GetChildren(rootKey)
	if err != nil {
		logError(err)
		return
	}

	// for the tree of local file data, found by subtracting child data from the node data
	childCosts := []Aggregates{}

	// recursion and build file summary
	for j := range children {
		fmt.Println("level", level, "depth", depth, "child", j)

		temp, _ := ts.GetTreeNode(children[j])
		fmt.Println(temp.Name, temp.Stats.FileType)
		if temp.Stats.FileType != 'f' {
			t2, err := ts.buildTree(children[j], level+1, depth) /// make next level tree for each child
			if err != nil {
				logError(err)
			}
			if t2.Path != "" {
				t.ChildDirs = append(t.ChildDirs, &t2)
			}
		}

		// for the tree of local file data combine aggregates
		next, _ := ts.GetCosts(children[j])
		childCosts = append(childCosts, next...)

	}

	temp2, err := subtractAggregateMap(mapFromAggregateArray(costs), mapFromAggregateArray(childCosts))
	if err != nil {
		logError(err)
	}
	agg := arrayFromAggregateMap(temp2)

	w, err := organiseCosts(agg)
	if err != nil {
		logError(err)
	}
	summaryTree := dirTree{Name: "*.*", Path: t.Path, Data: w}
	t.addChild(&summaryTree)
	return
}

/// organiseCosts returns a map to to get the correct json from the array of Aggregate Costs
// for the node this is the format.
// because some of the keys are dynamic (users, groups and tags) so can't be
// just a struct.
/*  The idea is:
a.Atime[g][u][t] = 3
a.Ctime[g][u][t] = 4
a.Mtime[g][u][t] = 5
a.Count[g][u][t] = 6
a.Size[g][u][t] = *big.NewInt(7)
// but can't add to an empty map
*/
func organiseCosts(costs []Aggregates) (a webAggData, err error) {

	if err != nil {
		logError(err)
		return
	}

	for i := range costs {

		costsItem := costs[i]

		g := costsItem.Group
		u := costsItem.User
		tag := costsItem.Tag

		z := NewBigint()
		if costsItem.Count.Equals(z) {
			break // don't add empty sets of aggregates
		}

		//Access Cost
		b := costsItem.AccessCost
		updateMap(true, &a.Atime, b, g, u, tag)

		// Modify Cost"count ", ag.Count,
		b = costsItem.ModifyCost
		updateMap(true, &a.Mtime, b, g, u, tag)

		// Create Cost
		b = costsItem.CreationCost
		updateMap(true, &a.Ctime, b, g, u, tag)

		// Size
		b = costsItem.Size
		updateMap(false, &a.Size, b, g, u, tag)

		// Count
		b = costsItem.Count
		updateMap(false, &a.Count, b, g, u, tag)

	}
	return
}

//--------------------------------------------------------------

// addChild adds a child dirTree to a dirTree
func (t *dirTree) addChild(child *dirTree) {
	//fmt.Println("addChild ", child)
	t.ChildDirs = append(t.ChildDirs, child)

}

// get path and depth from request, or use defaults
// /lustre/scratch115/realdata/mdt0 is an example
func getQueryParameters(r *http.Request) (string, int) {
	url := r.URL
	vals := url.Query()
	////fmt.Println("vals: ", vals)

	//defaults
	path := "/"
	depth := 2

	if val, ok := vals["path"]; ok {
		path = val[0]
	}
	if val, ok := vals["depth"]; ok {
		depth, _ = strconv.Atoi(val[0])
	}

	path = filepath.Clean(path)
	return path, depth
}

// updateMap takes a new set of category tags and a value and updates the three level map (this is needed so that json.Marshal outputs the correct format)
func updateMap(scaleMap bool, theMap *map[string]map[string]map[string]string, theValue *Bigint, g string, u string, tag string) {
	if len(*theMap) == 0 {
		//fmt.Println("**first making Atime")
		mt := make(map[string]string)
		if scaleMap {
			mt[tag] = convertCostsForOutput(theValue)
		} else {
			mt[tag] = theValue.Text(10)
		}

		mu := make(map[string]map[string]string)
		mu[u] = mt
		mg := make(map[string]map[string]map[string]string)
		mg[g] = mu

		*theMap = mg
		//fmt.Println("** len Atime ", len(a.Atime))

		//a.Atime = map[string]map[string]map[string]string{g: map[string]map[string]string{u: map[string]string{tag: *b}}}
	} else if _, ok := (*theMap)[g]; ok { // g exists in map
		if _, ok2 := (*theMap)[g][u]; !ok2 { // but u doesn't}
			//fmt.Println("first making Atime[g][u], g exists")
			mt := make(map[string]string)
			if scaleMap {
				mt[tag] = convertCostsForOutput(theValue)
			} else {
				mt[tag] = theValue.Text(10)
			}

			(*theMap)[g][u] = mt
		} else {
			// key tag does not exist in map
			//fmt.Println("first making Atime[g][u][tag], g and u exist")
			if scaleMap {
				(*theMap)[g][u][tag] = convertCostsForOutput(theValue)
			} else {
				(*theMap)[g][u][tag] = theValue.Text(10)
			}

		}

	}

}

// GetCosts takes a node key and returns the array of costs associated with it
// Used for output after the database has been built up. Returns an error if the node has no costs associated
// which may be the case for the parent of the root node but nothing else
func (ts *TreeServe) GetCosts(nodekey *Md5Key) (data []Aggregates, err error) {
	data = []Aggregates{}
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

		vals, _ := ts.StatMappingDB.Get(x)
		ag.Group = vals.(*StatMapping).Group
		ag.User = vals.(*StatMapping).User
		ag.Tag = vals.(*StatMapping).Tag

		temp, _ := ts.AggregateSizeDB.Get(x)
		size := temp.(*Bigint)
		ag.Size = size

		temp, _ = ts.AggregateCountDB.Get(x)

		count := temp.(*Bigint)
		ag.Count = count

		temp, _ = ts.AggregateAccessCostDB.Get(x)
		ag.AccessCost = temp.(*Bigint)

		temp, _ = ts.AggregateModifyCostDB.Get(x)
		ag.ModifyCost = temp.(*Bigint)

		temp, _ = ts.AggregateCreateCostDB.Get(x)
		ag.CreationCost = temp.(*Bigint)

		z := NewBigint()
		if !count.Equals(z) {
			// This is a hack as there should not be zero count aggregates in the database.
			// find out where they come from (think just parent of root but shouldn't be there)
			data = append(data, ag)
		} else {
			logError(errors.New("Zero count for node "))
		}

	}

	return

}

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
func convertCostsForOutput(b *Bigint) (s string) {
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
// is not present in the parent.
func subtractAggregateMap(parent, child map[string]Aggregates) (out map[string]Aggregates, err error) {
	out = parent
	for k, v := range child {
		if val, ok := out[k]; ok {

			temp, err := subtractAggregates(val, v)
			if err != nil {
				return out, err
			}

			out[k] = temp

		} else {
			err := errors.New("Child key not found in parent " + k)
			return out, err
		}

	}

	return

}
