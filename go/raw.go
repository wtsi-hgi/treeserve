package treeserve

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
)

// DatabaseEntries stores what is saved for a node, for checking
type DatabaseEntries struct {
	Path         string
	Node         TreeNode
	Parent       string
	Children     []string
	StatMappings []string
}

// tree handles requests of the form <url>/api/v2?maxdepth=1&path=/lustre/scratch115/projects
// and returns the data or a 404 error
func (ts *TreeServe) raw(w http.ResponseWriter, r *http.Request) {

	path, _ := queryParameters(r)

	j, err := ts.databaseEntries(path)

	if err != nil {

		LogError(err)

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, "Could not retrieve raw data for node")

	} else {
		w.Header().Set("Content-Type", "application/json; charset=utf-8") // normal header
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.WriteHeader(http.StatusOK)

		io.WriteString(w, string(j))
	}

}

// show what's in the LMBD database for a node
func (ts *TreeServe) databaseEntries(path string) (j []byte, err error) {
	data := DatabaseEntries{}

	logInfo(fmt.Sprintf("databaseEntries for "))
	data.Path = path

	temp, err := ts.GetTreeNode(ts.getPathKey(path))

	if err != nil {
		LogError(err)
		return
	}
	data.Node = *temp

	temp2, err := ts.databaseChildren(path)
	if err != nil {
		LogError(err)
		return
	}
	data.Children = temp2

	temp3, err := ts.databaseStatMappings(path)
	if err != nil {
		LogError(err)
		return
	}
	sort.Strings(temp3)
	data.StatMappings = temp3

	pk := Md5Key{}
	pk.SetBytes(data.Node.ParentKey[:])
	temp4, err := ts.GetTreeNode(&pk)

	data.Parent = temp4.Name

	j, err = json.Marshal(data)
	if err != nil {
		LogError(err)
		return
	}

	return

}

func (ts *TreeServe) databaseChildren(path string) (s []string, err error) {
	key := ts.getPathKey(path)
	temp, err := ts.children(key)

	if err != nil {
		LogError(err)
		return
	}

	for i := range temp {
		x, err := ts.GetTreeNode(temp[i])
		if err != nil {
			LogError(err)

		}
		s = append(s, x.Name)

	}

	return
}

func (ts *TreeServe) databaseStatMappings(path string) (s []string, err error) {
	key := ts.getPathKey(path)
	temp, err := ts.retrieveAggregates(key)

	if err != nil {
		LogError(err)
		return
	}

	for i := range temp {
		nextMapping := "Group: " + temp[i].Group
		nextMapping += "  User: " + temp[i].User
		nextMapping += "  Tag: " + temp[i].Tag
		nextMapping += "  :: Count: " + temp[i].Count.Text(10)
		nextMapping += "  Size: " + temp[i].Size.Text(10)
		s = append(s, nextMapping)

	}

	return
}
