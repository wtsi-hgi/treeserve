// Copyright (c) 2016 Genome Research Ltd.
// Author: Joshua C. Randall <jcrandall@alum.mit.edu>
//
// This program is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the Free Software
// Foundation; either version 3 of the License, or (at your option) any later
// version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
// details.
//
// You should have received a copy of the GNU General Public License along with
// this program. If not, see <http://www.gnu.org/licenses/>.
//

package treeserve

import (
	"bufio"
	"compress/gzip"
	"crypto/md5"
	"encoding"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
)

// Types & Structures
type PathCheck func(string) bool

// FinalizeWork .... type NodeVisitor func(nodeKey *Md5Key) (*AggregateStats, error)
type FinalizeWork struct {
	SubtreeNode *Md5Key
	Depth       int
	Results     chan []*AggregateStats
}
type TreeServe struct {
	LMDBPath                 string
	LMDBMapSize              int64
	CostReferenceTime        int64
	NodesCreatedInfoEveryN   int64
	NodesFinalizedInfoEveryN int64
	FileCategoryPathChecks   map[string]PathCheck
	LMDBEnv                  *lmdb.Env
	TreeServeDBI             lmdb.DBI  // overall state of the TreeServe database
	TreeNodeDB               GenericDB // maps path Md5Key to non-aggregated TreeNode data
	StatMappingDB            GenericDB // maps statmapping Md5Key back to StatMapping data
	ChildrenDB               KeySetDB  // maps node Md5Key to set of child Md5Keys
	StatMappingsDB           KeySetDB  // maps  node+aggregateData Md5Key to set of statMapping Md5Keys
	AggregateSizeDB          GenericDB // maps  node+aggregateData Md5Key to aggregated size for that node
	AggregateCountDB         GenericDB // maps  node+aggregateData  to aggregated count for that node
	AggregateCreateCostDB    GenericDB // maps  node+aggregateData  to aggregated cost since created for that node
	AggregateModifyCostDB    GenericDB // maps  node+aggregateData  to aggregated cost since modified for that node
	AggregateAccessCostDB    GenericDB // maps  node+aggregateData  to aggregated cost since accessed for that node
	NodesCreated             int64
	NodesFinalized           int64
	StopInputAfterNLines     int64
	StopFinalizeAfterNNodes  int64
	Debug                    bool
}

// BinaryMarshallerUnmarshaller is used to make sure every
// type used can provide a uniform binary encoding to save as an LMDB value.
type BinaryMarshalUnmarshaler interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type UpdateData func(existing BinaryMarshalUnmarshaler) (BinaryMarshalUnmarshaler, error)
type NewData func() BinaryMarshalUnmarshaler

func NewTreeServe(lmdbPath string, lmdbMapSize int64, costReferenceTime int64, nodesCreatedInfoEveryN int64, stopInputAfterNLines int64, nodesFinalizedInfoEveryN int64, stopFinalizeAfterNNodes int64, debug bool) (ts *TreeServe) {
	ts = new(TreeServe)
	ts.LMDBPath = lmdbPath
	ts.LMDBMapSize = lmdbMapSize
	ts.CostReferenceTime = costReferenceTime
	ts.NodesCreatedInfoEveryN = nodesCreatedInfoEveryN
	ts.StopInputAfterNLines = stopInputAfterNLines
	ts.NodesFinalizedInfoEveryN = nodesFinalizedInfoEveryN
	ts.StopFinalizeAfterNNodes = stopFinalizeAfterNNodes
	ts.Debug = debug
	ts.SetFileCategoryPathChecks()
	return ts
}

func (ts *TreeServe) NewTreeNodeDB(dbName string) (gdb GenericDB, err error) {
	gdb = GenericDB{DBCommon{TS: ts, Name: dbName}, func() BinaryMarshalUnmarshaler { return NewTreeNode() }}
	gdb.DBI, err = ts.openLMDBDBI(ts.LMDBEnv, gdb.Name, lmdb.Create)
	if ts.Debug {
		log.WithFields(log.Fields{
			"ts":     ts,
			"dbName": dbName,
		}).Debug("opened TreeNode database")
	}
	return
}

func (ts *TreeServe) NewStatMappingDB(dbName string) (gdb GenericDB, err error) {
	gdb = GenericDB{DBCommon{TS: ts, Name: dbName}, func() BinaryMarshalUnmarshaler { return NewStatMapping() }}
	gdb.DBI, err = ts.openLMDBDBI(ts.LMDBEnv, gdb.Name, lmdb.Create)
	if ts.Debug {
		log.WithFields(log.Fields{
			"ts":     ts,
			"dbName": dbName,
		}).Debug("opened StatMapping database")
	}
	return
}

func (ts *TreeServe) NewBigintDB(dbName string) (gdb GenericDB, err error) {
	gdb = GenericDB{DBCommon{TS: ts, Name: dbName}, func() BinaryMarshalUnmarshaler { return NewBigint() }}
	gdb.DBI, err = ts.openLMDBDBI(ts.LMDBEnv, gdb.Name, lmdb.Create)
	if ts.Debug {
		log.WithFields(log.Fields{
			"ts":     ts,
			"dbName": dbName,
		}).Debug("opened BigintDB")
	}
	return
}

func (ts *TreeServe) NewKeySetDB(dbName string) (ksdb KeySetDB, err error) {
	ksdb = KeySetDB{DBCommon{TS: ts, Name: dbName}}
	ksdb.DBI, err = ts.openLMDBDBI(ts.LMDBEnv, ksdb.Name, (lmdb.Create | lmdb.DupSort | lmdb.DupFixed))
	if ts.Debug {
		log.WithFields(log.Fields{
			"ts":     ts,
			"dbName": dbName,
		}).Debug("opened  Key Set database")
	}
	return
}

func (ts *TreeServe) SetFileCategoryPathChecks() {
	// TODO move to externally specifiable JSON?
	ts.FileCategoryPathChecks = make(map[string]PathCheck)
	ts.FileCategoryPathChecks["cram"] = func(path string) bool { return strings.HasSuffix(path, ".cram") }
	ts.FileCategoryPathChecks["bam"] = func(path string) bool { return strings.HasSuffix(path, ".bam") }
	ts.FileCategoryPathChecks["index"] = func(path string) bool {
		for _, ending := range []string{".crai", ".bai", ".sai", ".fai", ".csi"} {
			if strings.HasSuffix(path, ending) {
				return true
			}
		}
		return false
	}
	ts.FileCategoryPathChecks["compressed"] = func(path string) bool {
		for _, ending := range []string{".bzip2", ".gz", ".tgz", ".zip", ".xz", ".bgz", ".bcf"} {
			if strings.HasSuffix(path, ending) {
				return true
			}
		}
		return false
	}
	ts.FileCategoryPathChecks["uncompressed"] = func(path string) bool {
		for _, ending := range []string{".sam", ".fasta", ".fastq", ".fa", ".fq", ".vcf", ".csv", ".tsv", ".txt", ".text", "README"} {
			if strings.HasSuffix(path, ending) {
				return true
			}
		}
		return false
	}
	ts.FileCategoryPathChecks["checkpoint"] = func(path string) bool { return strings.HasSuffix(path, "jobstate.context") }
	ts.FileCategoryPathChecks["temporary"] = func(path string) bool {
		for _, containing := range []string{"tmp", "temp"} {
			if strings.Contains(path, containing) {
				return true
			}
		}
		return false
	}
}

func (ts *TreeServe) OpenLMDB() (err error) {
	if ts.Debug {
		log.WithFields(log.Fields{"ts": ts}).Debug("configuring and opening LMDB environment")
	}
	ts.LMDBEnv, err = lmdb.NewEnv()
	if err != nil {
		log.WithFields(log.Fields{"err": err}).Fatal("failed to create new LMDB environment")
	}
	err = ts.LMDBEnv.SetMapSize(ts.LMDBMapSize)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"ts":  ts,
		}).Fatal("failed to set LMDB environment map size")
	}
	err = ts.LMDBEnv.SetMaxDBs(10)
	if err != nil {
		log.WithFields(log.Fields{"err": err}).Fatal("failed to set LMDB environment max DBs")
	}
	err = ts.LMDBEnv.Open(ts.LMDBPath, (lmdb.MapAsync | lmdb.WriteMap | lmdb.NoSubdir), 0600)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"ts":  ts,
		}).Fatal("failed to open LMDB environment")
	}

	ts.TreeServeDBI, err = ts.openLMDBDBI(ts.LMDBEnv, "TreeServe", lmdb.Create)
	if ts.Debug {
		log.WithFields(log.Fields{"ts": ts}).Debug("opened TreeServe database")
	}

	ts.TreeNodeDB, err = ts.NewTreeNodeDB("TreeNode")
	if err != nil {
		log.WithFields(log.Fields{"ts": ts}).Fatal("failed to open TreeNode database")
	}

	ts.StatMappingDB, err = ts.NewStatMappingDB("StatMapping")
	if err != nil {
		log.WithFields(log.Fields{"ts": ts}).Fatal("failed to open StatMapping database")
	}

	ts.ChildrenDB, err = ts.NewKeySetDB("Children")
	if err != nil {
		log.WithFields(log.Fields{"ts": ts}).Fatal("failed to open Children database")
	}

	ts.StatMappingsDB, err = ts.NewKeySetDB("StatMappings")
	if err != nil {
		log.WithFields(log.Fields{"ts": ts}).Fatal("failed to open StatMappings database")
	}

	ts.AggregateSizeDB, err = ts.NewBigintDB("AggregateSize")
	if err != nil {
		log.WithFields(log.Fields{"ts": ts}).Fatal("failed to open AggregateSize database")
	}

	ts.AggregateCountDB, err = ts.NewBigintDB("AggregateCount")
	if err != nil {
		log.WithFields(log.Fields{"ts": ts}).Fatal("failed to open AggregateCount database")
	}

	ts.AggregateCreateCostDB, err = ts.NewBigintDB("AggregateCreateCost")
	if err != nil {
		log.WithFields(log.Fields{"ts": ts}).Fatal("failed to open AggregateCreateCost database")
	}

	ts.AggregateModifyCostDB, err = ts.NewBigintDB("AggregateModifyCost")
	if err != nil {
		log.WithFields(log.Fields{"ts": ts}).Fatal("failed to open AggregateModifyCost database")
	}

	ts.AggregateAccessCostDB, err = ts.NewBigintDB("AggregateAccessCost")
	if err != nil {
		log.WithFields(log.Fields{"ts": ts}).Fatal("failed to open AggregateAccessCost database")
	}

	return
}

func (ts *TreeServe) CloseLMDB() {
	ts.LMDBEnv.Close()
}

func (ts *TreeServe) lookupUid(uid uint64) (user string) {
	// TODO implement
	user = strconv.FormatUint(uid, 10)
	return
}

func (ts *TreeServe) lookupGid(gid uint64) (group string) {
	// TODO implement
	group = strconv.FormatUint(gid, 10)
	return
}

func (ts *TreeServe) getPathKey(path string) (pathKeyPtr *Md5Key) {
	pathKey := Md5Key{}
	pathKey.Sum([]byte(path))
	pathKeyPtr = &pathKey
	return
}

func (ts *TreeServe) GetTreeNode(nodeKey *Md5Key) (treeNode *TreeNode, err error) {
	dbData, err := ts.TreeNodeDB.Get(nodeKey)
	if err != nil {
		log.WithFields(log.Fields{
		//"ts":      ts,
		//"nodeKey": nodeKey,
		}).Error("failed to get tree node")
	}
	treeNode = dbData.(*TreeNode)
	return
}

func (ts *TreeServe) GetStatMapping(statMappingKey *Md5Key) (treeNode *StatMapping, err error) {
	dbData, err := ts.StatMappingDB.Get(statMappingKey)
	if err != nil {
		log.WithFields(log.Fields{
		//"ts":             ts,
		//"statMappingKey": statMappingKey,
		}).Error("failed to get stat mapping")
	}
	treeNode = dbData.(*StatMapping)
	return
}

// ensureDirectoryInTree checks whether a directory node exists and adds it if not
func (ts *TreeServe) ensureDirectoryInTree(dirPath string) (dirPathKey *Md5Key, err error) {
	dirPathKey = ts.getPathKey(dirPath)
	if ts.Debug {
		log.WithFields(log.Fields{
			"dirPath": dirPath,
			//"dirPathKey": dirPathKey,
			//"ts.LMDBEnv": ts.LMDBEnv,
		}).Debug("entered ensureDirectoryInTree()")
	}
	haveDir, err := ts.TreeNodeDB.HasKey(dirPathKey)
	if err != nil {
		log.WithFields(log.Fields{
			"err":     err,
			"dirPath": dirPath,
			//	"dirPathKey": dirPathKey,
		}).Fatal("failed to check if directory exists in tree")
	}
	if !haveDir {
		if ts.Debug {
			log.WithFields(log.Fields{
				"dirPath": dirPath,
			}).Debug("parent does not exist, attempting to create")
		}
		err = ts.createTreeNode(dirPath, "d", NodeStats{})
		if err != nil {
			log.WithFields(log.Fields{
				"dirPath": dirPath,
				"err":     err,
			}).Fatal("failed to create tree node")
		}
	}
	return
}

// addChildToParent updates the database with a link between a child and parent node
func (ts *TreeServe) addChildToParent(parentKey *Md5Key, nodeKey *Md5Key) (err error) {
	err = ts.ChildrenDB.AddKeyToKeySet(parentKey, nodeKey)
	if err != nil {
		log.WithFields(log.Fields{
			//	"parentKey": parentKey,
			//	"nodeKey":   nodeKey,
			"err": err,
		}).Error("failed to add child node to parent")
	}
	return
}

// children returns the keys of the chilren of a node as an array of pointers
func (ts *TreeServe) children(nodeKey *Md5Key) (children []*Md5Key, err error) {
	dbDataSet, err := ts.ChildrenDB.GetKeySet(nodeKey)
	if err != nil {
		log.WithFields(log.Fields{
			"ts": ts,
			//	"nodeKey": nodeKey,
		}).Error("failed to get children")
		return
	}
	for _, dbData := range dbDataSet {
		children = append(children, (dbData).(*Md5Key))
	}
	return
}

// GetTags returns the categories to use for the aggregate costs breakdown.
func (ts *TreeServe) GetTags(treeNode *TreeNode) (categories []string) {

	// if no regex-based properties applied, assign to "other"
	for tag, f := range ts.FileCategoryPathChecks {
		if f(strings.ToLower(treeNode.Name)) {
			categories = append(categories, tag)
		}
	}
	if len(categories) == 0 {
		categories = append(categories, "other") // done like that in C++ version
	}

	// every entry has '*' property
	categories = append(categories, "*")

	// add property based on file type
	switch treeNode.Stats.FileType {
	case 'f':
		categories = append(categories, "file")
	case 'd':
		categories = append(categories, "directory")
	case 'l':
		categories = append(categories, "link")
	default:
		categories = append(categories, fmt.Sprintf("type_%s", string(treeNode.Stats.FileType)))
	}
	return
}

// createTreeNode adds a node to the database, with its parent and data
func (ts *TreeServe) createTreeNode(nodePath string, fileType string, nodeStats NodeStats) (err error) {
	nodeKey := ts.getPathKey(nodePath)
	var parentPath string
	var parentKey = &Md5Key{}
	if nodePath != "/" {
		parentPath = path.Dir(nodePath)
		parentKey, err = ts.ensureDirectoryInTree(parentPath)
		if err != nil {
			log.WithFields(log.Fields{
				"parentPath": parentPath,
				"err":        err,
			}).Error("failed to ensure parent directory in tree")
			return
		}
	}
	node := &TreeNode{nodePath, parentKey.GetFixedBytes(), nodeStats}
	err = ts.TreeNodeDB.Update(nodeKey, func(existingDbData BinaryMarshalUnmarshaler) (updatedNode BinaryMarshalUnmarshaler, err error) {

		log.WithFields(log.Fields{
			"existingDbData": existingDbData,
			"node":           node,
			//	"nodeKey.String()": nodeKey.String(),
			"nodePath": nodePath,
		}).Debug("createTreeNode update")

		if existingDbData != nil {
			existing := existingDbData.(*TreeNode)
			if existing.Name != node.Name {
				err = fmt.Errorf("existing node Name '%v' does not match update Name '%v'", existing.Name, node.Name)
			}
			if existing.ParentKey != node.ParentKey {
				err = fmt.Errorf("existing node ParentKey '%v' does not match update ParentKey '%v'", existing.ParentKey, node.ParentKey)
			}
			if err != nil {
				log.WithFields(log.Fields{
					"existing": existing,
					"node":     node,
					"err":      err,
					"nodeKey":  nodeKey,
					"nodePath": nodePath,
				}).Error("existing node did not match update")
				return
			}
		}
		updatedNode = node
		if ts.Debug {
			log.WithFields(log.Fields{
				"updatedNode": updatedNode,
			}).Debug("returning updated node")
		}
		return
	})
	if err != nil {
		log.WithFields(log.Fields{
			"err":     err,
			"nodeKey": nodeKey,
			"node":    node,
		}).Error("failed to add tree node")
	}
	if nodePath != "/" {
		if ts.Debug {
			log.WithFields(log.Fields{
				"nodePath": nodePath,
				//	"nodeKey":    nodeKey,
				"parentPath": parentPath,
				//	"parentKey":  parentKey,
			}).Debug("adding node to parent")
		}
		err = ts.addChildToParent(parentKey, nodeKey)
		if err != nil {
			log.WithFields(log.Fields{
				//	"parentKey": parentKey,
				//	"nodeKey":   nodeKey,
				"err": err,
			}).Error("failed to add node to parent")
			return
		}
	}
	ts.NodesCreated++
	if ts.NodesCreated%ts.NodesCreatedInfoEveryN == 0 {
		log.WithFields(log.Fields{
			"ts.NodesCreated": ts.NodesCreated,
		}).Info("created nodes")
	}
	return
}

// processLine decodes a line from the input file and saves the data
func (ts *TreeServe) processLine(line string) (err error) {

	log.WithFields(log.Fields{
		"line": line,
	}).Debug("entered processLine()")

	s := strings.SplitN(line, "\t", 11)
	b64NodePath := s[0]
	nodePathBytes, err := base64.StdEncoding.DecodeString(b64NodePath)
	if err != nil {
		log.WithFields(log.Fields{
			"b64NodePath": b64NodePath,
			"line":        line,
		}).Fatal("failed to decode base64 encoded node path")
	}
	nodePath := string(nodePathBytes)
	size, err := strconv.ParseUint(s[1], 10, 64)
	if err != nil {
		log.WithFields(log.Fields{"s[1]": s[1]}).Fatal("failed to parse size as uint")
	}
	uid, err := strconv.ParseUint(s[2], 10, 64)
	if err != nil {
		log.WithFields(log.Fields{"s[2]": s[1]}).Fatal("failed to parse uid as uint")
	}
	gid, err := strconv.ParseUint(s[3], 10, 64)
	if err != nil {
		log.WithFields(log.Fields{"s[3]": s[1]}).Fatal("failed to parse gid as uint")
	}
	accessTime, err := strconv.ParseInt(s[4], 10, 64)
	if err != nil {
		log.WithFields(log.Fields{"s[4]": s[1]}).Fatal("failed to parse accessTime as int")
	}
	modificationTime, err := strconv.ParseInt(s[5], 10, 64)
	if err != nil {
		log.WithFields(log.Fields{"s[5]": s[1]}).Fatal("failed to parse modificationTime as int")
	}
	creationTime, err := strconv.ParseInt(s[6], 10, 64)
	if err != nil {
		log.WithFields(log.Fields{"s[6]": s[1]}).Fatal("failed to parse creationTime as int")
	}
	fileType := s[7]
	//iNode := s[8]
	//linkCount := s[9]
	//devId := s[10]
	nodeStats := NodeStats{size, uid, gid, accessTime, modificationTime, creationTime, fileType[0]}

	log.WithFields(log.Fields{
		"nodePath":  nodePath,
		"nodeStats": nodeStats,
	}).Debug("parsed line and populated nodeStats")

	ts.createTreeNode(nodePath, fileType, nodeStats)

	return err
}

// InputWorker takes a line while there is still a line on the lines channel, calls processLine and reports any errors
func (ts *TreeServe) InputWorker(WorkerID int, lines <-chan string) (err error) {

	log.WithFields(log.Fields{
		"WorkerID": WorkerID,
	}).Debug("entered InputWorker()")

	for line := range lines {
		err = ts.processLine(line)
		if err != nil {
			log.WithFields(log.Fields{
				"WorkerID": WorkerID,
				"line":     line,
				"err":      err,
			}).Error("InputWorker failed to process line")
			break
		}
	}

	log.WithFields(log.Fields{
		"WorkerID": WorkerID,
		"err":      err,
	}).Debug("leaving InputWorker()")

	return err
}

// FinalizeWorker takes work from the finalizeWorkQueue channel, calls aggregateSubtree and reports any errors
// It checks for the queue being closed and the aggregate subtree being cancelled.
func (ts *TreeServe) FinalizeWorker(ctx context.Context, WorkerID int, finalizeWorkQueue chan *FinalizeWork, nodesFinalized chan *Md5Key) (err error) {

	log.WithFields(log.Fields{
		"WorkerID": WorkerID,
	}).Debug("entered FinalizeWorker()")

	var work *FinalizeWork
	for {

		select {
		case <-ctx.Done():

			if ctx.Err() == context.Canceled {
				return nil
			}
			return ctx.Err()
		case work = <-finalizeWorkQueue:
			if work == nil {

				return nil
			}
		}

		err = ts.aggregateSubtree(ctx, WorkerID, work, finalizeWorkQueue, nodesFinalized)
		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {

				return nil
			}
		}
		if err != nil {

			return err
		}
	}
	return nil
}

func (ts *TreeServe) GetState() (state string, err error) {
	var stateData []byte
	err = ts.LMDBEnv.View(func(txn *lmdb.Txn) (err error) {
		stateData, err = txn.Get(ts.TreeServeDBI, []byte("state"))
		return
	})
	if lmdb.IsNotFound(err) {
		state = ""
		err = nil
		return
	} else if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("failed to get state from ts.TreeServeDBI")
	}
	state = string(stateData)
	return
}

func (ts *TreeServe) SetState(state string) (err error) {
	stateData := []byte(state)
	err = ts.LMDBEnv.Update(func(txn *lmdb.Txn) (err error) {
		err = txn.Put(ts.TreeServeDBI, []byte("state"), stateData, 0)
		return
	})
	if err != nil {
		log.WithFields(log.Fields{
			"state":     state,
			"stateData": stateData,
			"err":       err,
		}).Fatal("failed to set state in ts.TreeServeDBI")
	}
	return
}

func (ts *TreeServe) ProcessInput(inputPath string, workers int) (err error) {

	log.WithFields(log.Fields{
		"ts":        ts,
		"inputPath": inputPath,
		"workers":   workers,
	}).Debug("entered ProcessInput()")

	// Ensure databases are reset
	err = ts.TreeNodeDB.Reset()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"ts":  ts,
		}).Fatal("failed to reset treenode database")
	}
	err = ts.ChildrenDB.Reset()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"ts":  ts,
		}).Fatal("failed to reset children database")
	}
	var inputWorkerGroup errgroup.Group
	lines := make(chan string, workers*10)
	for WorkerID := 1; WorkerID <= workers; WorkerID++ {

		log.WithFields(log.Fields{
			"WorkerID": WorkerID,
		}).Debug("Starting goroutine for InputWorker")

		inputWorkerGroup.Go(func() (err error) {
			err = ts.InputWorker(WorkerID, lines)
			return err
		})
	}

	if ts.Debug {
		log.WithFields(log.Fields{"inputPath": inputPath}).Debug("opening input")
	}
	inputFile, err := os.Open(inputPath)
	if err != nil {
		log.WithFields(log.Fields{
			"err":       err,
			"inputPath": inputPath,
		}).Fatal("Error opening input")
	}
	defer inputFile.Close()

	gzipReader, err := gzip.NewReader(inputFile)
	if err != nil {
		log.WithFields(log.Fields{
			"err":       err,
			"inputPath": inputPath,
		}).Fatal("Error creating gzip reader")
	}
	defer gzipReader.Close()

	lineScanner := bufio.NewScanner(gzipReader)

	log.Debug("processing input and dispatching lines to workers")
	var lineCount int64
	for lineScanner.Scan() {
		lines <- lineScanner.Text()
		lineCount++
		if ts.StopInputAfterNLines >= 0 && lineCount > ts.StopInputAfterNLines {
			break
		}
	}
	if err := lineScanner.Err(); err != nil {
		log.WithFields(log.Fields{
			"err":       err,
			"inputPath": inputPath,
		}).Fatal("Error reading lines")
	}
	close(lines)

	log.Debug("waiting for InputWorkers to complete")
	if err := inputWorkerGroup.Wait(); err != nil {
		log.WithFields(log.Fields{"err": err}).Fatal("one or more InputWorkers failed")
	} else {
		log.Info("InputWorkers successfully processed all input lines")
	}

	return
}

// GenerateAggregateKeys makes a set of two MD5 keys for each costs breakdown for a node.
func (ts *TreeServe) GenerateAggregateKeys(nodeKey *Md5Key, statMappingKey *Md5Key) (aggregateKey *Md5Key, localAggregateKey *Md5Key, err error) {
	// calculate a set of aggregate keys (one rolled all the way up and one "local" one which only aggregates files within a directory)
	md5Hash := md5.New()
	io.WriteString(md5Hash, nodeKey.String())
	io.WriteString(md5Hash, statMappingKey.String())
	aggregateKey = &Md5Key{}
	aggregateKey.SetBytes(md5Hash.Sum(nil))
	io.WriteString(md5Hash, "*.*")
	localAggregateKey = &Md5Key{}
	localAggregateKey.SetBytes(md5Hash.Sum(nil))
	return
}

// GetStatMappings calculates a set of aggregate cost breakdown mappings for a node.
// Just the group/user/tag information not the actual costs.
func (ts *TreeServe) GetStatMappings(tn *TreeNode) (statMappings *StatMappings) {
	statMappings = NewStatMappings()
	tags := ts.GetTags(tn)
	for _, u := range []string{"*", strconv.FormatUint(tn.Stats.Uid, 10)} {
		for _, g := range []string{"*", strconv.FormatUint(tn.Stats.Gid, 10)} {
			for _, t := range tags {
				statMapping := StatMapping{u, g, t}
				statMappings.Add(statMapping.GetKey(), &statMapping)
			}
		}
	}
	return
}

// Calculate AggregateStats finds the aggregate costs breakdown for a node, worked out from the
// size and elapsed time.
func (ts *TreeServe) CalculateAggregateStats(nodeKey *Md5Key) (aggregateStats *AggregateStats, err error) {
	if ts.Debug {
		log.WithFields(log.Fields{
			"nodeKey": nodeKey,
		}).Debug("CalculateAggregateStats()")
	}
	treeNode, err := ts.GetTreeNode(nodeKey)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeKey": nodeKey,
			"err":     err,
		}).Error("CalculateAggregateStats() failed to get tree node")
		return
	}
	if ts.Debug {
		log.WithFields(log.Fields{
			"nodeKey":       nodeKey,
			"treeNode.Name": treeNode.Name,
		}).Debug("CalculateAggregateStats() got treeNode")
	}

	statMappings := ts.GetStatMappings(treeNode)

	size := NewBigint()
	size.SetUint64(treeNode.Stats.FileSize)

	count := NewBigint()
	count.SetUint64(1)

	secondsSinceCreation := NewBigint()
	secondsSinceCreation.SetInt64(ts.CostReferenceTime - treeNode.Stats.CreationTime)
	createCost := NewBigint()
	createCost.Mul(size, secondsSinceCreation)

	secondsSinceModification := NewBigint()
	secondsSinceModification.SetInt64(ts.CostReferenceTime - treeNode.Stats.ModificationTime)
	modifyCost := NewBigint()
	modifyCost.Mul(size, secondsSinceModification)

	secondsSinceAccess := NewBigint()
	secondsSinceAccess.SetInt64(ts.CostReferenceTime - treeNode.Stats.AccessTime)
	accessCost := NewBigint()
	accessCost.Mul(size, secondsSinceAccess)

	aggregateStats = &AggregateStats{
		StatMappings: statMappings,
		Size:         size,
		Count:        count,
		CreateCost:   createCost,
		ModifyCost:   modifyCost,
		AccessCost:   accessCost,
	}

	return
}

// Finalize uses a postorder traversal of the calculated tree to build up the aggregate stats of
// a node from its children.
func (ts *TreeServe) Finalize(startPath string, workers int) (err error) {

	// Ensure aggregation databases are reset
	err = ts.StatMappingDB.Reset()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"ts":  ts,
		}).Fatal("failed to reset stat mapping database")
	}
	err = ts.StatMappingsDB.Reset()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"ts":  ts,
		}).Fatal("failed to reset stat mappings database")
	}
	err = ts.AggregateSizeDB.Reset()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"ts":  ts,
		}).Fatal("failed to reset aggregate size database")
	}
	err = ts.AggregateCountDB.Reset()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"ts":  ts,
		}).Fatal("failed to reset aggregate count database")
	}
	err = ts.AggregateCreateCostDB.Reset()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"ts":  ts,
		}).Fatal("failed to reset aggregate create cost database")
	}
	err = ts.AggregateModifyCostDB.Reset()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"ts":  ts,
		}).Fatal("failed to reset aggregate modify cost database")
	}
	err = ts.AggregateAccessCostDB.Reset()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"ts":  ts,
		}).Fatal("failed to reset aggregate access cost database")
	}

	ctx := context.Background()
	ctx = context.WithValue(ctx, "workers", workers)
	ctx, cancel := context.WithCancel(ctx)
	finalizeWorkerGroup, ctx := errgroup.WithContext(ctx)

	finalizeWorkQueue := make(chan *FinalizeWork) // unbuffered channel
	nodesFinalized := make(chan *Md5Key, workers)
	WorkerIDs := make(chan int)

	workersStarted := 0
	for WorkerID := 1; WorkerID <= workers; WorkerID++ {

		finalizeWorkerGroup.Go(func() (err error) {
			id := <-WorkerIDs
			err = ts.FinalizeWorker(ctx, id, finalizeWorkQueue, nodesFinalized)
			return err
		})
		WorkerIDs <- WorkerID
		workersStarted++
	}

	startnode := ts.getPathKey(startPath)
	startnodeResults := make(chan []*AggregateStats, 1)
	startnodeWork := FinalizeWork{SubtreeNode: startnode, Depth: 0, Results: startnodeResults}

	log.Info("Finalize: submitting initial finalizework to workers")
	finalizeWorkQueue <- &startnodeWork

	log.Info("Finalize: waiting for results")
WaitForResults:
	for {
		select {
		case _ = <-startnodeResults:

			break WaitForResults
		case _ = <-nodesFinalized:

			ts.NodesFinalized++
			if ts.StopFinalizeAfterNNodes >= 0 && ts.NodesFinalized > ts.StopFinalizeAfterNNodes {

				break WaitForResults
			}
			if ts.NodesFinalized%ts.NodesFinalizedInfoEveryN == 0 {

			}

		}
	}

	log.Info("Finalize: cancelling FinalizeWorker context")
	cancel()

	log.Info("waiting for all FinalizeWorkers to complete")
	if err := finalizeWorkerGroup.Wait(); err != nil {
		log.WithFields(log.Fields{"err": err}).Fatal("one or more FinalizeWorkers failed")
	} else {
		log.Info("FinalizeWorkers successfully processed all subtree nodes")
	}

	return
}

func (ts *TreeServe) aggregateSubtree(ctx context.Context, WorkerID int, subtreeWork *FinalizeWork, finalizeWorkQueue chan *FinalizeWork, nodesFinalized chan *Md5Key) (err error) {
	node := subtreeWork.SubtreeNode
	//	nodeVisitor := subtreeWork.NodeVisitor
	level := subtreeWork.Depth

	childKeys, err := ts.children(node)
	if err != nil {
		log.WithFields(log.Fields{
			"err":      err,
			"node":     node.String(),
			"WorkerID": WorkerID,
		}).Error("failed to get child keys for node")
	}

	childResults := make(chan []*AggregateStats, len(childKeys))

	for _, childKey := range childKeys {
		childWork := &FinalizeWork{SubtreeNode: childKey, Depth: level + 1, Results: childResults}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case finalizeWorkQueue <- childWork:
			// this child has been successfully queued for finalizeWorker processing
		default:
			// could not be queued for concurrent processing, recurse using this goroutine
			logInfo("recursion")
			err = ts.aggregateSubtree(ctx, WorkerID, childWork, finalizeWorkQueue, nodesFinalized)
			if err != nil {
				return
			}
		}
	}

	// calculate aggregate stats for this node itself
	a, err := ts.CalculateAggregateStats(node)
	aggregateStats := []*AggregateStats{a}

	for range childKeys {

		var childAggregateStats []*AggregateStats
	WaitForIthChildResults:
		for {
			select {
			case <-ctx.Done():
				return
			case childAggregateStats = <-childResults:
				//aggregateStats.Add(childAggregateStats)
				aggregateStats = append(aggregateStats, childAggregateStats...)
				break WaitForIthChildResults
			}
		}

	}

	aggregateStats, _ = combineAggregateStats(aggregateStats)
	subtreeWork.Results <- aggregateStats
	// save here .... sarah
	/*
		x, _ := ts.GetTreeNode(node)
		fmt.Println("saving for " + x.Name)
	*/
	ts.saveAggregateStats(node, aggregateStats)
	//
	select {
	case <-ctx.Done():
		return
	case nodesFinalized <- node:
	}

	return
}

func (ts *TreeServe) openLMDBDBI(lmdbEnv *lmdb.Env, dbName string, flags uint) (dbi lmdb.DBI, err error) {
	if ts.Debug {
		log.WithFields(log.Fields{
			"lmdbEnv": lmdbEnv,
			"dbName":  dbName,
		}).Debug("Opening (creating if necessary) the LMDB dbi")
	}
	err = lmdbEnv.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenDBI(dbName, flags)
		return
	})
	if err != nil {
		log.WithFields(log.Fields{
			"err":    err,
			"dbName": dbName,
		}).Fatal("failed to open/create LMDB database")
	}
	var dbiStat *lmdb.Stat
	err = lmdbEnv.View(func(txn *lmdb.Txn) (err error) {
		dbiStat, err = txn.Stat(dbi)
		return
	})
	if err != nil {
		log.WithFields(log.Fields{
			"err":    err,
			"dbName": dbName,
		}).Fatal("failed to get stats for LMDB database")
	}
	log.WithFields(log.Fields{
		"dbiStat": dbiStat,
		"dbName":  dbName,
	}).Info("opened LMDB database")
	return
}

func init() {
}
