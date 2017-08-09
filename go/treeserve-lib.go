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
	"math/big"
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

//type NodeVisitor func(nodeKey *Md5Key) (*AggregateStats, error)
type FinalizeWork struct {
	SubtreeNode *Md5Key
	Depth       int
	Results     chan *AggregateStats
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
	ChildrenDB               KeySetDB  // maps Md5Key to set of child Md5Keys
	StatMappingsDB           KeySetDB  // maps Md5Key to set of statMapping Md5Keys
	AggregateSizeDB          GenericDB // maps Md5Key to aggregated size for that node
	AggregateCountDB         GenericDB // maps Md5Key to aggregated count for that node
	AggregateCreateCostDB    GenericDB // maps Md5Key to aggregated cost since created for that node
	AggregateModifyCostDB    GenericDB // maps Md5Key to aggregated cost since modified for that node
	AggregateAccessCostDB    GenericDB // maps Md5Key to aggregated cost since accessed for that node
	NodesCreated             int64
	NodesFinalized           int64
	StopInputAfterNLines     int64
	StopFinalizeAfterNNodes  int64
	Debug                    bool
}

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
			"ts":      ts,
			"nodeKey": nodeKey,
		}).Error("failed to get tree node")
	}
	treeNode = dbData.(*TreeNode)
	return
}

func (ts *TreeServe) GetStatMapping(statMappingKey *Md5Key) (treeNode *StatMapping, err error) {
	dbData, err := ts.StatMappingDB.Get(statMappingKey)
	if err != nil {
		log.WithFields(log.Fields{
			"ts":             ts,
			"statMappingKey": statMappingKey,
		}).Error("failed to get tree node")
	}
	treeNode = dbData.(*StatMapping)
	return
}

func (ts *TreeServe) ensureDirectoryInTree(dirPath string) (dirPathKey *Md5Key, err error) {
	dirPathKey = ts.getPathKey(dirPath)
	if ts.Debug {
		log.WithFields(log.Fields{
			"dirPath":    dirPath,
			"dirPathKey": dirPathKey,
			"ts.LMDBEnv": ts.LMDBEnv,
		}).Debug("entered ensureDirectoryInTree()")
	}
	haveDir, err := ts.TreeNodeDB.HasKey(dirPathKey)
	if err != nil {
		log.WithFields(log.Fields{
			"err":        err,
			"dirPath":    dirPath,
			"dirPathKey": dirPathKey,
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

func (ts *TreeServe) addChildToParent(parentKey *Md5Key, nodeKey *Md5Key) (err error) {
	err = ts.ChildrenDB.AddKeyToKeySet(parentKey, nodeKey)
	if err != nil {
		log.WithFields(log.Fields{
			"parentKey": parentKey,
			"nodeKey":   nodeKey,
			"err":       err,
		}).Error("failed to add child node to parent")
	}
	return
}

func (ts *TreeServe) GetChildren(nodeKey *Md5Key) (children []*Md5Key, err error) {
	dbDataSet, err := ts.ChildrenDB.GetKeySet(nodeKey)
	if err != nil {
		log.WithFields(log.Fields{
			"ts":      ts,
			"nodeKey": nodeKey,
		}).Error("failed to get children")
		return
	}
	for _, dbData := range dbDataSet {
		children = append(children, (dbData).(*Md5Key))
	}
	return
}

func (ts *TreeServe) GetTags(treeNode *TreeNode) (categories []string) {
	for tag, f := range ts.FileCategoryPathChecks {
		if f(strings.ToLower(treeNode.Name)) {
			categories = append(categories, tag)
		}
	}
	categories = append(categories, "*")
	categories = append(categories, fmt.Sprintf("type_%s", treeNode.Stats.FileType))
	return
}

func (ts *TreeServe) createTreeNode(nodePath string, fileType string, nodeStats NodeStats) (err error) {
	nodeKey := ts.getPathKey(nodePath)
	var parentPath string
	var parentKey *Md5Key = &Md5Key{}
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
		if ts.Debug {
			log.WithFields(log.Fields{
				"existingDbData":   existingDbData,
				"node":             node,
				"nodeKey.String()": nodeKey.String(),
				"nodePath":         nodePath,
			}).Debug("createTreeNode update")
		}
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
				"nodePath":   nodePath,
				"nodeKey":    nodeKey,
				"parentPath": parentPath,
				"parentKey":  parentKey,
			}).Debug("adding node to parent")
		}
		err = ts.addChildToParent(parentKey, nodeKey)
		if err != nil {
			log.WithFields(log.Fields{
				"parentKey": parentKey,
				"nodeKey":   nodeKey,
				"err":       err,
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

func (ts *TreeServe) processLine(line string) (err error) {
	if ts.Debug {
		log.WithFields(log.Fields{
			"line": line,
		}).Debug("entered processLine()")
	}
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
	if ts.Debug {
		log.WithFields(log.Fields{
			"nodePath":  nodePath,
			"nodeStats": nodeStats,
		}).Debug("parsed line and populated nodeStats")
	}

	ts.createTreeNode(nodePath, fileType, nodeStats)

	return err
}

func (ts *TreeServe) InputWorker(workerId int, lines <-chan string) (err error) {
	if ts.Debug {
		log.WithFields(log.Fields{
			"workerId": workerId,
		}).Debug("entered InputWorker()")
	}
	for line := range lines {
		err = ts.processLine(line)
		if err != nil {
			log.WithFields(log.Fields{
				"workerId": workerId,
				"line":     line,
				"err":      err,
			}).Error("InputWorker failed to process line")
			break
		}
	}
	if ts.Debug {
		log.WithFields(log.Fields{
			"workerId": workerId,
			"err":      err,
		}).Debug("leaving InputWorker()")
	}
	return err
}

func (ts *TreeServe) FinalizeWorker(ctx context.Context, workerId int, finalizeWorkQueue chan *FinalizeWork, nodesFinalized chan *Md5Key) (err error) {
	if ts.Debug {
		log.WithFields(log.Fields{
			"workerId": workerId,
		}).Debug("entered FinalizeWorker()")
	}
	var work *FinalizeWork
	for {
		if ts.Debug {
			log.WithFields(log.Fields{
				"workerId": workerId,
			}).Info("FinalizeWorker() waiting for work from finalizeWorkQueue or cancellation")
		}
		select {
		case <-ctx.Done():
			if ts.Debug {
				log.WithFields(log.Fields{
					"workerId":  workerId,
					"ctx.Err()": ctx.Err(),
				}).Debug("FinalizeWorker() canceled by context")
			}
			if ctx.Err() == context.Canceled {
				return nil
			} else {
				return ctx.Err()
			}
			return ctx.Err()
		case work = <-finalizeWorkQueue:
			if work == nil {
				if ts.Debug {
					log.WithFields(log.Fields{
						"workerId": workerId,
					}).Debug("FinalizeWorker() canceled by finalizeWorkQueue closure")
				}
				return nil
			}
		}
		if ts.Debug {
			log.WithFields(log.Fields{
				"workerId": workerId,
				"work":     work,
			}).Debug("FinalizeWorker() processing work")
		}
		log.WithFields(log.Fields{
			"workerId": workerId,
		}).Info("FinalizeWorker() processing work")
		// if ctx.Value("workers").(int) < 2 {
		// 	// don't let aggregateSubtree put work on finalizeWorkQueue
		// 	log.WithFields(log.Fields{
		// 		"workerId": workerId,
		// 		"work":     work,
		// 		"workers":  ctx.Value("workers").(int),
		// 	}).Info("Not enough finalizeWorkers, disabling finalizeWorkQueue")
		// 	finalizeWorkQueue = nil
		// }
		err = ts.aggregateSubtree(ctx, workerId, work, finalizeWorkQueue, nodesFinalized)
		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				if ts.Debug {
					log.WithFields(log.Fields{
						"ctx.Err()": ctx.Err(),
						"workerId":  workerId,
					}).Debug("aggregateSubtree appears to have been canceled by context, terminating worker")
				}
				return nil
			}
		}
		if err != nil {
			log.WithFields(log.Fields{
				"workerId": workerId,
				"work":     work,
				"err":      err,
			}).Error("FinalizeWorker() aggregateSubtree failed")
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
	if ts.Debug {
		log.WithFields(log.Fields{
			"ts":        ts,
			"inputPath": inputPath,
			"workers":   workers,
		}).Debug("entered ProcessInput()")
	}
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
	for workerId := 1; workerId <= workers; workerId++ {
		if ts.Debug {
			log.WithFields(log.Fields{
				"workerId": workerId,
			}).Debug("Starting goroutine for InputWorker")
		}
		inputWorkerGroup.Go(func() (err error) {
			err = ts.InputWorker(workerId, lines)
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
	var lineCount int64 = 0
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

func (ts *TreeServe) GetAggregateKeys(nodeKey *Md5Key, statMappingKey *Md5Key) (aggregateKey *Md5Key, localAggregateKey *Md5Key, err error) {
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

func (ts *TreeServe) AddAggregateStatsOLD(nodeKey *Md5Key, statMappingKey *Md5Key, nodeStats *NodeStats) (err error) {
	aggregateKey, localAggregateKey, err := ts.GetAggregateKeys(nodeKey, statMappingKey)
	if err != nil {
		log.WithFields(log.Fields{
			"err":            err,
			"ts":             ts,
			"nodeKey":        nodeKey,
			"statMappingKey": statMappingKey,
		}).Error("failed to get aggregate keys")
	}
	if ts.Debug {
		log.WithFields(log.Fields{
			"err":               err,
			"ts":                ts,
			"nodeKey":           nodeKey,
			"statMappingKey":    statMappingKey,
			"aggregateKey":      aggregateKey,
			"localAggregateKey": localAggregateKey,
		}).Debug("adding aggregate stats to db")
	}
	/*
		nodeStats.FileSize
		nodeStats.CreationTime
		nodeStats.ModificationTime
		nodeStats.AccessTime
	*/
	var nodeSize big.Int
	nodeSize.SetUint64(nodeStats.FileSize)
	if ts.Debug {
		log.WithFields(log.Fields{
			"nodeSize": nodeSize,
		}).Debug("have nodeSize, calling AggregateSizeDB.Update")
	}
	err = ts.AggregateSizeDB.Update(aggregateKey, func(existing BinaryMarshalUnmarshaler) (updated BinaryMarshalUnmarshaler, err error) {
		var updatedBigint *Bigint = NewBigint()
		if existing == nil {
			if ts.Debug {
				log.WithFields(log.Fields{
					"aggregateKey": aggregateKey,
					"nodeKey":      nodeKey,
					"nodeSize":     nodeSize,
				}).Debug("no existing AggregateSizeDB entry for aggregateKey, initializing to nodeSize")
			}
			updatedBigint.i = &nodeSize
		} else {
			if ts.Debug {
				log.WithFields(log.Fields{
					"aggregateKey": aggregateKey,
					"existing":     existing,
				}).Debug("have existing AggregateSizeDB entry for aggregateKey")
			}
			existingBigint := existing.(*Bigint)
			updatedBigint.i.Add(existingBigint.i, &nodeSize)
			if ts.Debug {
				log.WithFields(log.Fields{
					"aggregateKey":   aggregateKey,
					"nodeKey":        nodeKey,
					"nodeSize":       nodeSize,
					"existingBigint": existingBigint,
					"updatedBigint":  updatedBigint,
				}).Debug("have existing AggregateSizeDB entry for aggregateKey, added nodeSize to existing to yield updatedBigint")
			}
		}
		updated = updatedBigint
		if ts.Debug {
			log.WithFields(log.Fields{
				"aggregateKey":   aggregateKey,
				"nodeKey":        nodeKey,
				"statMappingKey": statMappingKey,
				"updated":        updated,
				"nodeStats":      nodeStats,
			}).Debug("AggregateSize updated")
		}
		return
	})
	if err != nil {
		log.WithFields(log.Fields{
			"err":          err,
			"aggregateKey": aggregateKey,
		}).Error("failed to update AggregateSize database")
	}
	/*
		var bigSize big.Int
		bigSize.SetUint64(size)
		var accessTimeByteSeconds big.Int
		accessTimeByteSeconds.Mul(&bigSize, big.NewInt(ts.CostReferenceTime - accessTime))
		var modificationTimeByteSeconds big.Int
		modificationTimeByteSeconds.Mul(&bigSize, big.NewInt(ts.CostReferenceTime - modificationTime))
		var creationTimeByteSeconds big.Int
		creationTimeByteSeconds.Mul(&bigSize, big.NewInt(ts.CostReferenceTime - creationTime))
	*/

	return
}

func (ts *TreeServe) AddStatMappingsOLD(nodeKey *Md5Key, treeNode *TreeNode) (smk *Md5KeySet, err error) {

	// tags := ts.GetTags(treeNode)
	// if ts.Debug {
	// 	log.WithFields(log.Fields{
	// 		"treeNode": treeNode,
	// 		"tags":     tags,
	// 	}).Debug("AddStatMappings got tags")
	// }
	// statMappingKeys := NewMd5KeySet()
	// for _, u := range []string{"*", strconv.FormatUint(treeNode.Stats.Uid, 10)} {
	// 	for _, g := range []string{"*", strconv.FormatUint(treeNode.Stats.Gid, 10)} {
	// 		for _, t := range tags {
	// 			statMapping := StatMapping{u, g, t}
	// 			if ts.Debug {
	// 				log.WithFields(log.Fields{
	// 					"treeNode":    treeNode,
	// 					"statMapping": statMapping,
	// 				}).Debug("AddStatMappings ensuring statmapping is in database")
	// 			}
	// 			var statMappingKey Md5Key
	// 			statMappingKey, err = ts.EnsureStatMapping(statMapping)
	// 			if err != nil {
	// 				log.WithFields(log.Fields{
	// 					"statMapping": statMapping,
	// 					"err":         err,
	// 				}).Error("failed to add statMapping to database")
	// 				return
	// 			}
	// 			if ts.Debug {
	// 				log.WithFields(log.Fields{
	// 					"treeNode":       treeNode,
	// 					"statMapping":    statMapping,
	// 					"nodeKey":        nodeKey,
	// 					"statMappingKey": statMappingKey,
	// 				}).Debug("AddStatMappings statMapping is in StatMapping database, adding nodeKey/statMappingKey to StatMappings database")
	// 			}
	// 			err = ts.StatMappingsDB.AddKeyToKeySet(nodeKey, &statMappingKey)
	// 			if err != nil {
	// 				log.WithFields(log.Fields{
	// 					"nodeKey":        nodeKey,
	// 					"statMappingKey": statMappingKey,
	// 					"err":            err,
	// 				}).Error("failed to add statMappingKey to node")
	// 			}
	// 			if ts.Debug {
	// 				log.WithFields(log.Fields{
	// 					"nodeKey":        nodeKey,
	// 					"statMappingKey": statMappingKey,
	// 				}).Debug("AddStatMappings added nodeKey / statMappingKey to StatMappings database")
	// 			}
	// 			statMappingKeys.Add(statMappingKey)
	// 			if ts.Debug {
	// 				log.WithFields(log.Fields{
	// 					"nodeKey":        nodeKey,
	// 					"statMappingKey": statMappingKey,
	// 				}).Debug("AddStatMappings added statMappingKey to StatMappingKeys")
	// 			}
	// 		}
	// 	}
	// }
	// smk = &statMappingKeys
	return
}

func (ts *TreeServe) EnsureStatMapping(statMappingKey Md5Key, statMapping StatMapping) (err error) {
	exists, err := ts.StatMappingDB.Exists(&statMappingKey)
	if err != nil {
		log.WithFields(log.Fields{
			"err":            err,
			"statMappingKey": statMappingKey,
		}).Error("failed to check whether statMappingKey exists in StatMapping database")
	}
	if !exists {
		// add with overwrite=true for efficiency
		err = ts.StatMappingDB.Add(&statMappingKey, &statMapping, true)
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Error("failed to ensure statmapping presence in database")
		}
	}
	// err = ts.StatMappingDB.Update(&statMappingKey, func(_ BinaryMarshalUnmarshaler) (sm BinaryMarshalUnmarshaler, err error) {
	// 	sm = &statMapping
	// 	return
	// })
	return
}

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

func (ts *TreeServe) Finalize(startPath string, workers int) (err error) {
	if ts.Debug {
		log.WithFields(log.Fields{
			"ts":        ts,
			"startPath": startPath,
			"workers":   workers,
		}).Debug("entered Finalize()")
	}
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
	workerIds := make(chan int)
	log.WithFields(log.Fields{
		"workers": workers,
	}).Info("starting finalize workers")
	workersStarted := 0
	for workerId := 1; workerId <= workers; workerId++ {
		if ts.Debug {
			log.WithFields(log.Fields{
				"workerId": workerId,
			}).Debug("Starting goroutine for FinalizeWorker")
		}
		finalizeWorkerGroup.Go(func() (err error) {
			id := <-workerIds
			err = ts.FinalizeWorker(ctx, id, finalizeWorkQueue, nodesFinalized)
			return err
		})
		workerIds <- workerId
		workersStarted++
	}
	log.WithFields(log.Fields{
		"workersStarted": workersStarted,
	}).Info("started finalize workers")

	startnode := ts.getPathKey(startPath)
	startnodeResults := make(chan *AggregateStats, 1)
	startnodeWork := FinalizeWork{SubtreeNode: startnode, Depth: 0, Results: startnodeResults}
	if ts.Debug {
		log.WithFields(log.Fields{
			"startPath":     startPath,
			"startnodeWork": startnodeWork,
		}).Debug("Finalize: submitting startnodeWork for startPath")
	}
	log.Info("Finalize: submitting initial finalizework to workers")
	finalizeWorkQueue <- &startnodeWork

	log.Info("Finalize: waiting for results")
WaitForResults:
	for {
		select {
		case results := <-startnodeResults:
			log.WithFields(log.Fields{
				"results.String()": results.String(),
			}).Info("Finalize got final results")
			break WaitForResults
		case node := <-nodesFinalized:
			if ts.Debug {
				treeNode, err := ts.GetTreeNode(node)
				if err != nil {
					log.WithFields(log.Fields{
						"node": node,
						"err":  err,
					}).Fatal("could not get node")
				}
				log.WithFields(log.Fields{
					"treeNode.Name": treeNode.Name,
				}).Debug("finalized node")
			}
			ts.NodesFinalized++
			if ts.StopFinalizeAfterNNodes >= 0 && ts.NodesFinalized > ts.StopFinalizeAfterNNodes {
				log.WithFields(log.Fields{
					"ts.NodesFinalized":          ts.NodesFinalized,
					"ts.StopFinalizeAfterNNodes": ts.StopFinalizeAfterNNodes,
				}).Warning("cancelling remaining work after reaching StopFinalizeAfterNNodes limit")
				break WaitForResults
			}
			if ts.NodesFinalized%ts.NodesFinalizedInfoEveryN == 0 {
				log.WithFields(log.Fields{
					"ts.NodesFinalized": ts.NodesFinalized,
				}).Info("finalized nodes")
			}
		}
	}

	log.Info("Finalize: cancelling FinalizeWorker context")
	cancel()

	// 	log.Info("Finalize: flushing finalizeWorkQueue")
	// FlushFinalizeWorkQueue:
	// 	for {
	// 		select {
	// 		// send nil on finalizeWorkQueue as long as anyone is listening
	// 		case finalizeWorkQueue <- nil:
	// 			log.Info("Finalize: sent nil on finalizeWorkQueue")
	// 		case <-finalizeWorkQueue:
	// 			log.Info("Finalize: received on finalizeWorkQueue")
	// 		default:
	// 			break FlushFinalizeWorkQueue
	// 		}
	// 	}

	// 	log.Info("Finalize: closing finalizeWorkQueue")
	// 	close(finalizeWorkQueue)

	log.Info("waiting for all FinalizeWorkers to complete")
	if err := finalizeWorkerGroup.Wait(); err != nil {
		log.WithFields(log.Fields{"err": err}).Fatal("one or more FinalizeWorkers failed")
	} else {
		log.Info("FinalizeWorkers successfully processed all subtree nodes")
	}

	return
}

func (ts *TreeServe) aggregateSubtree(ctx context.Context, workerId int, subtreeWork *FinalizeWork, finalizeWorkQueue chan *FinalizeWork, nodesFinalized chan *Md5Key) (err error) {
	node := subtreeWork.SubtreeNode
	//	nodeVisitor := subtreeWork.NodeVisitor
	level := subtreeWork.Depth
	if ts.Debug {
		log.WithFields(log.Fields{
			"node":     node.String(),
			"level":    level,
			"workerId": workerId,
		}).Debug("aggregateSubtree pre-order")
	}
	childKeys, err := ts.GetChildren(node)
	if err != nil {
		log.WithFields(log.Fields{
			"err":      err,
			"node":     node.String(),
			"workerId": workerId,
		}).Error("failed to get child keys for node")
	}
	if ts.Debug {
		log.WithFields(log.Fields{
			"node":           node.String(),
			"len(childKeys)": len(childKeys),
			"workerId":       workerId,
		}).Debug("got children for node, making results chan")
	}
	// if len(childKeys) > 4 {
	// 	childKeys = childKeys[0:4]
	// }
	// create results channel for this node's children
	childResults := make(chan *AggregateStats, len(childKeys))
	for _, childKey := range childKeys {
		if ts.Debug {
			log.WithFields(log.Fields{
				"node":     node,
				"childKey": childKey.String(),
				"level":    level,
				"workerId": workerId,
			}).Debug("aggregateSubtree in-order")
		}
		childWork := &FinalizeWork{SubtreeNode: childKey, Depth: level + 1, Results: childResults}
		if ts.Debug {
			log.WithFields(log.Fields{
				"node":      node,
				"childKey":  childKey.String(),
				"level":     level,
				"childWork": childWork,
				"workerId":  workerId,
			}).Debug("aggregateSubtree prepared childWork")
		}
		select {
		case <-ctx.Done():
			if ts.Debug {
				log.WithFields(log.Fields{
					"workerId":  workerId,
					"ctx.Err()": ctx.Err(),
				}).Debug("aggregateSubtree() canceled by context")
			}
			return ctx.Err()
		case finalizeWorkQueue <- childWork:
			// this child has been successfully queued for finalizeWorker processing
			if ts.Debug {
				log.WithFields(log.Fields{
					"workerId":  workerId,
					"childWork": childWork,
				}).Debug("aggregateSubtree() had finalizeWorkQueue slot for concurrent processing of childWork")
			}
		default:
			// could not be queued for concurrent processing, recurse using this goroutine
			if ts.Debug {
				log.WithFields(log.Fields{
					"workerId":  workerId,
					"childWork": childWork,
				}).Debug("aggregateSubtree() had no free concurrent processing slots, recursing within this worker to process childWork")
			}
			// log.WithFields(log.Fields{
			// 	"workerId": workerId,
			// }).Info("aggregateSubtree recursing within worker")
			err = ts.aggregateSubtree(ctx, workerId, childWork, finalizeWorkQueue, nodesFinalized)
			if err != nil {
				return
			}
		}
	}
	if ts.Debug {
		log.WithFields(log.Fields{
			"node":     node.String(),
			"level":    level,
			"workerId": workerId,
		}).Debug("aggregateSubtree post-order")
	}
	// calculate aggregate stats for this node itself
	aggregateStats, err := ts.CalculateAggregateStats(node)
	if ts.Debug {
		log.WithFields(log.Fields{
			"node":           node.String(),
			"level":          level,
			"aggregateStats": aggregateStats,
			"workerId":       workerId,
		}).Debug("calculated aggregate stats for node")
	}

	if ts.Debug {
		log.WithFields(log.Fields{
			"node":     node.String(),
			"level":    level,
			"workerId": workerId,
		}).Debug("aggregateSubtree waiting for childResults")
	}
	for i := range childKeys {
		if ts.Debug {
			log.WithFields(log.Fields{
				"node":           node.String(),
				"level":          level,
				"workerId":       workerId,
				"i":              i,
				"len(childKeys)": len(childKeys),
			}).Debug("aggregateSubtree waiting for i-th childResults")
		}
		var childAggregateStats *AggregateStats
	WaitForIthChildResults:
		for {
			select {
			case <-ctx.Done():
				if ts.Debug {
					log.WithFields(log.Fields{
						"node":     node.String(),
						"level":    level,
						"workerId": workerId,
						"i":        i,
					}).Debug("aggregateSubtree cancelled while waiting for i-th childResults")
				}
				return
			case childAggregateStats = <-childResults:
				break WaitForIthChildResults
			}
		}
		if ts.Debug {
			log.WithFields(log.Fields{
				"node":                node.String(),
				"level":               level,
				"workerId":            workerId,
				"childAggregateStats": childAggregateStats,
				"aggregateStats":      aggregateStats,
			}).Debug("aggregateSubtree adding childAggregateStats to aggregateStats")
		}
		aggregateStats.Add(childAggregateStats)
	}
	if ts.Debug {
		log.WithFields(log.Fields{
			"node":           node.String(),
			"level":          level,
			"aggregateStats": aggregateStats,
			"workerId":       workerId,
		}).Debug("added child aggregate stats to node")
	}

	if ts.Debug {
		log.WithFields(log.Fields{
			"node":           node.String(),
			"level":          level,
			"aggregateStats": aggregateStats,
			"workerId":       workerId,
		}).Debug("sending results for node on results channel")
	}
	subtreeWork.Results <- aggregateStats
	if ts.Debug {
		log.WithFields(log.Fields{
			"node":           node.String(),
			"level":          level,
			"aggregateStats": aggregateStats,
			"workerId":       workerId,
		}).Debug("sent results for node on results channel")
	}

	if ts.Debug {
		log.WithFields(log.Fields{
			"node":     node.String(),
			"level":    level,
			"workerId": workerId,
		}).Debug("sending node on nodesFinalized channel")
	}
	select {
	case <-ctx.Done():
		if ts.Debug {
			log.WithFields(log.Fields{
				"node":     node.String(),
				"level":    level,
				"workerId": workerId,
			}).Debug("aggregateSubtree cancelled while sending node on nodesFinalized")
		}
	case nodesFinalized <- node:
		if ts.Debug {
			log.WithFields(log.Fields{
				"node":     node.String(),
				"level":    level,
				"workerId": workerId,
			}).Debug("sent node on nodesFinalized channel")
		}
	}

	// create or update the parent node's  entries in the Aggregate* databases with this node's stats
	// parentKey := Md5Key(treeNode.ParentKey)
	// for _, statMappingKey := range statMappingKeys {
	// 	if ts.Debug {
	// 		log.WithFields(log.Fields{
	// 			"parentKey":      parentKey,
	// 			"statMappingKey": statMappingKey,
	// 			"treeNode.Stats": treeNode.Stats,
	// 		}).Debug("calling AddAggregateStats")
	// 	}
	// 	err = ts.AddAggregateStats(&parentKey, statMappingKey, &treeNode.Stats) // aggregated all the way up to root
	// 	if err != nil {
	// 		log.WithFields(log.Fields{
	// 			"err":            err,
	// 			"ts":             ts,
	// 			"treeNode":       treeNode,
	// 			"statMappingKey": statMappingKey,
	// 		}).Error("failed to add aggregate stats")
	// 	}
	// }
	// if err != nil {
	// 	log.WithFields(log.Fields{
	// 		"err":  err,
	// 		"node": node.String(),
	// 	}).Error("nodeVisitor failed")
	// 	return
	// }
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