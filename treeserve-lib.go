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
	log "github.com/Sirupsen/logrus"
	"strings"
	"encoding/base64"
	"fmt"
	"strconv"
	//	"math/big"
	"crypto/md5"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"path"
	"golang.org/x/sync/errgroup"
	"os"
	"compress/gzip"
	"bufio"
	"bytes"
)

// Types & Structures
type PathCheck func(string) bool
type NodeVisitor func(nodeKey NodeKey) error
type NodeKey [16]byte
type TreeServe struct {
	LMDBPath string
	LMDBMapSize int64
	CostReferenceTime int64
	NodesCreatedInfoEveryN int64
	FileCategoryPathChecks map[string]PathCheck
	LMDBEnv *lmdb.Env
	TreeServeDBI lmdb.DBI
	TreeNodesDBI lmdb.DBI
	ChildrenDBI lmdb.DBI
	StatMappingsDBI lmdb.DBI
	NodesCreated int64
	StopAfterNLines int64
	Debug bool
}

func NewTreeServe(lmdbPath string, lmdbMapSize int64, costReferenceTime int64, nodesCreatedInfoEveryN int64, stopAfterNLines int64, debug bool) (ts *TreeServe) {
	ts = new(TreeServe)
	ts.LMDBPath = lmdbPath
	ts.LMDBMapSize = lmdbMapSize
	ts.CostReferenceTime = costReferenceTime
	ts.NodesCreatedInfoEveryN = nodesCreatedInfoEveryN
	ts.StopAfterNLines = stopAfterNLines
	ts.Debug = debug
	ts.SetFileCategoryPathChecks()
	return ts
}

func (ts *TreeServe) SetFileCategoryPathChecks() {
	// TODO move to externally specifiable JSON?
	ts.FileCategoryPathChecks = make(map[string]PathCheck)
	ts.FileCategoryPathChecks["cram"] = func(path string) bool {return strings.HasSuffix(path, ".cram")}
	ts.FileCategoryPathChecks["bam"] = func(path string) bool {return strings.HasSuffix(path, ".bam")}
	ts.FileCategoryPathChecks["index"] = func(path string) bool {
		for _, ending := range []string {".crai",".bai",".sai",".fai",".csi"} {
			if strings.HasSuffix(path, ending) {
				return true
			}
		}
		return false
	}
	ts.FileCategoryPathChecks["compressed"] = func(path string) bool {
		for _, ending := range []string {".bzip2", ".gz", ".tgz", ".zip", ".xz", ".bgz", ".bcf"} {
			if strings.HasSuffix(path, ending) {
				return true
			}
		}
		return false
	}
	ts.FileCategoryPathChecks["uncompressed"] = func(path string) bool {
		for _, ending := range []string {".sam", ".fasta", ".fastq", ".fa", ".fq", ".vcf", ".csv", ".tsv", ".txt", ".text", "README"} {
			if strings.HasSuffix(path, ending) {
				return true
			}
		}
		return false
	}
	ts.FileCategoryPathChecks["checkpoint"] = func(path string) bool {return strings.HasSuffix(path, "jobstate.context")}
	ts.FileCategoryPathChecks["temporary"] = func(path string) bool {
		for _, containing := range []string {"tmp", "temp"} {
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
			"ts": ts,
		}).Fatal("failed to set LMDB environment map size")
	}
	err = ts.LMDBEnv.SetMaxDBs(4)
	if err != nil {
		log.WithFields(log.Fields{"err": err}).Fatal("failed to set LMDB environment max DBs")
	}
	err = ts.LMDBEnv.Open(ts.LMDBPath, (lmdb.MapAsync | lmdb.WriteMap | lmdb.NoSubdir), 0600)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"ts": ts,
		}).Fatal("failed to open LMDB environment")
	}

	ts.TreeNodesDBI, err = ts.openLMDBDBI(ts.LMDBEnv, "treenodes", lmdb.Create)
	if ts.Debug {
		log.WithFields(log.Fields{"ts": ts}).Debug("opened nodes database") 
	}

	ts.ChildrenDBI, err = ts.openLMDBDBI(ts.LMDBEnv, "children", (lmdb.Create | lmdb.DupSort | lmdb.DupFixed))
	if ts.Debug {
		log.WithFields(log.Fields{"ts": ts}).Debug("opened children database") 
	}

	ts.StatMappingsDBI, err = ts.openLMDBDBI(ts.LMDBEnv, "statMappings", lmdb.Create)
	if ts.Debug {
		log.WithFields(log.Fields{"ts": ts}).Debug("opened statMappings database") 
	}

	ts.TreeServeDBI, err = ts.openLMDBDBI(ts.LMDBEnv, "treeServe", lmdb.Create)
	if ts.Debug {
		log.WithFields(log.Fields{"ts": ts}).Debug("opened treeServe database") 
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

func (ts *TreeServe) makeCategories(path string, fileType string) (categories []string) {
	for tag,f := range ts.FileCategoryPathChecks {
		if f(strings.ToLower(path)) {
			categories = append(categories, tag)
		}
	}
	categories = append(categories, "*")
	categories = append(categories, fmt.Sprintf("type_%s", fileType))
	return
}

func (ts *TreeServe) getPathKey(path string) (pathKey NodeKey) {
	pathKey = NodeKey(md5.Sum([]byte(path)))
	return
}

func (ts *TreeServe) GetTreeNode(nodeKey NodeKey) (treeNode TreeNode, err error) {
	var treeNodeData []byte
	err = ts.LMDBEnv.View(func(txn *lmdb.Txn) (err error) {
		treeNodeData, err = txn.Get(ts.TreeNodesDBI, nodeKey[:])
		return
	})
	if lmdb.IsNotFound(err) {
		return 
	} else if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Fatal("failed to get node from ts.TreeServeDBI")
	}
	_, err = treeNode.Unmarshal(treeNodeData)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"treeNodeData": treeNodeData,
		}).Fatal("failed to unmarshal treeNodeData")
	}
	return
}

func (ts *TreeServe) ensureDirectoryInTree(dirPath string) (dirPathKey NodeKey, err error) {
	dirPathKey = ts.getPathKey(dirPath)
	if ts.Debug { 
		log.WithFields(log.Fields{
			"dirPath": dirPath,
			"dirPathKey": dirPathKey,
			"ts.LMDBEnv": ts.LMDBEnv,
			"ts.TreeNodesDBI": ts.TreeNodesDBI,
		}).Debug("entered ensureDirectoryInTree()") 
	}
	err = ts.LMDBEnv.View(func(txn *lmdb.Txn) (err error) {
		_, err = txn.Get(ts.TreeNodesDBI, dirPathKey[:])
		return
	})
	if lmdb.IsNotFound(err) {
		if ts.Debug {
			log.WithFields(log.Fields{
				"dirPath": dirPath,
			}).Debug("parent does not exist, creating") 
		}
		err = ts.createTreeNode(dirPath, "d", NodeStats{})
		if err != nil {
			log.WithFields(log.Fields{
				"dirPath": dirPath,
				"err": err,
			}).Fatal("failed to create tree node")
		}
	} else if err != nil {
		log.WithFields(log.Fields{
			"dirPath": dirPath,
			"err": err,
		}).Fatal("failed to get parent from ts.TreeNodesDBI")
	}
	return
}

func (ts *TreeServe) addChildToParent(parentKey NodeKey, nodeKey NodeKey) (err error) {
	err = ts.LMDBEnv.Update(func(txn *lmdb.Txn) (err error) {
		err = txn.Put(ts.ChildrenDBI, parentKey[:], nodeKey[:], lmdb.NoDupData)
		return
	})
	if lmdb.IsErrno(err, lmdb.KeyExist) {
		if ts.Debug {
			log.WithFields(log.Fields{
				"parentKey": parentKey,
				"nodeKey": nodeKey,
			}).Debug("node is already a child of parent") 
		}
		err = nil
	} else if err != nil {
		log.WithFields(log.Fields{
			"parentKey": parentKey,
			"nodeKey": nodeKey,
			"err": err,
		}).Error("failed to add child to parent node")
	}
	return
}

func (ts *TreeServe) createTreeNode(nodePath string, fileType string, nodeStats NodeStats) (err error) {
	nodeKey := ts.getPathKey(nodePath)
	var parentPath string
	var parentKey NodeKey
	if nodePath != "/" {
		parentPath = path.Dir(nodePath)
		parentKey, err = ts.ensureDirectoryInTree(parentPath)
		if err != nil {
			log.WithFields(log.Fields{
				"parentPath": parentPath,
				"err": err,
			}).Error("failed to ensure parent directory in tree")
			return
		}
	}
	node := TreeNode{nodePath, parentKey, nodeStats}
	//nodeData, err := json.Marshal(node)
	nodeData, err := node.Marshal(nil)
	if err != nil {
		log.WithFields(log.Fields{
			"nodePath": nodePath,
			"parentKey": parentKey,
			"err": err,
		}).Error("failed to marshall TreeNode")
		return
	}
	if ts.Debug {
		log.WithFields(log.Fields{
			"nodePath": nodePath,
			"nodeKey": nodeKey,
			"nodeData": nodeData,
		}).Debug("creating node") 
	}
	err = ts.LMDBEnv.Update(func(txn *lmdb.Txn) (err error) {
		// check if node already exists
		// THIS MUST BE DONE INSIDE WRITE TRANSACTION OR WE COULD ACCIDENTALLY OVERWRITE NODE
		existingData, err := txn.Get(ts.TreeNodesDBI, nodeKey[:])
		if err == nil {
			if ts.Debug {
				log.WithFields(log.Fields{
					"nodeKey": nodeKey,
				}).Debug("node already exists in LMDB tree") 
			}
			var existing TreeNode
			//err = json.Unmarshal(existingData, &existing)
			_, err = existing.Unmarshal(existingData)
			if err != nil {
				log.WithFields(log.Fields{
					"existingData": existingData,
					"err": err,
				}).Error("failed to unmarshall existing node data")
				return
			}
			if existing.Name != node.Name {
				log.WithFields(log.Fields{
					"existing": existing,
					"node": node,
				}).Error("existing node Name mismatch")
			}
			if existing.ParentKey != node.ParentKey {
				log.WithFields(log.Fields{
					"existing": existing,
					"node": node,
				}).Error("existing node ParentKey mismatch")
			}
			return
		}
		err = txn.Put(ts.TreeNodesDBI, nodeKey[:], nodeData, 0)
		if err != nil  {
			log.WithFields(log.Fields{
				"nodePath": nodePath,
				"nodeKey": nodeKey,
				"err": err,
			}).Error("failed to create node in tree")
			return
		}
		if ts.Debug {
			log.WithFields(log.Fields{
				"nodePath": nodePath,
				"nodeKey": nodeKey,
				"nodeData": nodeData,
			}).Debug("new node created") 
		}
		return
	})
	if nodePath != "/" {
		if ts.Debug {
			log.WithFields(log.Fields{
				"nodePath": nodePath,
				"nodeKey": nodeKey,
				"parentPath": parentPath,
				"parentKey": parentKey,
			}).Debug("adding node to parent") 
		}
		err = ts.addChildToParent(parentKey, nodeKey)
		if err != nil {
			log.WithFields(log.Fields{
				"parentKey": parentKey,
				"nodeKey": nodeKey,
				"err": err,
			}).Error("failed to add node to parent")
			return
		}
	}
	ts.NodesCreated++
	if ts.NodesCreated % ts.NodesCreatedInfoEveryN == 0 {
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
			"line": line,
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
			"nodePath": nodePath,
			"nodeStats": nodeStats,
		}).Debug("parsed line and populated nodeStats") 
	}

	//	user := ts.lookupUid(uid)
	//	group := ts.lookupGid(gid)
	categories := ts.makeCategories(nodePath, fileType)
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
	if ts.Debug {
		log.WithFields(log.Fields{
			//		"nodePath": nodePath,
			//		"user": user,
			//		"group": group,
			"categories": categories,
			//		"accessTimeByteSeconds": accessTimeByteSeconds.Text(10),
			//		"modificationTimeByteSeconds": modificationTimeByteSeconds.Text(10),
			//		"creationTimeByteSeconds": creationTimeByteSeconds.Text(10),
		}).Debug("Calculated values") 
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
				"line": line,
				"err": err,
			}).Error("InputWorker failed to process line")
			break
		}
	}
	if ts.Debug {
		log.WithFields(log.Fields{
			"workerId": workerId,
			"err": err,
		}).Debug("leaving InputWorker()") 
	}
	return err
}

func (ts *TreeServe) FinalizeWorker(workerId int, subtreeNodes <-chan NodeKey) (err error) {
	if ts.Debug {
		log.WithFields(log.Fields{
			"workerId": workerId,
		}).Debug("entered FinalizeWorker()") 
	}
	for nodeKey := range subtreeNodes {
		var node TreeNode
		node, err = ts.GetTreeNode(nodeKey)
		if err != nil {
			log.WithFields(log.Fields{
				"workerId": workerId,
				"nodeKey": nodeKey,
			}).Error("failed to get tree node")
			return
		}
		if ts.Debug {
			log.WithFields(log.Fields{
				"workerId": workerId,
				"nodeKey": nodeKey,
				"node": node,
			}).Debug("FinalizeWorker processing subtree") 
		}
	}
	if ts.Debug {
		log.WithFields(log.Fields{
			"workerId": workerId,
			"err": err,
		}).Debug("leaving FinalizeWorker()") 
	}
	return err
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
			"state": state,
			"stateData": stateData,
			"err": err,
		}).Fatal("failed to set state in ts.TreeServeDBI")
	}
	return
}

func (ts *TreeServe) ProcessInput(inputPath string, workers int) (err error) {
	if ts.Debug {
		log.WithFields(log.Fields{
			"ts": ts,
			"inputPath": inputPath,
			"workers": workers,
		}).Debug("entered ProcessInput()") 
	}
	var inputWorkerGroup errgroup.Group
	lines := make(chan string, workers * 10)
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
			"err": err,
			"inputPath": inputPath,
		}).Fatal("Error opening input")
	}
	defer inputFile.Close()

	gzipReader, err := gzip.NewReader(inputFile)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
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
		if ts.StopAfterNLines >= 0 && lineCount > ts.StopAfterNLines {
			break
		}
	}
	if err := lineScanner.Err(); err != nil {
		log.WithFields(log.Fields{
			"err": err,
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

func (ts *TreeServe) getChildKeys(nodeKey NodeKey) (childKeys []NodeKey, err error) {
	if ts.Debug {
		log.WithFields(log.Fields{
			"nodeKey": nodeKey,
		}).Debug("about to start read transaction") 
	}
	err = ts.LMDBEnv.View(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(ts.ChildrenDBI)
		if err != nil {
			log.WithFields(log.Fields{"err": err}).Error("failed to open ChildrenDBI cursor")
			return
		}
		defer cur.Close()

		if ts.Debug {
			log.WithFields(log.Fields{
				"nodeKey": nodeKey,
			}).Debug("moving cursor to start path") 
		}
		//_, firstChildKey, err := cur.Get(nodeKey[:], nil, lmdb.Set)
		_, firstChildKey, err := cur.Get(nodeKey[:], nil, lmdb.Set)
		if lmdb.IsNotFound(err) {
			if ts.Debug {
				log.WithFields(log.Fields{
					"err": err,
					"nodeKey": nodeKey,
				}).Debug("no children found") 
			}
			err = nil
			return 
		}
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
				"nodeKey": nodeKey,
			}).Error("failed to get children")
		}
		stride := len(firstChildKey)
		if ts.Debug {
			log.WithFields(log.Fields{
				"nodeKey": nodeKey,
				"stride": stride,
				"firstChildKey": firstChildKey,
			}).Debug("have stride, getting children") 
		}
		var childKey NodeKey
		//key, values, err := cur.Get(nodeKey[:], nil, (lmdb.Set | lmdb.GetMultiple))
		key, values, err := cur.Get(nil, nil, lmdb.NextMultiple)
		if lmdb.IsNotFound(err) {
			log.Debug("nextmultiple not found, this node only has one child")
			copy(childKey[:], firstChildKey)
			childKeys = append(childKeys, childKey)
			err = nil
			return
		}
		childCount := 0
		childPageCount := 0
		for {
			if lmdb.IsNotFound(err) {
				if ts.Debug {
					log.WithFields(log.Fields{
						"key": key,
						"values": values,
						"err": err,
						"childCount": childCount,
						"childPageCount": childPageCount,
					}).Debug("no more sets of children") 
				}
				err = nil
				break
			}
			if err != nil {
				log.WithFields(log.Fields{
					"err": err,
					"nodeKey": nodeKey,
					"key": key,
				}).Fatal("failed to iterate over children")
			}
			if !bytes.Equal(key, nodeKey[:]) {
				if ts.Debug {
					log.WithFields(log.Fields{
						"nodeKey": nodeKey,
						"key": key,
						"childPageCount": childPageCount,
						"childCount": childCount,
					}).Debug("got unexpected key") 
				}
				break
			}
			multi := lmdb.WrapMulti(values, stride)
			if ts.Debug {
				log.WithFields(log.Fields{
					"multi": multi,
					"multi.Len()": multi.Len(),
					"values": values,
					"stride": stride,
				}).Debug("have wrapped multi") 
			}
			for i := 0; i < multi.Len(); i++ {
				childCount++
				copy(childKey[:], multi.Val(i))
				if ts.Debug {
					log.WithFields(log.Fields{
						"childKey": childKey,
						"i": i,
						"key": key,
						"childCount": childCount,
					}).Debug("got child, appending") 
				}
				childKeys = append(childKeys, childKey)
			}
			key, values, err = cur.Get(nil, nil, lmdb.NextMultiple)
			childPageCount++
		}
		return
	})
	return
}

func (ts *TreeServe) Finalize(startPath string, workers int) (err error) {
	if ts.Debug {
		log.WithFields(log.Fields{
			"ts": ts,
			"startPath": startPath,
			"workers": workers,
		}).Debug("entered Finalize()") 
	}

	var finalizeWorkerGroup errgroup.Group
	subtreeNodes := make(chan NodeKey, workers * 10)
	for workerId := 1; workerId <= workers; workerId++ {
		if ts.Debug {
			log.WithFields(log.Fields{
				"workerId": workerId,
			}).Debug("Starting goroutine for FinalizeWorker") 
		}
		finalizeWorkerGroup.Go(func() (err error) {
			err = ts.FinalizeWorker(workerId, subtreeNodes)
			return err
		})
	}
	// TODO actually dispatch subtree work on channel
	err = ts.aggregateSubtreePath(startPath, func(nodeKey NodeKey) (err error) {
		treeNode, err := ts.GetTreeNode(nodeKey)
		if err != nil {
			log.WithFields(log.Fields{
				"nodeKey": nodeKey,
				"err": err,
			}).Error("visitor failed to get tree node")
			return
		}
		if ts.Debug {
			
			log.WithFields(log.Fields{
				"treeNode.Name": treeNode.Name,
			}).Debug("visited node")
			
		}
		return
	})
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"startPath": startPath,
		}).Error("failed to aggregate subtree at startPath")
	}	
	close(subtreeNodes)

	log.Debug("waiting for FinalizeWorkers to complete")
	if err := finalizeWorkerGroup.Wait(); err != nil {
		log.WithFields(log.Fields{"err": err}).Fatal("one or more FinalizeWorkers failed")
	} else {
		log.Info("FinalizeWorkers successfully processed all subtree nodes")
	}
	
	return
}

func (ts *TreeServe) aggregateSubtreePath(subtreePath string, nodeVisitor NodeVisitor) (err error) {
	subtreeNode := ts.getPathKey(subtreePath)
	err = ts.aggregateSubtree(subtreeNode, nodeVisitor)
	return 
}

func (ts *TreeServe) aggregateSubtree(node NodeKey, nodeVisitor NodeVisitor) (err error) {
	childKeys, err := ts.getChildKeys(node)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"node": node,
		}).Error("failed to get child keys for node")
	}
	if ts.Debug {
		log.WithFields(log.Fields{
			"node": node,
			"childKeys": childKeys,
		}).Debug("got children for node") 
	}
	for _, childKey := range childKeys {
		if ts.Debug {
			log.WithFields(log.Fields{
				"node": node,
				"childKey": childKey,
				"childKeys": childKeys,
			}).Debug("aggregateSubtree recursing") 
		}
		ts.aggregateSubtree(childKey, nodeVisitor)
	}
	err = nodeVisitor(node)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"node": node,
		}).Error("nodeVisitor failed")
		return
	}
	return
}

func (ts *TreeServe) openLMDBDBI(lmdbEnv *lmdb.Env, dbName string, flags uint) (dbi lmdb.DBI, err error) {
	if ts.Debug {
		log.WithFields(log.Fields{
			"lmdbEnv": lmdbEnv,
			"dbName": dbName,
		}).Debug("Opening (creating if necessary) the LMDB dbi") 
	}
	err = lmdbEnv.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenDBI(dbName, flags)
		return
	})
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
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
			"err": err,
			"dbName": dbName,
		}).Fatal("failed to get stats for LMDB database")
	}
	log.WithFields(log.Fields{
		"dbiStat": dbiStat,
		"dbName": dbName,
	}).Info("opened LMDB database")
	return
}

func init() {
}

