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
	"math/big"
	"crypto/md5"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"path"
)

// Types & Structures
type PathCheck func(string) bool
type NodeKey [16]byte
type TreeServe struct {
	LMDBPath string
	LMDBMapSize int64
	CostReferenceTime int64
	NodesCreatedInfoEveryN int64
	FileCategoryPathChecks map[string]PathCheck
	LMDBEnv *lmdb.Env
	TreeDBI lmdb.DBI
	StatMappingsDBI lmdb.DBI
	NodesCreated int64
}

func NewTreeServe(lmdbPath string, lmdbMapSize int64, costReferenceTime int64, nodesCreatedInfoEveryN int64) (ts *TreeServe) {
	ts = new(TreeServe)
	ts.LMDBPath = lmdbPath
	ts.LMDBMapSize = lmdbMapSize
	ts.CostReferenceTime = costReferenceTime
	ts.NodesCreatedInfoEveryN = nodesCreatedInfoEveryN
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
	log.WithFields(log.Fields{"ts": ts}).Debug("configuring and opening LMDB environment")
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

	ts.TreeDBI, err = openLMDBDBI(ts.LMDBEnv, "tree")
	log.WithFields(log.Fields{
		"ts": ts,
	}).Debug("opened tree database")

	ts.StatMappingsDBI, err = openLMDBDBI(ts.LMDBEnv, "statMappings")
	log.WithFields(log.Fields{"ts": ts}).Debug("opened statMappings database")
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

func (ts *TreeServe) ensureDirectoryInTree(dirPath string) (dirPathKey NodeKey, err error) {
	dirPathKey = ts.getPathKey(dirPath)
	log.WithFields(log.Fields{
		"dirPath": dirPath,
		"dirPathKey": dirPathKey,
		"ts.LMDBEnv": ts.LMDBEnv,
		"ts.TreeDBI": ts.TreeDBI,
	}).Debug("entered ensureDirectoryInTree()")
	err = ts.LMDBEnv.View(func(txn *lmdb.Txn) (err error) {
		_, err = txn.Get(ts.TreeDBI, dirPathKey[:])
		return
	})
	if lmdb.IsNotFound(err) {
		log.WithFields(log.Fields{
			"dirPath": dirPath,
		}).Debug("parent does not exist, creating")
		err = ts.createTreeNode(dirPath, "d")
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
		}).Fatal("failed to get parent from ts.TreeDBI")
	}
	return
}

func (ts *TreeServe) addNodeToParent(parentKey NodeKey, nodeKey NodeKey) (err error) {
	err = ts.LMDBEnv.Update(func(txn *lmdb.Txn) (err error) {
		parentData, err := txn.Get(ts.TreeDBI, parentKey[:])
		if err != nil {
			log.WithFields(log.Fields{
				"parentKey": parentKey,
				"err": err,
			}).Error("failed to get parent node from LMDB")
		}
		var parent TreeNode
		//err = json.Unmarshal(parentData, &parent)
		_, err = parent.Unmarshal(parentData)
		if err != nil {
			log.WithFields(log.Fields{
				"parentData": parentData,
				"err": err,
			}).Error("failed to unmarshal parent node data")
		}
		parent.ChildrenKeys = append(parent.ChildrenKeys, nodeKey)
		//parentData, err = json.Marshal(parent)
		parentData, err = parent.Marshal(nil)
		log.WithFields(log.Fields{
			"len(parentData)": len(parentData),
		}).Debug("about to put updated parent node")
		err = txn.Put(ts.TreeDBI, parentKey[:], parentData, 0)
		if err != nil {
			log.WithFields(log.Fields{
				"parentKey": parentKey,
				"parentData": parentData,
				"err": err,
			}).Error("failed to put updated parent node into LMDB")
		}
		return
	})
	return
}

func (ts *TreeServe) createTreeNode(nodePath string, fileType string) (err error) {
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
	node := TreeNode{nodePath, parentKey, [][16]byte{}}
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
	log.WithFields(log.Fields{
		"nodePath": nodePath,
		"nodeKey": nodeKey,
		"nodeData": nodeData,
	}).Debug("creating node")
	err = ts.LMDBEnv.Update(func(txn *lmdb.Txn) (err error) {
		// check if node already exists
		// THIS MUST BE DONE INSIDE WRITE TRANSACTION OR WE COULD ACCIDENTALLY OVERWRITE NODE
		existingData, err := txn.Get(ts.TreeDBI, nodeKey[:])
		if err == nil {
			log.WithFields(log.Fields{
				"nodeKey": nodeKey,
			}).Debug("node already exists in LMDB tree")
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
		err = txn.Put(ts.TreeDBI, nodeKey[:], nodeData, 0)
		if err != nil  {
			log.WithFields(log.Fields{
				"nodePath": nodePath,
				"nodeKey": nodeKey,
				"err": err,
			}).Error("failed to create node in tree")
			return
		}
		log.WithFields(log.Fields{
			"nodePath": nodePath,
			"nodeKey": nodeKey,
			"nodeData": nodeData,
		}).Debug("new node created")
		return
	})
	if nodePath != "/" {
		log.WithFields(log.Fields{
			"nodePath": nodePath,
			"nodeKey": nodeKey,
			"parentPath": parentPath,
			"parentKey": parentKey,
		}).Debug("adding node to parent")
		err = ts.addNodeToParent(parentKey, nodeKey)
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
	log.WithFields(log.Fields{
		"line": line,
	}).Debug("entered processLine()")
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
	log.WithFields(log.Fields{
		"nodePath": nodePath,
		"size": size,
		"uid": uid,
		"gid": gid,
		"accessTime": accessTime,
		"modificationTime": modificationTime,
		"creationTime": creationTime,
		"fileType": fileType,
		//"iNode": iNode,
		//"linkCount": linkCount,
		//"devId": devId,
	}).Debug("Parsed line")

	user := ts.lookupUid(uid)
	group := ts.lookupGid(gid)
	categories := ts.makeCategories(nodePath, fileType)
	var bigSize big.Int
	bigSize.SetUint64(size)
	var accessTimeByteSeconds big.Int
	accessTimeByteSeconds.Mul(&bigSize, big.NewInt(ts.CostReferenceTime - accessTime))
	var modificationTimeByteSeconds big.Int
	modificationTimeByteSeconds.Mul(&bigSize, big.NewInt(ts.CostReferenceTime - modificationTime))
	var creationTimeByteSeconds big.Int
	creationTimeByteSeconds.Mul(&bigSize, big.NewInt(ts.CostReferenceTime - creationTime))

	log.WithFields(log.Fields{
		"nodePath": nodePath,
		"user": user,
		"group": group,
		"categories": categories,
		"accessTimeByteSeconds": accessTimeByteSeconds.Text(10),
		"modificationTimeByteSeconds": modificationTimeByteSeconds.Text(10),
		"creationTimeByteSeconds": creationTimeByteSeconds.Text(10),
	}).Debug("Calculated values")

	ts.createTreeNode(nodePath, fileType)

	return err
}

func (ts *TreeServe) InputWorker(id int, lines <-chan string) (err error) {
	log.WithFields(log.Fields{
		"id": id,
	}).Debug("entered InputWorker()")
	for line := range lines {
		err = ts.processLine(line)
		if err != nil {
			log.WithFields(log.Fields{
				"id": id,
				"line": line,
				"err": err,
			}).Error("InputWorker failed to process line")
			break
		}
	}
	log.WithFields(log.Fields{
		"id": id,
		"err": err,
	}).Debug("leaving InputWorker()")
	return err
}

func openLMDBDBI(lmdbEnv *lmdb.Env, dbName string) (dbi lmdb.DBI, err error) {
	log.WithFields(log.Fields{
		"lmdbEnv": lmdbEnv,
		"dbName": dbName,
	}).Debug("Opening (creating if necessary) the LMDB dbi")
	err = lmdbEnv.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenDBI(dbName, lmdb.Create)
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

