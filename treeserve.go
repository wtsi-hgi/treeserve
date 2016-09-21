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

package main

import (
	"flag"
	log "github.com/Sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"os"
	"compress/gzip"
	"bufio"
	"strings"
	"encoding/base64"
	"time"
	"fmt"
	"strconv"
	"math/big"
	"crypto/md5"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"path"
	"encoding/json"
	"runtime/pprof"
)

// Variables set by command-line flags
var inputPath string
var lmdbPath string
var inputWorkers int
var costTime int64
var lmdbMapSize int64
var debug bool

// Types & Structures
type PathCheck func(string) bool
type NodeKey [16]byte
type TreeNode struct {
	Name string
	ParentKey NodeKey
	ChildrenKeys []NodeKey
}

// Globals
var nodesCreated int64 = 0
var fileCategoryPathChecks map[string]PathCheck
var lmdbEnv *lmdb.Env
var treeDBI lmdb.DBI
var statMappingsDBI lmdb.DBI
var cpuProfilePath string


func init() {
	flag.StringVar(&inputPath, "inputPath", "input.dat.gz", "Input file")
	flag.StringVar(&lmdbPath, "lmdbPath", "/tmp/treeserve_lmdb", "Path to LMDB environment")
	flag.Int64Var(&lmdbMapSize, "lmdbMapSize", 200 * 1024 * 1024 * 1024, "LMDB map size (maximum)")
	flag.IntVar(&inputWorkers, "inputWorkers", 4, "Number of workers to use for processing lines of input data")
	flag.Int64Var(&costTime, "costTime", time.Now().Unix(), "The time to use for cost calculations in seconds since the epoch")
	flag.BoolVar(&debug, "debug", false, "Enable debug logging")
	flag.StringVar(&cpuProfilePath, "cpuProfilePath", "", "Write cpu profile to file")
}

func lookupUid(uid uint64) (user string) {
	// TODO implement
	user = strconv.FormatUint(uid, 10)
	return
}

func lookupGid(gid uint64) (group string) {
	// TODO implement
	group = strconv.FormatUint(gid, 10)
	return 
}

func makeCategories(path string, fileType string) (categories []string) {
	for tag,f := range fileCategoryPathChecks {
		if f(strings.ToLower(path)) {
			categories = append(categories, tag)
		}
	}
	categories = append(categories, "*")
	categories = append(categories, fmt.Sprintf("type_%s", fileType))
	return
}

func getPathKey(path string) (pathKey NodeKey) {
	pathKey = NodeKey(md5.Sum([]byte(path)))
	return
}

func ensureDirectoryInTree(dirPath string) (dirPathKey NodeKey, err error) {
	dirPathKey = getPathKey(dirPath)
	err = lmdbEnv.View(func(txn *lmdb.Txn) (err error) {
		txn.RawRead = true
		_, err = txn.Get(treeDBI, dirPathKey[:])
		return
	})
	if lmdb.IsNotFound(err) {
		log.WithFields(log.Fields{
			"dirPath": dirPath,
		}).Debug("parent does not exist, creating")
		err = createTreeNode(dirPath, "d")
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
		}).Fatal("failed to get parent from treeDBI")
	}
	return
}

func addNodeToParent(parentKey NodeKey, nodeKey NodeKey) (err error) {
	err = lmdbEnv.Update(func(txn *lmdb.Txn) (err error) {
		parentData, err := txn.Get(treeDBI, parentKey[:])
		if err != nil {
			log.WithFields(log.Fields{
				"parentKey": parentKey,
				"err": err,
			}).Error("failed to get parent node from LMDB")
		}
		var parent TreeNode
		err = json.Unmarshal(parentData, &parent)
		if err != nil {
			log.WithFields(log.Fields{
				"parentData": parentData,
				"err": err,
			}).Error("failed to unmarshall parent node data")
		}
		parent.ChildrenKeys = append(parent.ChildrenKeys, nodeKey)
		parentData, err = json.Marshal(parent)
		log.WithFields(log.Fields{
			"len(parentData)": len(parentData),
		}).Debug("about to put updated parent node")
		err = txn.Put(treeDBI, parentKey[:], parentData, 0)
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

func createTreeNode(nodePath string, fileType string) (err error) {
	nodeKey := getPathKey(nodePath)
	var parentPath string
	var parentKey NodeKey
	if nodePath != "/" {
		parentPath = path.Dir(nodePath)
		parentKey, err = ensureDirectoryInTree(parentPath)
		if err != nil {
			log.WithFields(log.Fields{
				"parentPath": parentPath,
				"err": err,
			}).Error("failed to ensure parent directory in tree")
			return
		}
	}
	node := TreeNode{nodePath, parentKey, []NodeKey{}}
	nodeData, err := json.Marshal(node)
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
	err = lmdbEnv.Update(func(txn *lmdb.Txn) (err error) {
		// check if node already exists
		// THIS MUST BE DONE INSIDE WRITE TRANSACTION OR WE COULD ACCIDENTALLY OVERWRITE NODE
		existingData, err := txn.Get(treeDBI, nodeKey[:])
		if err == nil {
			log.WithFields(log.Fields{
				"nodeKey": nodeKey,
			}).Debug("node already exists in LMDB tree")
			var existing TreeNode
			err = json.Unmarshal(existingData, &existing)
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
		err = txn.Put(treeDBI, nodeKey[:], nodeData, 0)
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
		err = addNodeToParent(parentKey, nodeKey)
		if err != nil {
			log.WithFields(log.Fields{
				"parentKey": parentKey,
				"nodeKey": nodeKey,
				"err": err,
			}).Error("failed to add node to parent")
			return
		}
	}
	nodesCreated++
	if nodesCreated % 1000 == 0 {
		log.WithFields(log.Fields{
			"nodesCreated": nodesCreated,
		}).Info("created nodes")
	}
	return
}

func processLine(line string) (err error) {
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

	user := lookupUid(uid)
	group := lookupGid(gid)
	categories := makeCategories(nodePath, fileType)
	var bigSize big.Int
	bigSize.SetUint64(size)
	var accessTimeByteSeconds big.Int
	accessTimeByteSeconds.Mul(&bigSize, big.NewInt(costTime - accessTime))
	var modificationTimeByteSeconds big.Int
	modificationTimeByteSeconds.Mul(&bigSize, big.NewInt(costTime - modificationTime))
	var creationTimeByteSeconds big.Int
	creationTimeByteSeconds.Mul(&bigSize, big.NewInt(costTime - creationTime))

	log.WithFields(log.Fields{
		"nodePath": nodePath,
		"user": user,
		"group": group,
		"categories": categories,
		"accessTimeByteSeconds": accessTimeByteSeconds.Text(10),
		"modificationTimeByteSeconds": modificationTimeByteSeconds.Text(10),
		"creationTimeByteSeconds": creationTimeByteSeconds.Text(10),
	}).Debug("Calculated values")

	createTreeNode(nodePath, fileType)

	return err
}

func inputWorker(id int, lines <-chan string) (err error) {
	log.WithFields(log.Fields{
		"id": id,
	}).Debug("entered inputWorker()")
	for line := range lines {
		err = processLine(line)
		if err != nil {
			log.WithFields(log.Fields{
				"id": id,
				"line": line,
				"err": err,
			}).Error("inputWorker failed to process line")
			break
		}
	}
	log.WithFields(log.Fields{
		"id": id,
		"err": err,
	}).Debug("leaving inputWorker()")
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

func main() {
	flag.Parse()
	if debug {
		log.SetLevel(log.DebugLevel)
	}
	if cpuProfilePath != "" {
		f, err := os.Create(cpuProfilePath)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	flag_fields := log.Fields{}
	flag.VisitAll(func(f *flag.Flag) {
		flag_fields[f.Name] = f.Value
	})
	log.WithFields(flag_fields).Debug("entered main()")

	// TODO move to externally specifiable JSON?
	fileCategoryPathChecks = make(map[string]PathCheck)
	fileCategoryPathChecks["cram"] = func(path string) bool {return strings.HasSuffix(path, ".cram")}
	fileCategoryPathChecks["bam"] = func(path string) bool {return strings.HasSuffix(path, ".bam")}
	fileCategoryPathChecks["index"] = func(path string) bool {
		for _, ending := range []string {".crai",".bai",".sai",".fai",".csi"} {
			if strings.HasSuffix(path, ending) {
				return true
			}
		}
		return false
	}
	fileCategoryPathChecks["compressed"] = func(path string) bool {
		for _, ending := range []string {".bzip2", ".gz", ".tgz", ".zip", ".xz", ".bgz", ".bcf"} {
			if strings.HasSuffix(path, ending) {
				return true
			}
		}
		return false
	}
	fileCategoryPathChecks["uncompressed"] = func(path string) bool {
		for _, ending := range []string {".sam", ".fasta", ".fastq", ".fa", ".fq", ".vcf", ".csv", ".tsv", ".txt", ".text", "README"} {
			if strings.HasSuffix(path, ending) {
				return true
			}
		}
		return false
	}
	fileCategoryPathChecks["checkpoint"] = func(path string) bool {return strings.HasSuffix(path, "jobstate.context")}
	fileCategoryPathChecks["temporary"] = func(path string) bool {
		for _, containing := range []string {"tmp", "temp"} {
			if strings.Contains(path, containing) {
				return true
			}
		}
		return false
	}

	var err error
	log.WithFields(log.Fields{
		"lmdbPath": lmdbPath,
		"lmdbMapSize": lmdbMapSize,
	}).Debug("Configuring and opening LMDB environment")
	lmdbEnv, err = lmdb.NewEnv()
	if err != nil {
		log.WithFields(log.Fields{"err": err}).Fatal("failed to create new LMDB environment")
	}
	err = lmdbEnv.SetMapSize(lmdbMapSize)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"lmdbMapSize": lmdbMapSize,
		}).Fatal("failed to set LMDB environment map size")
	}
	err = lmdbEnv.SetMaxDBs(4)
	if err != nil {
		log.WithFields(log.Fields{"err": err}).Fatal("failed to set LMDB environment max DBs")
	}
	err = lmdbEnv.Open(lmdbPath, (lmdb.MapAsync | lmdb.WriteMap | lmdb.NoSubdir), 0600)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"lmdbPath": lmdbPath,
		}).Fatal("failed to open LMDB environment")
	}
	defer lmdbEnv.Close()

	treeDBI, err = openLMDBDBI(lmdbEnv, "tree")
	log.WithFields(log.Fields{
		"treeDBI": treeDBI,
	}).Debug("opened tree database")

	statMappingsDBI, err = openLMDBDBI(lmdbEnv, "statMappings")
	log.WithFields(log.Fields{
		"statMappingsDBI": statMappingsDBI,
	}).Debug("opened statMappings database")

	log.WithFields(log.Fields{
		"inputWorkers": inputWorkers,
	}).Debug("starting inputWorker goroutines")
	var inputWorkerGroup errgroup.Group
	lines := make(chan string, inputWorkers * 10)
	for workerId := 1; workerId <= inputWorkers; workerId++ {
		log.WithFields(log.Fields{
			"workerId": workerId,
		}).Debug("Starting goroutine for inputWorker")
		inputWorkerGroup.Go(func() (err error) {
			err = inputWorker(workerId, lines)
			return err
		})
	}

	log.WithFields(log.Fields{"inputPath": inputPath}).Debug("opening input")
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
	lineCount := 0
	for lineScanner.Scan() {
		lines <- lineScanner.Text()
		lineCount++
		if lineCount > 10000 {
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

	log.Debug("waiting for inputWorkers to complete")
	if err := inputWorkerGroup.Wait(); err != nil {
		log.WithFields(log.Fields{"err": err}).Fatal("one or more inputWorkers failed")
	} else {
		log.Info("successfully processed all input lines")
	}

	log.Debug("leaving main()")
	return
}

