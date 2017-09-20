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
// example start command

package main

import (
	"flag"
	"fmt"
	"runtime"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/profile"
	"github.com/wtsi-hgi/treeserve/go"
)

// Variables set by command-line flags
var inputPath string
var lmdbPath string
var groupFile string
var userFile string
var inputWorkers int
var costReferenceTime int64
var lmdbMapSize int64
var nodesCreatedInfoEveryN int64
var stopInputAfterNLines int64
var nodesFinalizedInfoEveryN int64
var stopFinalizeAfterNNodes int64
var finalizeWorkers int
var maxProcs int
var debug bool
var cpuProfilePath string
var memProfilePath string
var blockProfilePath string
var port int

func init() {
	flag.StringVar(&inputPath, "inputPath", "/tmp/input.dat.gz", "Input file")
	flag.StringVar(&groupFile, "groupFile", "/etc/groups", "Group mapping file")
	flag.StringVar(&userFile, "userFile", "/etc/users", "User mapping file file")
	flag.StringVar(&lmdbPath, "lmdbPath", "/tmp/treeserve_lmdb", "Path to LMDB environment")
	flag.Int64Var(&lmdbMapSize, "lmdbMapSize", 200*1024*1024*1024, "LMDB map size (maximum)")
	flag.IntVar(&inputWorkers, "inputWorkers", 2, "Number of parallel workers to use for processing lines of input data to build the tree")
	flag.Int64Var(&costReferenceTime, "costReferenceTime", time.Now().Unix(), "The time to use for cost calculations in seconds since the epoch")
	flag.Int64Var(&nodesCreatedInfoEveryN, "nodesCreatedInfoEveryN", 10000, "Number of node creations between info logs")
	flag.Int64Var(&stopInputAfterNLines, "stopInputAfterNLines", -1, "Stop processing input after this number of lines (-1 to process all input)")
	flag.Int64Var(&stopFinalizeAfterNNodes, "stopFinalizeAfterNNodes", -1, "Stop finalizing after this number of nodes (-1 to finalize all nodes)")
	flag.Int64Var(&nodesFinalizedInfoEveryN, "nodesFinalizedInfoEveryN", 10000, "Number of node creations between info logs")
	flag.IntVar(&finalizeWorkers, "finalizeWorkers", 10, "Number of parallel workers to use for finalizing the tree")
	flag.IntVar(&maxProcs, "maxProcs", runtime.GOMAXPROCS(0), "Maximum number of CPUs to use simultaneously (default: $GOMAXPROCS)")
	flag.BoolVar(&debug, "debug", false, "Enable debug logging")
	flag.StringVar(&cpuProfilePath, "cpuProfilePath", "", "Write CPU profile to path")
	flag.StringVar(&memProfilePath, "memProfilePath", "", "Write Memory profile to path")
	flag.StringVar(&blockProfilePath, "blockProfilePath", "", "Write Block (contention) profile to path")
	flag.IntVar(&port, "port", 8000, "webserver port")
}

func main() {
	flag.Parse()
	//log.SetFormatter(&log.JSONFormatter{})
	// profiling
	//f1, err := os.Create("/tmp/cpu.dat")
	//if err != nil {
	//	log.Fatal(err)
	//}

	//pprof.StartCPUProfile(f1)
	//defer pprof.StopCPUProfile()

	//
	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	starttime := time.Now()
	formerMaxProcs := runtime.GOMAXPROCS(maxProcs)
	log.WithFields(log.Fields{
		"formerMaxProcs": formerMaxProcs,
		"maxProcs":       maxProcs,
	}).Info("set GOMAXPROCS")
	if cpuProfilePath != "" {
		defer profile.Start(profile.CPUProfile, profile.ProfilePath(cpuProfilePath)).Stop()
	}
	if memProfilePath != "" {
		defer profile.Start(profile.MemProfileRate(4096), profile.ProfilePath(memProfilePath)).Stop()
	}
	if blockProfilePath != "" {
		runtime.SetBlockProfileRate(1)
		defer profile.Start(profile.BlockProfile, profile.ProfilePath(blockProfilePath)).Stop()
	}
	flagFields := log.Fields{}
	flag.VisitAll(func(f *flag.Flag) {
		flagFields[f.Name] = f.Value
	})

	log.Debug("entered main()")

	ts := treeserve.NewTreeServe(lmdbPath, lmdbMapSize, costReferenceTime, nodesCreatedInfoEveryN, stopInputAfterNLines, nodesFinalizedInfoEveryN, stopFinalizeAfterNNodes, debug)
	err := ts.OpenLMDB()
	if err != nil {
		log.Fatal("failed to open TreeServe LMDB")
	}
	defer ts.CloseLMDB()

	//MainStateMachine:
	for {
		state, err := ts.GetState()

		if err != nil {
			log.Fatal(fmt.Sprintf("failed to get state, %s", err.Error()))
		}

		nextState := "failed"
		switch state {
		case "":
			log.Debug("main state machine: initial state")
			nextState = "inputProcessing"
		case "inputProcessing":
			log.Info("main state machine: inputProcessing")
			err = ts.ProcessInput(inputPath, inputWorkers)
			if err != nil {
				log.Fatal(fmt.Sprintf("failed to process input, %s", err.Error()))
			} else {
				nextState = "inputProcessed"
			}
		case "inputProcessed":
			log.Info("main state machine: inputProcessed")
			nextState = "finalize"
		case "finalize":
			log.Info("main state machine: finalize")
			err = ts.Finalize("/", finalizeWorkers)
			if err != nil {
				log.Error(fmt.Sprintf("failed to finalize input, %s", err.Error()))
				nextState = "failed"
			} else {
				nextState = "treeReady"
			}

		case "treeReady":
			log.Info("main state machine: tree ready after " + time.Since(starttime).String())
			//pprof.StopCPUProfile()
			ts.Webserver(groupFile, userFile, strconv.Itoa(port))
		case "failed":

			log.Fatal(fmt.Sprintf("Treeserve data processing failed, %s", err.Error()))
		default:
			log.Fatal("main state machine: unimplemented state transition")
		}
		err = ts.SetState(nextState)
		if err != nil {
			log.Fatal(fmt.Sprintf("failed to set state, %s, %s", nextState, err.Error()))
		}
	}

}
