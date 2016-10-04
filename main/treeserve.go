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
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/wtsi-hgi/treeserve"
)

// Variables set by command-line flags
var inputPath string
var lmdbPath string
var inputWorkers int
var costReferenceTime int64
var lmdbMapSize int64
var nodesCreatedInfoEveryN int64
var stopAfterNLines int64
var finalizeWorkers int
var maxProcs int
var debug bool
var cpuProfilePath string

func init() {
	flag.StringVar(&inputPath, "inputPath", "input.dat.gz", "Input file")
	flag.StringVar(&lmdbPath, "lmdbPath", "/tmp/treeserve_lmdb", "Path to LMDB environment")
	flag.Int64Var(&lmdbMapSize, "lmdbMapSize", 200*1024*1024*1024, "LMDB map size (maximum)")
	flag.IntVar(&inputWorkers, "inputWorkers", 2, "Number of parallel workers to use for processing lines of input data to build the tree")
	flag.Int64Var(&costReferenceTime, "costReferenceTime", time.Now().Unix(), "The time to use for cost calculations in seconds since the epoch")
	flag.Int64Var(&nodesCreatedInfoEveryN, "nodesCreatedInfoEveryN", 10000, "Number of node creations between info logs")
	flag.Int64Var(&stopAfterNLines, "stopAfterNLines", -1, "Stop processing input after this number of lines (-1 to process all input)")
	flag.IntVar(&finalizeWorkers, "finalizeWorkers", 10, "Number of parallel workers to use for finalizing the tree")
	flag.IntVar(&maxProcs, "maxProcs", runtime.GOMAXPROCS(0), "Maximum number of CPUs to use simultaneously (default: $GOMAXPROCS)")
	flag.BoolVar(&debug, "debug", false, "Enable debug logging")
	flag.StringVar(&cpuProfilePath, "cpuProfilePath", "", "Write cpu profile to file")
}

func main() {
	flag.Parse()
	//log.SetFormatter(&log.JSONFormatter{})
	if debug {
		log.SetLevel(log.DebugLevel)
	}
	formerMaxProcs := runtime.GOMAXPROCS(maxProcs)
	log.WithFields(log.Fields{
		"formerMaxProcs": formerMaxProcs,
		"maxProcs":       maxProcs,
	}).Info("set GOMAXPROCS")
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
	if debug {
		log.WithFields(flag_fields).Debug("entered main()")
	}

	ts := treeserve.NewTreeServe(lmdbPath, lmdbMapSize, costReferenceTime, nodesCreatedInfoEveryN, stopAfterNLines, debug)
	err := ts.OpenLMDB()
	if err != nil {
		log.WithFields(log.Fields{
			"lmdbPath":    lmdbPath,
			"lmdbMapSize": lmdbMapSize,
			"ts":          ts,
		}).Fatal("failed to open TreeServe LMDB")
	}
	defer ts.CloseLMDB()

MainStateMachine:
	for {
		state, err := ts.GetState()
		if err != nil {
			log.WithFields(log.Fields{"err": err}).Fatal("failed to get state")
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
				log.WithFields(log.Fields{"err": err}).Fatal("failed to process input")
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
				nextState = "treeReady"
			}
			nextState = "finalize"
			break MainStateMachine // for development only
		case "treeReady":
			log.Info("main state machine: tree ready")
			break MainStateMachine
		case "failed":
			log.WithFields(log.Fields{
				"err": err,
			}).Fatal("main state machine: failed")
		default:
			log.WithFields(log.Fields{
				"state": state,
			}).Fatal("main state machine: unimplemented state transition")
		}
		err = ts.SetState(nextState)
		if err != nil {
			log.WithFields(log.Fields{
				"nextState": nextState,
				"err":       err,
			}).Fatal("failed to set state")
		}
	}

	log.Debug("leaving main()")

	return
}
