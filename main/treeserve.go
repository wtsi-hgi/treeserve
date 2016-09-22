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
	"time"
	"runtime/pprof"
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
var debug bool
var cpuProfilePath string

func init() {
	flag.StringVar(&inputPath, "inputPath", "input.dat.gz", "Input file")
	flag.StringVar(&lmdbPath, "lmdbPath", "/tmp/treeserve_lmdb", "Path to LMDB environment")
	flag.Int64Var(&lmdbMapSize, "lmdbMapSize", 200 * 1024 * 1024 * 1024, "LMDB map size (maximum)")
	flag.IntVar(&inputWorkers, "inputWorkers", 4, "Number of workers to use for processing lines of input data")
	flag.Int64Var(&costReferenceTime, "costReferenceTime", time.Now().Unix(), "The time to use for cost calculations in seconds since the epoch")
	flag.Int64Var(&nodesCreatedInfoEveryN, "nodesCreatedInfoEveryN", 10000, "Number of node creations between info logs")
	flag.Int64Var(&stopAfterNLines, "stopAfterNLines", -1, "Stop processing input after this number of lines (-1 to process all input)")
	flag.BoolVar(&debug, "debug", false, "Enable debug logging")
	flag.StringVar(&cpuProfilePath, "cpuProfilePath", "", "Write cpu profile to file")
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

	ts := treeserve.NewTreeServe(lmdbPath, lmdbMapSize, costReferenceTime, nodesCreatedInfoEveryN)
	err := ts.OpenLMDB()
	if err != nil {
		log.WithFields(log.Fields{
			"lmdbPath": lmdbPath,
			"lmdbMapSize": lmdbMapSize,
			"ts": ts,
		}).Fatal("failed to open TreeServe LMDB")
	}
	defer ts.CloseLMDB()

	log.WithFields(log.Fields{
		"inputWorkers": inputWorkers,
	}).Debug("starting InputWorker goroutines")
	var inputWorkerGroup errgroup.Group
	lines := make(chan string, inputWorkers * 10)
	for workerId := 1; workerId <= inputWorkers; workerId++ {
		log.WithFields(log.Fields{
			"workerId": workerId,
		}).Debug("Starting goroutine for InputWorker")
		inputWorkerGroup.Go(func() (err error) {
			err = ts.InputWorker(workerId, lines)
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
	var lineCount int64 = 0
	for lineScanner.Scan() {
		lines <- lineScanner.Text()
		lineCount++
		if stopAfterNLines >= 0 && lineCount > stopAfterNLines {
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
		log.Info("successfully processed all input lines")
	}

	log.Debug("leaving main()")
	return
}

