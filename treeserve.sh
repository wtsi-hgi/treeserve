#!/bin/bash

export LD_LIBRARY_PATH=/software/hgi/pkglocal/gcc-4.9.1/lib64:/software/hgi/pkglocal/gcc-4.9.1/lib:/software/boost-1.57/lib:$LD_LIBRARY_PATH

$HOME/git/treeserve/bin/lstatTree  /lustre/scratch114/teams/hgi/lustre_reports/mpistat/data/20150122_113.dat.gz
