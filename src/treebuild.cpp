// Copyright (C)  2015, Wellcome Trust Sanger Institute
#include <unistd.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iostream>
#include <string>
#include <cstdint>
#include <vector>

#include "globals.hpp"

#include "TreeBuilder.hpp"
#include "Tree.hpp"
#include "MemLogger.hpp"

//////////////////////////////////////////////////////////////////////
// define command-line options using the google                     //
// gflags library                                                   //
// https://gflags.googlecode.com/git-history/master/doc/gflags.html //
//////////////////////////////////////////////////////////////////////
DEFINE_string(lstat, "", "paths of lstat gzipped text files - output "
    "produced by mpistat or equivalent");
DEFINE_string(out, "", "path to store serialized tree to");

int main(int argc, char **argv) {
    TreeBuilder *tb = new TreeBuilder();

    // Initialize Google's logging library.
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, true);

    // make sure we have at least one lstat parameter specified
    if (FLAGS_lstat == "") {
        std::cerr << "you must specify an lstat file(s)"
            << std::endl;
        return 1;
    }

    // make sure we have an out parameter specified
    if (FLAGS_out == "") {
        std::cerr << "you must specify an output file"
            << std::endl;
        return 1;
    }

    // create a tree from the lstat files
    LOG(INFO) << "building tree from lstat files : " << FLAGS_lstat
                << " and dumping to " << FLAGS_dump << std::endl;
    std::vector<std::string> lstat_files;
    boost::split(lstat_files, FLAGS_lstat, boost::is_any_of("\t, "));
    global_tree = tb->from_lstat(lstat_files, FLAGS_dump);

    // serialize the tree to the output file

    // tidy up and exit
    google::ShutdownGoogleLogging();
    delete tb;  // TreeBuilder responsible for deleting tree as well
    return 0;
}
