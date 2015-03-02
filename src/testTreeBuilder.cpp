#include <gflags/gflags.h>
#include <glog/logging.h>

#include <string>

#include "TreeBuilder.hpp"
#include "Tree.hpp"

int main(int argc, char **argv) {
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, true);
    TreeBuilder *tb = new TreeBuilder();
    std::vector<std::string> lstat_files;
    lstat_files.push_back(argv[1]);
    Tree *tree = tb->from_lstat(lstat_files, std::string(argv[2]));
    delete tb;
    google::ShutdownGoogleLogging();
    return 0;
}

