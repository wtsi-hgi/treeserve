// Copyright (C)  2015, Wellcome Trust Sanger Institute

#include <unistd.h>

#include <folly/Memory.h>
#include <folly/Portability.h>
#include <folly/io/async/EventBaseManager.h>
#include <proxygen/httpserver/HTTPServer.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>

#include <iostream>
#include <string>
#include <cstdint>

// define command-line options using the google
// gflags library
DEFINE_string("lstat", "", "paths of lstat gzipped text files - output "
    "produced by mpistat or equivalent");
DEFINE_string("serial", "", "path of formerly serialized tree to de-serialize "
    "from");
DEFINE_string(dump, "", "path of dump file - tree is serialized to this file "
    "after construction");
DEFINE_int32(port, 11000, "Port to listen on with HTTP protocol");
DEFINE_string(ip, "localhost", "IP/Hostname to bind to");
DEFINE_int32(threads, 0, "Number of threads to listen on. Numbers <= 0 will use"
    " the number of cores on this machine.");

int main(int argc, char **argv) {
    TreeBuilder *tb = new TreeBuilder();
    Tree *tree = 0;
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    // pint help and quit if passed
    if (vm.count("help") || argc < 2) {
        std::cerr << desc << std::endl;
        return 1;
    }

    // make sure we have an lstat or a serial
    // this is an exclusive-or
    if (!(!vm.count("lstat") != !vm.count("serial"))) {
        std::cerr << "you must specify an lstat file(s) OR a serial file"
            << std::endl;
        return 1;
    }

    // check option consistency for initializing from an lstat file
    if (vm.count("lstat")) {
        if (vm.count("serial")) {
            std::cerr << "you must either specify an lstat file(s) or a serial"
                " file, not both" << std::endl;
            return 1;
        } else {
            if (!vm.count("dump")) {
                std::cerr << "you need to specify a dump file if using an"
                    " lstat file" << std::endl;
                return 1;
            }
            // if here, create a tree from the lstat file
            // and then dump it to a file when built
            tree = tb->from_lstat(lstat_files, dump_file, 16*1024*1024);
        }
    }

    // check option consistency if starting from a previously serialized tree
    if (vm.count("serial")) {
        if (vm.count("dump")) {
            std::cerr << "do not specify a dump file if using a serial file"
                << std::endl;
            return 1;
        }
        // if here, build a tree from the supplied serial file
        tree = tb->from_serial(serial_file);
    }

    // start the http server if 'port' option is set
    if (vm.count("port")) {
        // start server listening on 'port'
        std::cout << "will start a server listening on " << port << std::endl;
    } else {
        delete tb;  // TreeBuilder responsible for deleting tree as well
    }

    return 0;
}
