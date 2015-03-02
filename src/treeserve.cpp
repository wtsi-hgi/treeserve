// Copyright (C)  2015, Wellcome Trust Sanger Institute
#include <folly/io/async/EventBaseManager.h>
#include <proxygen/httpserver/HTTPServer.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
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
#include "TreeserveRouter.hpp"

//////////////////////////////////////////////////////////////////////
// define command-line options using the google                     //
// gflags library                                                   //
// https://gflags.googlecode.com/git-history/master/doc/gflags.html //
//////////////////////////////////////////////////////////////////////
DEFINE_string(lstat, "", "paths of lstat gzipped text files - output "
    "produced by mpistat or equivalent");
DEFINE_string(serial, "", "path of formerly serialized tree to de-serialize "
    "from");
DEFINE_string(dump, "", "path of dump file - tree is serialized to this file "
    "after construction");
DEFINE_int32(port, -1, "Port to listen on with HTTP protocol");
DEFINE_string(ip, "localhost", "IP/Hostname to bind to");
DEFINE_int32(http_threads, 4, "Number of threads to listen on. Numbers <= 0 will use"
    " the number of cores on this machine.");

int main(int argc, char **argv) {
    TreeBuilder *tb = new TreeBuilder();

    // Initialize Google's logging library.
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, true);

    // make sure we have an lstat or a serial
    // this is an exclusive-or
    if (!((FLAGS_lstat == "") != (FLAGS_serial == ""))) {
        std::cerr << "you must specify an lstat file(s) OR a serial file"
            << std::endl;
        return 1;
    }

    // check option consistency for initializing from an lstat file
    if (FLAGS_lstat != "") {
        if (FLAGS_serial != "") {
            std::cerr << "you must either specify an lstat file(s) or a serial"
                " file, not both" << std::endl;
            return 1;
        } else {
            if (FLAGS_dump == "") {
                std::cerr << "you need to specify a dump file if using"
                    " lstat files" << std::endl;
                return 1;
            }
            // if here, create a tree from the lstat files
            // and then dump it to a file when built
            LOG(INFO) << "building tree from lstat files : " << FLAGS_lstat
                << " and dumping to " << FLAGS_dump << std::endl;
            std::vector<std::string> lstat_files;
            boost::split(lstat_files, FLAGS_lstat, boost::is_any_of("\t, "));
            global_tree = tb->from_lstat(lstat_files, FLAGS_dump);
        }
    }

    // check option consistency if starting from a previously serialized tree
    if (FLAGS_serial != "") {
        if (FLAGS_dump != "") {
            std::cerr << "do not specify a dump file if using a serial file"
                << std::endl;
            return 1;
        }
        // if here, build a tree from the supplied serial file
        LOG(INFO) << "building tree from serial file : " << FLAGS_serial
            << std::endl;
        global_tree = tb->from_serial(FLAGS_serial);
    }

    // start the http server if 'port' option is set
    if (FLAGS_port != -1) {
        google::InstallFailureSignalHandler();
        // start server listening on 'port'
        std::vector<proxygen::HTTPServer::IPConfig> IPs = {
            {folly::SocketAddress(FLAGS_ip, FLAGS_port, true), proxygen::HTTPServer::Protocol::HTTP}
        };

        if (FLAGS_http_threads <= 0) {
            FLAGS_http_threads = 4;
            CHECK_GT(FLAGS_http_threads, 0);
        }

        proxygen::HTTPServerOptions options;
        options.threads = static_cast<size_t>(FLAGS_http_threads);
        options.idleTimeout = std::chrono::milliseconds(60000);
        options.shutdownOn = {SIGINT, SIGTERM};
        options.handlerFactories = proxygen::RequestHandlerChain()
            .addThen<TreeserveRouter>()
            .build();

        proxygen::HTTPServer server(std::move(options));
        server.bind(IPs);

        // Start HTTPServer mainloop in a separate thread
        std::thread t([&] () {
            server.start();
        });

        t.join();
    } 
    google::ShutdownGoogleLogging();
    delete tb;  // TreeBuilder responsible for deleting tree as well
    return 0;
}
