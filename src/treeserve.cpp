#include <iostream>
#include <string>
#include <cstdint>

#include <boost/program_options.hpp>

namespace po = boost::program_options;

int main(int argc, char **argv) {

    TreeBuilder *tb=new TreeBuilder();
    Tree *tree=0;
        
    // command line variables
    uint32_t port;
    std::vector<std::string> lstat_files;
    std::string serial_file;
    std::string dump_file;

    // declare command line options.
    po::options_description desc("Usage");
    desc.add_options()
    ("help", "produce help message")
    ("port", po::value<uint32_t>(&port), "port to listen on")
    ("lstat", po::value< std::vector<std::string> >(&lstat_files), "paths of lstat gzipped text files - output produced by mpistat or equivalent")
    ("serial", po::value<std::string>(&serial_file), "path of formerly serialized tree to de-serialize from")
    ("dump", po::value<std::string>(&dump_file), "path of dump file - tree is serialized to this file after construction");

    // parse the options
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    // pint help and quit if passed
    if (vm.count("help") || argc < 2) {
        std::cerr << desc << std::endl;
        return 1;
    }

    // make sure we have an lstat or a serial
    if (!(!vm.count("lstat") != !vm.count("serial"))) { // this is an exclusive-or
        std::cerr << "you must specify an lstat file(s) OR a serial file" << std::endl;
        return 1;
    }
    
    // check option consistency for initializing from an lstat file
    if (vm.count("lstat")) {
        if (vm.count("serial")) {
            std::cerr << "you must either specify an lstat file(s) or a serial file, not both" << std::endl;
            return 1;
        } else {
            if (!vm.count("dump")) {
                std::cerr << "you need to specify a dump file if using an lstat file" << std::endl;
                return 1;
            }
            // if here, create a tree from the lstat file and then dump it to a file when built
            tree=tb->from_lstat(lstat_files, dump_file, 16*1024*1024);
        }
    }

    // check option consistency if initializing from a previously serialized tree
    if (vm.count("serial")) {
        if (vm.count("dump")) {
            std::cerr << "do not specify a dump file if using a serial file" << std::endl;
            return 1;
        }
        // if here, build a tree from the supplied serial file
        tree=tb->from_serial(serial_file);
    }

    // start the http server if 'port' option is set
    if (vm.count("port")) {
        // start server listening on 'port'
        std::cout << "will start a server listening on " << port << std::endl;
    } else {
        delete tb; // TreeBuilder responsible for deleting tree as well
    }

    return 0;
}
