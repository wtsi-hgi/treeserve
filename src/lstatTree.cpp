// standard library headers
#include <iostream>
#include <string>
#include <cstdlib>
#include <fstream>
#include <cstring>
#include <cerrno>

// linux syscalls
#include <unistd.h>

// boost headers
#include <boost/algorithm/string.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>

// 3rd party local headers
#include "fossa.h"

// my local headers
#include "Tree.hpp"
#include "base64.h"

#define PORT 6666

Tree *tree;

int main(int argc, char **argv) {

    // get the filename argument
    if (argc != 2) {
        std::cerr << "Usage : lstatTree <data.gz>" << std::endl;
        return 1;
    }
    
    // set up the gzip streaming
    // bzip2 compresses things a bit smaller but is much slower
    std::ifstream file(argv[1], std::ios_base::in | std::ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::input> gz;
    gz.push(boost::iostreams::gzip_decompressor());
    gz.push(file);
    std::istream in(&gz);

    // process lines to build the tree    
    tree=new Tree();
    for (std::string line; std::getline(in, line);) {

        // tokenize the line
        std::vector<std::string> tokens;
        boost::split(tokens, line, boost::is_any_of("\t"));

        // get the path
        std::string path=base64_decode(tokens[1]).substr(1);

        // get the size
        double size=atof(tokens[2].c_str())/(1024.0*1024.0*1024.0);

        // get the file type
        std::string file_type=tokens[8];

        if (file_type == "d") {
            tree->addNode(path,size);
        } else if (file_type == "f") {
            // find last / in the path
            size_t pos=path.find_last_of("/");
            path=path.substr(0,pos);
            tree->addNode(path,size);
        } 
    }

    // print out json for the tree...
    std::cout << tree->toJSON("lustre/scratch113/admin/hb5");
    std::cout << tree->toJSON("lustre/scratch113/admin",2);
    std::cout << tree->toJSON(4);

    // print out the json for a tree rooted at a particular path

    // clean up
    delete tree;

    return 0;
}
