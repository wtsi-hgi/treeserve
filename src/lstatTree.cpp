#include <iostream>
#include <string>
#include <cstdlib>
#include <fstream>

#include <boost/algorithm/string.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>

#include "Tree.hpp"
#include "base64.h"

int main(int argc, char **argv) {

    // get the filename argument
    if (argc != 2) {
        std::cerr << "Usage : lstatTree <data.gz>" << std::endl;
        return 1;
    }
    
    // set up the gzip streaming
    // bzip2 compresses things a bit smaller but is much slowere
    std::ifstream file(argv[1], std::ios_base::in | std::ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::input> gz;
    gz.push(boost::iostreams::gzip_decompressor());
    gz.push(file);
    std::istream in(&gz);

    // process lines to build the tree    
    Tree tree;
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
            tree.addNode(path,size);
        } else if (file_type == "f") {
            // find last / in the path
            size_t pos=path.find_last_of("/");
            path=path.substr(0,pos);
            tree.addNode(path,size);
        } 
    }
    std::cout << tree.toJSON() << std::endl; 
    return 0;
}
