#include <string>
#include <iostream>
#include <vector>

#include <boost/algorithm/string.hpp>

int main(int argc, char **argv) {

    std::string path="a/b/c/d";
    std::vector<std::string> names;
    boost::split(names, path, boost::is_any_of("/"));
    std::vector<std::string>::iterator it;
    for (it=names.begin(); it<names.end(); it++) {
        std::cout << *it << std::endl;
    }
    return 0;
}
