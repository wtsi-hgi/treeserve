#include <iostream>
#include <string>
#include <cstdint>
#include <regex>

#include "Tree.hpp"

int main(int argc, char **argv) {
    std::string path="a/b/c/d/e";
    uint64_t size=123;
    Tree tree;
    tree.addNode(path,size);
    std::cout << tree.toJSON() << std::endl; 
    return 0;
}
