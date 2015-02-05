#include <iostream>
#include <string>
#include <cstdint>
#include <regex>

#include "Tree.hpp"
#include "TreeNode.hpp"

int main(int argc, char **argv) {
    Tree tree;
    tree.addNode("/a/b/c/d/e",123);
    tree.addNode("/a/b/c/d/f",345);
    tree.addNode("/a/b/c/g/h",534);
    //std::cout << (tree.getRoot())->getPath() << std::endl; 
    std::cout << tree.toJSON() << std::endl; 
    return 0;
}
