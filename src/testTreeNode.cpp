#include <iostream>
#include <string>
#include "TreeNode.hpp"

int main(int argc, char **argv) {

    TreeNode *tree=new TreeNode("a",0);
    tree->incrSize(50);
    TreeNode *tmp=new TreeNode("b",tree);
    tmp->incrSize(2);
    std::cout << tree->toJson() << std::endl; 
    return 0;
}
