// Copyright (C)  2015, Wellcome Trust Sanger Institute
#include <iostream>
#include <string>

#include "TreeNode.hpp"
#include "IndexedMap.hpp"

int main() {
    // create 2 IndexedMap instances
    IndexedMap im1;
    IndexedMap im2;

    // create some values
    uint64_t ival_1 = 1234;
    double fval_1 = 2.2;

    uint64_t ival_2 = 3456;
    double fval_2 = 1.1;

    // add them to the maps
    im1.addItem("sizesize$hgi$user$other", ival_1);
    im1.addItem("costsize$hgi$user$other", fval_1);
    im2.addItem("timestampsize$hgi$user$other", ival_2);
    im2.addItem("costsize$hgi$user$other", fval_2);

    // create a smal tree
    TreeNode *tree = new TreeNode("a", 0);
    tree->combine(im1);
    TreeNode *tmp = new TreeNode("b", tree);
    tmp->combine(im2);
    std::cout << tree->toJSON(2, 0) << std::endl;
    tree->finalize();
    delete tree;
    return 0;
}
