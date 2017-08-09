// Copyright (C)  2015, Wellcome Trust Sanger Institute
#include <iostream>

#include <cstdint>

#include "Tree.hpp"
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
    im1.addItem("size$hgi$user$other", ival_1);
    im1.addItem("cost$hgi$user$other", fval_1);
    im2.addItem("timestamp$hgi$user$other", ival_2);
    im2.addItem("cost$hgi$user$other", fval_2);

    Tree tree;
    tree.addNode("/a/b/c/d/e", im1);
    tree.addNode("/a/b/c/d/f", im2);
    std::cout << tree.toJSON() << std::endl;
    return 0;
}
