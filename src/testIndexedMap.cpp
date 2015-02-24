#include <iostream>

#include "IndexedMap.hpp"
#include "Datum.hpp"

int main(int argc, char **argv) {

    // create 2 IndexedMap instances
    IndexedMap *im1=new IndexedMap();
    IndexedMap *im2=new IndexedMap();

    // create some values
    uint64_t ival_1=1234;
    double fval_1=2.2;

    uint64_t ival_2=3456;
    double fval_2=1.1;
    
    // add them to the maps
    im1->addItem("size",ival_1);
    im1->addItem("cost",fval_1);
    im2->addItem("timestamp",ival_2);
    im2->addItem("cost",fval_2);
    
    // print out the map instances
    std::cout << "im1..." << std::endl;
    std::cout << im1->toJSON() << std::endl;
    std::cout << std::endl;
    std::cout << "im2..." << std::endl;
    std::cout << im2->toJSON() << std::endl;
    std::cout << std::endl;

    // combine im2 with im1
    im1->combine(*im2);

    // print out modified im1
    std::cout << "modified im1..." << std::endl;
    std::cout << im1->toJSON() << std::endl;
    std::cout << std::endl;

    // subtracting im1 from itself should give an empty
    im1->subtract(*im1);
    std::cout << "im1 should be empty now..." << std::endl;
    std::cout << im1->toJSON() << std::endl;
 
    // print out the static indexing map
    std::cout << "indexing map : " << std::endl;
    std::cout << im1->getIndex() << std::endl;
        delete im1;
        delete im2;
    return 0;
}
