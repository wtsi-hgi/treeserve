#include <iostream>
#include <vector>
#include <fstream>

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/serialization.hpp>

#include "Datum.hpp"

int main() {
    uint64_t val1=123;
    double val2=3.14;
    uint64_t val3=345345;
    double val4=2.348543;
    std::vector<Datum> datums;
    datums.push_back(Datum(val1));
    datums.push_back(Datum(val2));
    datums.push_back(Datum(val3));
    datums.push_back(Datum(val4));
    
    for (auto it : datums) {
        std::cout << it.toString() << std::endl;
    }

    // test the sub method
    datums[1].sub(datums[3]);
    std::cout << datums[1].toString() << std::endl;
   
    // remove a datum from itself, should be zero
    datums[1].sub(datums[1]);
    if (datums[1].isZero()) {
        std::cout << "datums[1] is zero" << std::endl;
    } else {
        std::cout << "datums[1] is zero" << std::endl;
    }

    // test serialization
    {
        std::ofstream ofs("datums.ar");
        boost::archive::binary_oarchive oa(ofs);
        oa << datums;
    } 
    std::vector<Datum> datums1;
    {
        std::ifstream ifs("datums.ar");
        boost::archive::binary_iarchive ia(ifs);
        ia >> datums1;
    }

    std::cout << "before..." << std::endl;
    for (auto it : datums) {
        std::cout << it.toString() << std::endl;
    }
    std::cout << "after..." << std::endl;
    for (auto it : datums) {
        std::cout << it.toString() << std::endl;
    }
    return 0;
}


