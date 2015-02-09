#include <iostream>
#include <vector>

#include "Datum.hpp"

int main(int argc, char **argv) {
    uint64_t val1=123;
    double val2=3.14;
    uint64_t val3=345345;
    double val4=2.348543;
    std::vector<Datum> datums;
    datums.push_back(Datum(val1));
    datums.push_back(Datum(val2));
    datums.push_back(Datum(val3));
    datums.push_back(Datum(val4));
    
    std::vector<Datum>::iterator it;
    for (it=datums.begin(); it < datums.end(); it++) {
        std::cout << (*it).toString() << std::endl;
    }
    return 0;
}

