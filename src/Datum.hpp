#ifndef __DATUM_HPP__
#define __DATUM_HPP__

#include <string>

#include <boost/lexical_cast.hpp>

// nlohmann's json source library
#include "json.hpp"
using json = nlohmann::json;

// Datum class
// Holds a unit64_t (sizes in bytes) or a double (cost in pounds)  in a union
// yes this could be done with templates / polymorphism etc.
// but doing it this way saves dev time and complexity
// also avoids the overhead of virtual functions
// and saves a bit of ram due to the union
union {
    uint64_t i;
    double   f;
} typedef uif;

class Datum {
    public :
    
        Datum(uint64_t v) {
            u.i=v;
            is_double=false;
        }
        
        Datum(double v) {
            u.f=v;
            is_double=true;
        }

        // copy constructor
        Datum(const Datum &d) {
            is_double=d.is_double;
            if (is_double) {
                u.f=d.u.f;
            } else {
                u.i=d.u.i;
            }
        }
    
        void add(uint64_t v) {
            u.i += v;
        }
        
        void add(double v) {
            u.f += v;
        }
        
        void add(const Datum &d) {
            if (d.is_double) {
                u.f += d.u.f;
            } else {
                u.i += d.u.i;
            }
        }

        void sub(uint64_t v) {
            u.i -= v;
        }
        
        void sub(double v) {
            u.f -= v;
        }
        
        void sub(const Datum &d) {
            if (d.is_double) {
                u.f -= d.u.f;
            } else {
                u.i -= d.u.i;
            }
        }

        bool isZero() {
            if (is_double) {
                // assumes all negative numbers should be zero
                // threshold on cost would ignore a single file of 1 byte that is less than ~6.5h old
                // or a 23148 byte file that is 1s old
                return (u.f < 1e-13 ? true : false);
            } else {
                return (u.i == 0 ? true : false);
            }
        }
        
        std::string toString() {
            if (is_double) {
                return boost::lexical_cast<std::string>(u.f);
            } else {
                return boost::lexical_cast<std::string>(u.i);
            }
        }

    json toJSON() {
        json j;
        if (is_double) {
            j = u.f;
        } else {
            j = u.i;
        }
        return j;
    }

    private :
        uif u;
        bool is_double;
};
#endif


