#ifndef __DATUM_HPP__
#define __DATUM_HPP__

#include <string>
#include <fstream>

#include <boost/lexical_cast.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/vector.hpp>
//#include <boost/serialization/base_object.hpp>

// Datum class
// Holds a unit64_t (sizes in bytes) or a double (cost in pounds)  in a union
// yes this could be done with templates / polymorphism etc.
// but doing it this way saves dev time and complexity
// also avoids the overhead of virtual functions
// and saves a bit of ram due to the union
union uif {

    uint64_t i;
    double   f;

    uif() : i(0UL) {}
    uif(uint64_t v) : i(v) {}
    uif(double v) : f(v) {}
};

class Datum {
    public :
        friend class boost::serialization::access;

        Datum() : is_double(false), u(0UL) {}

        Datum(uint64_t v) : is_double(false), u(v) {}
        
        Datum(double v) : is_double(true), u(v) {}

        // copy constructor
        Datum(const Datum &d) : is_double(d.is_double), u(d.u) {}
    
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
                return (u.f == 0 ? true : false);
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

        template<class Archive>
        void serialize(Archive & ar, const unsigned int version) {
            if (version==0) {
                ar & is_double;
                if (is_double) {
                    ar & u.f;
                } else {
                    ar & u.i;
                }
            }
        }

    private :
        bool is_double;
        uif u;
};
BOOST_CLASS_VERSION(Datum, 0)
#endif


