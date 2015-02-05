#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>

// indexed map
// since there will be a lot of repeated strings in maps throughout the
// tree structure, there will be a single copy of each string in a static lookup table
// so that IndexedMap instances are of type <uint64_t, double> rather than <string double>
// with the key being the index in the lookup table of the actual key of the datum
// since there are static members, there will need to be a cpp file - this class can't be
// header only

// we will want the values of the key-value pairs
// to be a mix of uint64s and doubles so use a template base class and
// fully sepcced derived classes
template <class T> class Datum {
    public :
        Datum(T v) : val(v) {}
        void incr(T&  v) {val += v;}
        void decr(T& v) {val -= v;}
        virtual std::string toString()=0;
    private:
        T val;
}

class IndexedMap {

    public :
        IndexedMap() : 
        void addItem(std::string key, Datum d) {
            // try to get the index associated with the key from the
            // static map
            if () {
                // key not in static map so add it
            
                // add entry to the instance map with the key being the
                // index into the static map            

            } else {
                // key is already in the static map
                // is it part of this instance map ?
                // if so increment the datum with the value
                // if not add it with this inital datum
            }
        }

        void combine(IndexedMap& other) {
            // loop over keys in 'other'
            // and call addItem on each one
        }
        
        Datum getItem(std::string) {
            // get the index of the key from the
            // static map
            // loop up the item in the instance map
            // with the index
        }
        std::string toJSON() {
            // return "name1" : value1, "name2" : value2 etc.
        }
        
        std::string toJSON(std::string item) {
            // return "item" : item_value
        }
        
    private :
        static std::unorderd_map<std::string, uint64_t> keyLookup;
        static uint64_t keyCounter;
        std::unordered_map<uint64_t,Datum> map;
};