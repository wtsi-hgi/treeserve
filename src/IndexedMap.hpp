#ifndef __INDEXED_MAP_HPP__
#define __INDEXED_MAP_HPP__

#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <sstream>
#include <string>
#include <utility>
#include <iostream>

#include "Datum.hpp"

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

class IndexedMap {

    public :
        // default constructor - just creates an empty map ready to fill
        IndexedMap() : datumMap() {}

        template <typename T>
        void addItem(std::string key, T val) {
            // try to get the index associated with the key from the
            // static map
            std::unordered_map<std::string, uint64_t>::const_iterator got = keyLookup.find(key);
            if (got == keyLookup.end()) {
                // key not in static map so add it
                keyLookup.insert(std::make_pair(key,keyCounter));

                // add to the valueLookup too
                valueLookup.insert(std::make_pair(keyCounter,key));
                
                // add entry to the instance map with the key being the
                // index into the static map
                datumMap.insert(std::make_pair(keyCounter,Datum(val)));

                // increment the static key counter
                keyCounter++;

            } else {
                // key is already in the static map, get it's index value
                uint64_t index=(*got).second;

                // is it part of this instance map ?                
                std::unordered_map<uint64_t, Datum>::iterator got = datumMap.find(index);
                if (got==datumMap.end()) {
                    // if not add it with this inital datum
                    datumMap.insert(std::make_pair(index,Datum(val)));
                } else {
                    // if so increment the datum with the value
                    (*got).second.add(val);
                }
            }
        }

        template <typename T>
        void addItem(uint64_t index, T val) {
            // does the index exist in the current map
            std::unordered_map<uint64_t, Datum>::const_iterator got = datumMap.find(index);
            if (got == datumMap.end()) {
                // add the datum with the specified index
                datumMap.insert(std::make_pair(index,Datum(val)));
            } else {
                // index already in the map so need to combine datums 
                (*got).second.add(val);
            }
        }

        void combine(IndexedMap& other) {
            for (auto it : other.datumMap) {
                uint64_t index=it.first;

                // does the index exist in this map
                std::unordered_map<uint64_t, Datum>::iterator got = datumMap.find(index);
                if (got==datumMap.end()) {                
                    // no, so create a new entry
                    datumMap.insert(std::make_pair(index,Datum(it.second)));
                } else {
                    // yes, so add datum to the current value
                    (*got).second.add(it.second);    
                }
            }
        }

        void subtract(IndexedMap& other) {
            // loop over datums in this map
            for (auto it : datumMap) {

                // get the index and datum
                uint64_t index=it.first;

                // does the index exist in the other map?
                std::unordered_map<uint64_t, Datum>::const_iterator got = other.datumMap.find(index);
                if (got==datumMap.end()) {
                    // no - nothing to do
                    continue;
                } else {
                    // yes, so subtract other datums value from the current value
                    it.second.sub((*got).second);
                    if (it.second.isZero()) {
                        // if the new value is zero then can remove the datum completely
                        datumMap.erase(index);
                    }
                }
            }
        }

        std::string toJSON() {
            std::ostringstream oss;
            std::string comma="";
            for (auto it : datumMap) {
                oss << comma << "\"" << valueLookup[it.first]<< "\" : " << it.second.toString();
                comma=", ";
            }
            return oss.str();
        }
        
        std::string toJSON(std::string item) {
            std::ostringstream oss;
            uint64_t index=keyLookup[item];
            oss << "\"" << item << "\" : " << datumMap.at(index).toString();
            return oss.str();
        }
        
        std::string getIndex() {
            std::ostringstream oss;
            for (auto it : keyLookup) {
                oss << it.first << " : " << it.second << std::endl;
            }
            return oss.str();
        }
        
        std::string keysJSON() {
            std::ostringstream oss;
            oss << "{ \"attributes\": [";
            std::string comma="";
            for (auto it : keyLookup) {
                oss << comma << "\"" << it.first << "\"";
                comma=",";
            }
            oss << "]}";
            return oss.str();
        }        

        bool empty() {
            return datumMap.empty();
        }

    private :

        static std::unordered_map<std::string, uint64_t> keyLookup;
        static std::unordered_map<uint64_t, std::string> valueLookup;
        static uint64_t keyCounter;
        std::unordered_map<uint64_t, Datum> datumMap;
};
#endif