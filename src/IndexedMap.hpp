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
        
        // destructor
        // assume that the map is responsible for cleaning the datum pointers stored in it
        // so need to iterate through and delete them all
        ~IndexedMap() {
            for (auto it : datumMap) {
                delete it.second;
            }
            datumMap.clear();
        }

        void addItem(std::string key, uint64_t ival) {
            Datum *d=new Datum(ival);
            addItem(key,d);
        }

        void addItem(std::string key, double fval) {
            Datum *d=new Datum(fval);
            addItem(key,d);
        }

        void combine(IndexedMap& other) {
            for (auto it : other.datumMap) {
                uint64_t index=it.first;
                Datum *other_datum=it.second;
                
                // does the index exist in this map
                std::unordered_map<uint64_t, Datum*>::const_iterator got = datumMap.find(index);
                if (got==datumMap.end()) {                
                    // no, so create a new entry
                    datumMap.insert(std::make_pair(index,new Datum(*other_datum)));
                } else {
                    // yes, so add datum to the current value
                    (*got).second->add(*other_datum);    
                }
            }
        }

        std::string toJSON() {
            std::ostringstream oss;
            std::string comma="";
            for (auto it : datumMap) {
                oss << comma << "\"" << valueLookup[it.first]<< "\" : " << it.second->toString();
                comma=", ";
            }
            return oss.str();
        }
        
        std::string toJSON(std::string item) {
            std::ostringstream oss;
            uint64_t index=keyLookup[item];
            Datum *d=datumMap[index];
            oss << "\"" << item << "\" : " << d->toString();
            return oss.str();
        }
        
        std::string getIndex() {
            std::ostringstream oss;
            for (auto it : keyLookup) {
                oss << it.first << " : " << it.second << std::endl;
            }
            return oss.str();
        }
        
    private :

        // this function is not thread safe as it modifies static members
        // typically would want to generate the tree in the main thread
        // once the map is built, accessing it is thread safe as then it is
        // read only.
        void addItem(std::string key, Datum *d) {
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
                datumMap.insert(std::make_pair(keyCounter,d));

                // increment the static key counter
                keyCounter++;

            } else {
                // key is already in the static map, get it's index value
                uint64_t index=(*got).second;

                // is it part of this instance map ?                
                std::unordered_map<uint64_t, Datum*>::const_iterator got = datumMap.find(index);
                if (got==datumMap.end()) {
                    // if not add it with this inital datum
                    datumMap.insert(std::make_pair(index,d));
                } else {
                    // if so increment the datum with the value
                    (*got).second->add(*d);
                }
            }
        }

        void addItem(uint64_t index, Datum *d) {
            // does the index exist in the current map
            std::unordered_map<uint64_t, Datum*>::const_iterator got = datumMap.find(index);
            if (got == datumMap.end()) {
                // add the datum with the specified index
                datumMap.insert(std::make_pair(index,d));
            } else {
                // index already in the map so need to combine datums 
                (*got).second->add(*d);
            }
        }

        static std::unordered_map<std::string, uint64_t> keyLookup;
        static std::unordered_map<uint64_t, std::string> valueLookup;
        static uint64_t keyCounter;
        std::unordered_map<uint64_t, Datum*> datumMap;
};
#endif