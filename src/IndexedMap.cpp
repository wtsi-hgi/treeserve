// Copyright (C)  2015, Wellcome Trust Sanger Institute
#include <unordered_map>
#include <string>
#include <cstdint>

#include "IndexedMap.hpp"

// definition of static members
std::unordered_map<std::string, uint64_t> *IndexedMap::keyLookup=new std::unordered_map<std::string, uint64_t>();
std::unordered_map<uint64_t, std::string> *IndexedMap::valueLookup=new std::unordered_map<uint64_t, std::string>();
uint64_t *IndexedMap::keyCounter=new uint64_t();
