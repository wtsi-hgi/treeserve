// Copyright (C)  2015, Wellcome Trust Sanger Institute
#include <unordered_map>
#include <string>
#include <cstdint>

#include "IndexedMap.hpp"

// definition of static members
std::unordered_map<std::string, uint64_t> IndexedMap::keyLookup;
std::unordered_map<uint64_t, std::string> IndexedMap::valueLookup;
uint64_t IndexedMap::keyCounter;
