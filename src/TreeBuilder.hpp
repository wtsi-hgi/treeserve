#ifndef __TREE_BUILDER_HPP__
#define __TREE_BUILDER_HPP__

// standard library headers
#include <iostream>
#include <string>
#include <cstdlib>
#include <fstream>
#include <cstring>
#include <cerrno>
#include <sstream>
#include <unordered_map>

// linux syscalls
#include <unistd.h>
#include <pwd.h>
#include <grp.h>
#include <time.h>

// boost headers
#include <boost/algorithm/string.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/lexical_cast.hpp>

#include "Tree.hpp"
#include "IndexedMap.hpp"

// build a tree either from an lstat data file or
// from a previously serialised tree

class TreeBuilder {

    public:

        TreeBuilder() {
            tree=new Tree();
        }

        ~TreeBuilder() {
            delete tree;
        }


        Tree *from_lstat(std::string &lstat_file, std::string &dump_file);
        Tree *from_serial(std::string &serial_file);

    private:

        // methods for group and user lookups
        std::string uid_lookup(uint64_t uid);
        std::string gid_lookup(uint64_t gid);

        template<typename T>
        void addAttribute(IndexedMap &im, std::string attr_name, T attr_val, std::string gid_str, std::string uid_str, std::string property) {
            std::ostringstream oss;
            oss << attr_name << "$" << gid_str << "$" << uid_str << "$" << property;
            addAttribute(im, oss.str(),attr_val);
        }

        template<typename T>
        void addAttributes(IndexedMap &im, std::string attr_name, T attr_val, std::string grp, std::string usr, std::string property) {
            addAttribute(im, attr_name, attr_val, "*", "*", property);
            addAttribute(im, attr_name, attr_val, grp, "*", property);
            addAttribute(im, attr_name, attr_val, "*", usr, property);
            addAttribute(im, attr_name, attr_val, grp, usr, property);
        }

        // The tree being built
        Tree *tree;

        // maps for cacheing uid and gid lookups
        std::unordered_map<uint64_t, std::string> uid_map;
        std::unordered_map<uint64_t, std::string> gid_map;
};

#endif
