// Copyright (C)  2015, Wellcome Trust Sanger Institute
#ifndef SRC_TREEBUILDER_HPP_
#define SRC_TREEBUILDER_HPP_

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
#include <boost/regex.hpp>

// standard library headers
#include <string>
#include <unordered_map>
#include <vector>
#include <regex>

#include "Tree.hpp"
#include "IndexedMap.hpp"

// build a tree either from an lstat data file or
// from a previously serialised tree

class TreeBuilder {
 public:
    TreeBuilder(const std::string date_string_v) : tree(0),uid_map(), gid_map() {
        tree=new Tree(date_string_v);
    }

    ~TreeBuilder() {
        delete tree;
    }

    Tree* from_lstat(const std::vector<std::string>& lstat_file,
                     const std::string& dump_file);

    Tree* from_serial(const std::string& serial_file);

 private:
    // private copy constructor and assignment operator
    // to stop inadverdent copies and to satisfy -Weffc++
    // see http://jrdodds.blogs.com/blog/2004/04/disallowing_cop.html
    TreeBuilder(const TreeBuilder&);
    TreeBuilder& operator=(const TreeBuilder&);

    // methods for group and user lookups
    std::string uid_lookup(uint64_t uid);
    std::string gid_lookup(uint64_t gid);

    template<typename T>
    inline void addAttribute(IndexedMap *im,
                const std::string& attr_name, T attr_val) {
       im->addItem(attr_name, attr_val);
    }

    template<typename T>
    void addAttribute(IndexedMap* im, const std::string& attr_name,
                T attr_val, const std::string& gid_str,
                const std::string& uid_str, const std::string& property) {
        std::ostringstream oss;
        oss << attr_name << "$" << gid_str << "$" << uid_str << "$" << property;
        addAttribute(im, oss.str(), attr_val);
    }

    template<typename T>
    void addAttributes(IndexedMap* im, const std::string& attr_name,
                T attr_val, const std::string& grp, const std::string& usr,
                const std::string& property) {
        addAttribute(im, attr_name, attr_val, "*", "*", property);
        addAttribute(im, attr_name, attr_val, grp, "*", property);
        addAttribute(im, attr_name, attr_val, "*", usr, property);
        addAttribute(im, attr_name, attr_val, grp, usr, property);
    }

    // The tree being built
    Tree* tree;

    // maps for cacheing uid and gid lookups
    std::unordered_map<uint64_t, std::string> uid_map;
    std::unordered_map<uint64_t, std::string> gid_map;

    // regexes for path properties
    static std::unordered_map<std::string, boost::regex> path_property_regexes;
};

#endif  // SRC_TREEBUILDER_HPP_
