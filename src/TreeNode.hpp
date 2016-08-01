// Copyright (C)  2015, Wellcome Trust Sanger Institute
#ifndef SRC_TREE_NODE_HPP_
#define SRC_TREE_NODE_HPP_

#include <boost/algorithm/string/join.hpp>

#include <string>
#include <vector>
#include <unordered_map>
#include <limits>
#include <sstream>
#include <utility>
#include <stack>
#include <atomic>

#include "IndexedMap.hpp"
#include "Datum.hpp"

class TreeNode {
 public :
    explicit TreeNode(std::string n, TreeNode *p = 0) : name(n), parent(p),
                data(), children(), depth(0) {
        ++node_count;
        if (parent != 0) {
            depth = (parent->depth)+1;
            parent->addChild(this);
        }
    }

    ~TreeNode() {
        for (auto iter : children) {
            delete iter.second;
        }
    }

    std::string getName() {
        return name;
    }

    void combine(const IndexedMap &other_map) {
        data.combine(other_map);
    }

    void addChild(TreeNode* c) {
        children.insert(std::make_pair(c->getName(), c));
    }

    TreeNode* getChild(std::string n) {
        auto got = children.find(n);
        if (got == children.end()) {
            return 0;
        } else {
            return got->second;
        }
    }

    std::string getPath() {
        // this recurses up the parent links to construct the full path to a
        // node
        // don't want to store the full path in the node as that will
        // increase the memory requirements
        // the only time we really need this is when we output the json
        // if we are only outputting 2 or 3 levels deep then the cost overhead
        // in terms of cpu will probably be worth it in terms of the amount of
        // memory saved
        // can revisit this after seeing how things go....

        // stack to store the path fragments
        std::stack<std::string> stck;
        TreeNode *curr = this;
        do {
            stck.push(curr->name);
            curr = curr->parent;
        } while (curr != 0);
        std::string tmp = "";
        while (!stck.empty()) {
            tmp += "/";
            tmp += stck.top();
            stck.pop();
        }
        return tmp;
    }

    json toJSON(uint64_t d, uint64_t s = 0) {
        json j;

        j["name"] = name;
        j["path"] = getPath();
        j["data"] = data.toJSON();

        if ( d > 0 && (!children.empty()) ) {
            json child_dirs;
            for (auto iter : children) {
                child_dirs.push_back(iter.second->toJSON(d-1, s));
            }
            j["child_dirs"] = child_dirs;
        }
        return j;
    }

    // adds a *.* to the children of a node
    // this calculates an indexed map which is the combination of
    // all the child indexed maps, and gives the result of subtracting
    // the child combination from the parents
    // don't have to call this server side - it might be better to let
    // the client side work it all out purely from the JSON
    void finalize() {
        // create an clone of the current indexed map
        IndexedMap im(data);

        if (!children.empty()) {
            // loop over children and subtract all their maps from it
            for (auto iter : children) {
                iter.second->finalize();
                im.subtract(iter.second->data);
                }
        }
        if (!im.empty()) {
            TreeNode *child = new TreeNode("*.*", this);
            child->combine(im);
            addChild(child);
        }
    }

    static uint64_t getNodeCount() {
        return node_count.load();
    }

 private:

    // private copy constructor and assignment operator
    // to stop inadverdent copies and to satisfy -Weffc++
    // see http://jrdodds.blogs.com/blog/2004/04/disallowing_cop.html
    TreeNode(const TreeNode&);
    TreeNode& operator=(const TreeNode&);

    std::string name;
    TreeNode *parent;
    IndexedMap data;
    std::unordered_map<std::string, TreeNode*> children;
    uint64_t depth;
    static std::atomic<uint64_t> node_count;
};

#endif  // SRC_TREE_NODE_HPP_
