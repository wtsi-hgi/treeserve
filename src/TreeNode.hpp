#ifndef __TREE_NODE_HPP__
#define __TREE_NODE_HPP__

#include <string>
#include <vector>
#include <unordered_map>
#include <limits>
#include <sstream>
#include <utility>
#include <stack>

#include <boost/algorithm/string/join.hpp>

#include "IndexedMap.hpp"
#include "Datum.hpp"

// nlohmann's json source library
#include "json.hpp"
using json = nlohmann::json;

class TreeNode {

    public :

        TreeNode(std::string n, TreeNode *p=0) : name(n), parent(p), data(), children() {
            if (parent != 0) {
                depth=(parent->depth)+1;
                parent->addChild(this);
            } else {
                depth=0;
            }
        }
		~TreeNode() {
			std::unordered_map<std::string,TreeNode*>::iterator it;
			for (it=children.begin(); it != children.end(); it++) {
				delete it->second;
			}
		}

        std::string getName() {
            return name;
        }
        
        void combine(IndexedMap &other_map) {
            data.combine(other_map);
        }    

        void addChild(TreeNode* c) {
            children.insert(std::make_pair(c->getName(),c));
        }

        TreeNode* getChild(std::string n) {
            std::unordered_map< std::string, TreeNode* >::const_iterator got = children.find(n);
            if (got==children.end()) {
                return 0;
            } else {
                return (*got).second;
            }
        }

        std::string getPath() {
            // this recurses up the parent links to construct the full path to a node
            // don't want to store the full path in the node as that will increase the memory requirements
            // the only time we really need this is when we output the json
            // if we are only outputting 2 or 3 levels deep then the cost overhead in terms of cpu
            // will probably be worth it in terms of the amount of memory saved
            // can revisit this after seeing how things go....
            
            // stack to store the path fragments
            std::stack<std::string> stck;
            TreeNode *curr=this;
            do {
                stck.push(curr->name);
                curr=curr->parent;
            } while (curr != 0);
            std::string tmp="";
            while (!stck.empty()) {
                tmp += "/";
                tmp += stck.top();
                stck.pop();
            }
            return tmp;
        }
        
        json toJSON(uint64_t d, uint64_t s=0) {
            std::stringstream oss;
            std::string space="";
            for (int i=0; i<s; i++) {
                space+="  ";
            }
            ++s;
            oss << space << "{" << std::endl;
	    oss << space << "\"name\": \"" << name << "\", " << "\"path\": \"" << getPath() << "\", " << data.toJSON() << std::endl;
            --d;
            if ( d > 0 && (!children.empty()) ) {
                oss << space << ", \"child_dirs\": [" << std::endl;
                bool sep=false;
                std::unordered_map< std::string, TreeNode* >::iterator it;
                for (it=children.begin(); it != children.end(); it++) {
                    if (sep) {
                        oss << space << "," << std::endl;;
                        sep=true;
                    }
                    oss << ((*it).second)->toJSON(d,s);
                    sep=",";
                }
                oss << space << "]" << std::endl;
            }
            oss << space << "}" << std::endl;
            return json::parse(oss.str());
        }

        // adds a *.* to the children of a node
        // this calculates an indexed map which is the combination of
        // all the child indexed maps, and gives the result of subtracting
        // the child combination from the parents
        // don't have to call this server side - it might be better to let
        // the client side work it all out purely from the JSON
        void finalize() {
            // only finalize if the node has children
            if (!children.empty()) {
                // create an clone of the current indexed map
                IndexedMap im(data);
                // loop over children and subtract all their maps from it
                for (auto it : children) {
		    it.second->finalize();
                    im.subtract(it.second->data);
                }
                // create the *.* child if the resultant map is not empty
                if (!im.empty()) {
#ifndef NDEBUG
					std::cout << "creating *.* child at " << getPath() << std::endl;
#endif
                    TreeNode *child=new TreeNode("*.*",this);
                    child->combine(im);
                    addChild(child);
                }
            }
        }
    
    private:
        std::string name;
        TreeNode *parent;   
        IndexedMap data;
        std::unordered_map<std::string,TreeNode*> children;
        uint64_t depth;
};

#endif
