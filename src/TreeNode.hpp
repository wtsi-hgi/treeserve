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

class TreeNode {

    public :

        TreeNode(std::string n, TreeNode *p=0) : name(n), parent(p), size(0.0), children() {
            if (parent != 0) {
                depth=(parent->depth)+1;
                parent->addChild(this);
            } else {
                depth=0;
            }
        }

        std::string getName() {
            return name;
        }
        
        void  incrSize(uint64_t s) {
            size += s;
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
        
        std::string toJSON(uint64_t d, uint64_t s=0) {
            std::stringstream oss;
            std::string space="";
            for (int i=0; i<s; i++) {
                space+="  ";
            }
            ++s;
            oss << space << "{" << std::endl;
            oss << space << "\"path\" : \"" << getPath() << "\", \"size\" : " << size << std::endl;
            --d;
            if ( d > 0 && (!children.empty()) ) {
                oss << space << "\"children\" : [" << std::endl;
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
            return oss.str();       
        }

        // adds a *.* to the children of a node if the sum of sizes of children is less than the size
        // of the dir itself
        void finalize() {
            // only finalize if the node has children
            if (!children.empty()) {
                // loop over children, sum their sizes and call finalize on them
                uint64_t childSize=0;
                std::unordered_map< std::string, TreeNode* >::iterator it;
                for (it=children.begin(); it != children.end(); it++) {
                    childSize += ((*it).second)->size;
                    ((*it).second)->finalize();
                }
                // once returned from finalizing children, finalize this node
                uint64_t mySize=size-childSize;
                if (mySize > 0) {
                    TreeNode *child=new TreeNode("*.*",this);
                    addChild(child);
                    child->incrSize(size-childSize);
                }
            }
        }
    
    private:
        std::string name;
        TreeNode *parent;   
        uint64_t size;
        std::unordered_map<std::string,TreeNode*> children;
        uint64_t depth;
};

#endif
