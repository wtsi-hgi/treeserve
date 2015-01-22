#ifndef __TREE_NODE_HPP__
#define __TREE_NODE_HPP__

#include <string>
#include <vector>
#include <unordered_map>
#include <limits>
#include <sstream>
#include <utility>


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

        std::string  getName() {
            return name;
        }
        
        void  incrSize(double s) {
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

        std::string toJSON(uint64_t d=std::numeric_limits<uint64_t>::max()) {
            std::stringstream oss;
            std::string space="";
            for (uint64_t i=0; i<depth; i++) {
                space += " ";
            }
            oss << space << "{" << std::endl;
            oss << space << " " << "\"" << name << "\"" << " : " << size << std::endl;
            if (depth<d) {
                std::unordered_map< std::string, TreeNode* >::iterator it;
                for (it=children.begin(); it != children.end(); it++) {
                    oss << ((*it).second)->toJSON();
                }
            }
            oss << space << "}" << std::endl;
            return oss.str();       
        }

    
    private:
        std::string name;
        TreeNode *parent;   
        double size;
        std::unordered_map<std::string,TreeNode*> children;
        uint64_t depth;
};

#endif
