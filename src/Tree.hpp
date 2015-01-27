#ifndef __TREE_HPP__
#define __TREE_HPP__

#include <cstdint>

#include <boost/algorithm/string.hpp>

#include "TreeNode.hpp"

class Tree {
    
    public :

        Tree() : root(0) {}

        ~Tree() {delete root;}
        
        void addNode(std::string path, double size) {
            // path will be a string of form a/b/c/d
            // i.e. it mustn't start with a / but with a real name
            // need to create any nodes that don't exist
            // e.g. for the above path, if we are adding to an empty tree
            // will need to create the 'a' node, then create 'b' as a child, then 'c'
            // as a child of 'b' then add the leaf 'd' as a child of 'c'
            // also need to increment the size on each node as we descend down
            // we may not need to actually make any nodes if all the ones in the path exist,
            // but we need to increment the size on each node in the path
            
            // turn the path into a vector of names
            std::vector<std::string> names;
            boost::split(names, path, boost::is_any_of("/"));            
            if (root==0) {
                root=new TreeNode(names[0]);
            }
            TreeNode *current=root;
            std::vector<std::string>::iterator it=names.begin();
            ++it;
            for (;it<names.end();it++) {
                current->incrSize(size);
                TreeNode *tmp=current->getChild(*it);
                if (tmp == 0) {
                    current=new TreeNode(*it,current);
                } else {
                    current=tmp;
                }
            }
            current->incrSize(size);
        }

        TreeNode* getNodeAt(std::string path) {
            // turn the path into a vector of names
            std::vector<std::string> names;
            boost::split(names, path, boost::is_any_of("/"));
            TreeNode *current=root;
            std::vector<std::string>::iterator it=names.begin();
            ++it;
            for (;it<names.end();it++) {
                current=current->getChild(*it);
                if (current==0) {
                    return 0;
                }
            }
            return current;
        }

        std::string toJSON(std::string path, uint64_t d=std::numeric_limits<uint64_t>::max()) {
            TreeNode *tmp=getNodeAt(path);
            return tmp->toJSON(d,0);
        }
        std::string toJSON(uint64_t d) {
            return root->toJSON(d,0);
        }
        std::string toJSON() {
            return root->toJSON(std::numeric_limits<uint64_t>::max(),0);
        }    
    private:
        TreeNode *root;
};

#endif
