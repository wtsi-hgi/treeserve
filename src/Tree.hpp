#ifndef __TREE_HPP__
#define __TREE_HPP__

#include <cstdint>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/trim.hpp>

#include "TreeNode.hpp"

class Tree {
    
    public :

        Tree() : root(0) {}

        ~Tree() {delete root;}
        
        void addNode(std::string path, IndexedMap& im) {
            // path will be a string of form (/)a/b/c/d(/)
            // need to create any nodes that don't exist
            // e.g. for the above path, if we are adding to an empty tree
            // will need to create the 'a' node, then create 'b' as a child, then 'c'
            // as a child of 'b' then add the leaf 'd' as a child of 'c'
            // also need to combine the indexed map on each node as we descend down
            // we may not need to actually make any nodes if all the ones in the path exist,
            // but we need to combine the map in the path
            
            // turn the path into a vector of names
            std::vector<std::string> names;
            boost::trim_if(path, boost::is_any_of("/"));
            boost::split(names, path, boost::is_any_of("/"));            
            if (root==0) {
                root=new TreeNode(names[0]);
            }
            TreeNode *current=root;
            std::vector<std::string>::iterator it=names.begin();
            ++it;
            for (;it<names.end();it++) {
                current->combine(im);
                TreeNode *tmp=current->getChild(*it);
                if (tmp == 0) {
                    current=new TreeNode(*it,current);
                } else {
                    current=tmp;
                }
            }
            current->combine(im);
        }

        TreeNode* getNodeAt(std::string path) {
            // turn the path into a vector of names
            std::vector<std::string> names;
            boost::trim_if(path, boost::is_any_of("/"));
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

        TreeNode* getRoot() {
            return root;
        }
        
        // once we've finished a tree, want to add  a child to each node to represent *.*
        // i.e. size of files within the directory itself. this will be calculated by
        // summing the sizes of all children and subtracting from the size of the node
        void finalize() {
            if (root != NULL) {
              root->finalize();
            }
        }
        
        json toJSON(std::string path, uint64_t d=std::numeric_limits<uint64_t>::max()) {
            json j;
            if (d==0) d=1;
            TreeNode *tmp=getNodeAt(path);
            if (tmp == NULL) {
              j =  json::object();
            } else {
              j = tmp->toJSON(d,0);
            }
            return j;
        }
        json toJSON(uint64_t d) {
            if (d==0) d=1;
            return root->toJSON(d,0);
        }
        json toJSON() {
            return root->toJSON(std::numeric_limits<uint64_t>::max(),0);
        }    
    private:

        // private copy constructor and assignment operator
        // to stop inadverdent copies and to satisfy -Weffc++
        // see http://jrdodds.blogs.com/blog/2004/04/disallowing_cop.html
        Tree(const Tree&);
        Tree& operator=(const Tree&);

        TreeNode *root;
};

#endif

