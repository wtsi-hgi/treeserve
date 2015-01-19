#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <limits>
#include <sstream>
#include <utility>


class TreeNode {
    public :

        TreeNode(std::string n, TreeNode *p) : name(n), parent(p), size(0), children() {
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
        void  incrSize(int s) {
            size += s;
        }    

        void addChild(TreeNode* c) {
            children.insert(std::make_pair(c->getName(),c));
        }

        bool hasChild(std::string n) {
            std::unordered_map< std::string, TreeNode* >::const_iterator got = children.find(n);
            if (got==children.end()) {
                return false;
            } else {
                return true;
            }
        }

        std::string toJson(uint64_t d=std::numeric_limits<uint64_t>::max()) {
            std::stringstream oss;
            std::string space="";
            for (int i=0; i<depth; i++) {
                space += " ";
            }
            oss << space << "{" << std::endl;
            oss << space << " " << "\"" << name << "\"" << " : " << size << std::endl;
            if (depth<d) {
                std::unordered_map< std::string, TreeNode* >::iterator it;
                for (it=children.begin(); it != children.end(); it++) {
                    oss << ((*it).second)->toJson();
                }
            }
            oss << space << "}" << std::endl;
            return oss.str();       
        }

    
    private:
        TreeNode *parent;
        uint64_t depth;
        std::string name;
        uint64_t size;
        std::unordered_map<std::string,TreeNode*> children;
};
