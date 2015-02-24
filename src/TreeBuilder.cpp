#include "Tree.hpp"

// build a tree from an lstat gzipped file
Tree* TreeBuilder::from_lstat(std::string &lstat_file, std::string &dump_file) {
}

// re-serialize a tree from a previous save
Tree* TreeBuilder::from_serial(std::string &serial_file) {
}

// convert a uid into it's text equivalent
// retrieve from the map if it's there, otherwise do a syscall and cache it
std::string TreeBuilder::uid_lookup(uint64_t uid) {
    // is the uid in the map ?
    auto got = uid_map.find(uid);
    if (got == uid_map.end()) {
        struct passwd *pwd=getpwuid(uid);
        if (pwd) {
            std::string uid_str(pwd->pw_name);
            uid_map.insert(std::make_pair(uid,uid_str));
            return uid_str;
        } else {
            // uid not in the db, just return the uid
            std::string uid_str=boost::lexical_cast<std::string>(uid);
            uid_map.insert(std::make_pair(uid,uid_str));
            return uid_str;
        }
    } else {
        return got.second;
    }
}

// convert a gid into it's text equivalent
// retrieve from the map if it's there, otherwise do a syscall and cache it
std::string TreeBuilder::gid_lookup(uint64_t gid) {
    // is the gid in the map ?
    auto got = gid_map.find(gid);
    if (got == gid_map.end()) {
        struct group *grp=getgrgid(gid);
        if (grp) {
            std::string grp_str(grp->gr_name);
            gid_map.insert(std::make_pair(gid,grp_str));
            return grp_str;
        } else {
            // gid not in the db, just return the gid
            std::string grp_str=boost::lexical_cast<std::string>(gid);
            gid_map.insert(std::make_pair(gid,grp_str));
            return grp_str;
        }
    } else {
        return got.second;
    }
}
