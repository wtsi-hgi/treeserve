#include "Tree.hpp"

// build a tree from an lstat gzipped file
Tree* TreeBuilder::from_lstat(std::string &lstat_files, std::string &dump_file, uin64_t gzip_buf_size);) {
    // set the current timestamp in epoch seconds,
    // seconds in a year and cost per terabyte per year
    uint64_t now=time(0);
    uint64_t seconds_in_year=60*60*24*365;
    double cost_per_tib_year=150.0;

    //iterate over the lstat files
    uint64_t linecount=0;    
    for (auto it : lstat_files) {
    
        // set up gzip streaming for the file
        std::cout << "processing " << it << std::endl;
        std::ifstream file(it, std::ios_base::in | std::ios_base::binary);
        boost::iostreams::filtering_streambuf<boost::iostreams::input> gz;
        if (gzip_buf_size>0) {
            gz.push(boost::iostreams::gzip_decompressor(15,gzip_buf_size)); // set buffer, first parameter is default 'window bits
        } else {
            gz.push(boost::iostreams::gzip_decompressor());
        }
        gz.push(file);
        std::istream in(&gz);

        // iterate over lines
        for (std::string line; std::getline(in, line);) {
            linecount++;
            if (linecount % 100000 == 0) {
                std::cout << "processed " << linecount << " lines" << std::endl;
            }

             // tokenize the line
            std::vector<std::string> tokens;
            boost::split(tokens, line, boost::is_any_of("\t"));

            // create an IndexedMap object
            IndexedMap im;

            // get the path
            std::string path=base64_decode(tokens[1]);

            // get the size and calc in TiB
            uint64_t size=boost::lexical_cast<uint64_t>(tokens[2]);
            double tib=1.0*size/TiB;

            // get the owner
            uint64_t uid=boost::lexical_cast<uint64_t>(tokens[3]);
            std::string owner=uid_lookup(uid);
     
            // get group
            uint64_t gid=boost::lexical_cast<uint64_t>(tokens[4]);
            std::string grp=gid_lookup(gid);

            // get the atime and calc in years
            uint64_t atime=boost::lexical_cast<uint64_t>(tokens[5]);
            double atime_years=1.0*(now-atime)/seconds_in_year;

            // get the mtime and calc in years
            uint64_t mtime=boost::lexical_cast<uint64_t>(tokens[6]);
            double mtime_years=1.0*(now-mtime)/seconds_in_year;

            // get the ctime and calc in years
            uint64_t ctime=boost::lexical_cast<uint64_t>(tokens[7]);
            double ctime_years=1.0*(now-ctime)/seconds_in_year;

            // get the file type
            std::string file_type=tokens[8];

            // create properties vector
            std::vector<std::string> properties;

            // check what regex-based properties (e.g. suffix match, compressed/uncompressed) apply
            std::unordered_map<std::string, boost::regex>::iterator iter;
            for (auti iter : path_property_regexes) {
                if(regex_match(path, iter.second)) {
                    properties.push_back(iter.first);
                }
            }

            // if no regex-based properties applied, assign to "other"
            if (properties.size() < 1) {
                properties.push_back("other");
            }

            // every entry has '*' property
            properties.push_back("*");

            // add property based on file type
            if (file_type == "d") {
              properties.push_back("directory");
            } else if (file_type == "f") {
              properties.push_back("file");
            } else if (file_type == "l") {
              properties.push_back("link");
            } else {
              properties.push_back("type_" + file_type);
            }

            for (auto property : properties ) {

                // inode counts
                addAttributes(im, "count", static_cast<uint64_t>(1), grp, owner, property);

                // size related
                addAttributes(im, "size", size, grp, owner, property);

                // atime related
                double atime_cost=cost_per_tib_year*tib*atime_years;
                addAttributes(im, "atime", atime_cost, grp, owner, property);

                // mtime related
                double mtime_cost=cost_per_tib_year*tib*mtime_years;
                addAttributes(im, "mtime", mtime_cost, grp, owner, property);

                // ctime related
                double ctime_cost=cost_per_tib_year*tib*ctime_years;
                addAttributes(im, "ctime", ctime_cost, grp, owner, property);
            }

            if (file_type == "d") {
                tree->addNode(path,im);
            } else if (file_type == "f" || file_type == "l") {
                // find last / in the path
                size_t pos=path.find_last_of("/");
                path=path.substr(0,pos);
                tree->addNode(path,im);
            } 
        }
        if ( !in.eof() && in.fail() ) {
          std::cerr << "failed reading input stream: " << strerror(errno) << std::endl;
          return 0;
        }
        

    }        
    tree->finalize();
    std::cout << "Built tree in " << time(0)-now << " seconds" << std::endl;
    return tree;
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
