// comment line below to enable debug mode
#define NDEBUG

// standard library headers
#include <iostream>
#include <string>
#include <cstdlib>
#include <fstream>
#include <cstring>
#include <cerrno>
#include <sstream>
#include <unordered_map>

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

// 3rd party local headers

// fossa network library
#include "fossa.h"

// my local headers
#include "Tree.hpp"
#include "IndexedMap.hpp"
#include "base64.h"

// globals

// the tree structure
Tree *tree;

// stuff for the http server
static struct ns_serve_http_opts s_http_server_opts;

// maps for cacheing uid and gid lookups
std::unordered_map<uint64_t, std::string> uid_map;
std::unordered_map<uint64_t, std::string> gid_map;

// map for defining "properties" by regex
// TODO: load property definitions from a configuration file 
// rather than hardcoding them
static std::unordered_map<std::string, boost::regex> path_property_regexes {
    {"cram", boost::regex (".*[.]cram$")},
    {"bam", boost::regex (".*[.]bam$")},
    {"index", boost::regex (".*[.](crai|bai|sai|fai|csi)$")},
    {"compressed", boost::regex (".*[.](bzip2|gz|tgz|zip|xz|bgz|bcf)$")},
    {"uncompressed", boost::regex (".*([.]sam|[.]fasta|[.]fastq|[.]fa|[.]fq|[.]vcf|[.]csv|[.]tsv|[.]txt|[.]text|README|[.]o|[.]e|[.]oe|[.]dat)$")},
    {"checkpoint", boost::regex (".*jobstate[.]context$")},
    {"temporary", boost::regex (".*(tmp|TMP|temp|TEMP).*")},
};

// convert a uid into it's text equivalent
// retrieve from the map if it's there, otherwise do a syscall and cache it
std::string uid_lookup(uint64_t uid) {
    // is the uid in the map ?
    std::unordered_map<uint64_t, std::string>::const_iterator got = uid_map.find(uid);
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
        return uid_map[uid];
    }
}

// convert a gid into it's text equivalent
// retrieve from the map if it's there, otherwise do a syscall and cache it
std::string gid_lookup(uint64_t gid) {
    // is the gid in the map ?
    std::unordered_map<uint64_t, std::string>::const_iterator got = gid_map.find(gid);
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
        return gid_map[gid];
    }
}

static void handle_sum_call(struct ns_connection *nc, struct http_message *hm) {
    char path[4*1024];
    char depth[10];
    // get the path request argument
    ns_get_http_var(&(hm->query_string),"path",path,sizeof(path));

    // get the depth argument
    ns_get_http_var(&(hm->query_string),"depth",depth,sizeof(depth));

    /* Compute the JSON */
    // get uint64_t from the depth parameter
    uint64_t d;
    try {
        d=boost::lexical_cast<uint64_t>(depth);
    } catch (...) {
        d=1;
    }
    // debug
    std::cout << "path=" << path << ", depth=" << d << std::endl;
    
    // get JSON
    json result = tree->toJSON(std::string(path), d+1);

    /* Send headers */
    ns_printf(nc, "%s", "HTTP/1.1 200 OK\r\nAccess-Control-Allow-Origin: *\r\nTransfer-Encoding: chunked\r\n\r\n");
    
    // send json -- pretty printed with 2 spaces per indent level
    ns_printf_http_chunk(nc, "%s", result.dump(2).c_str()); 
    ns_send_http_chunk(nc, "", 0);  /* Send empty chunk, the end of response */
}

static void ev_handler(struct ns_connection *nc, int ev, void *ev_data) {
  struct http_message *hm = (struct http_message *) ev_data;

  switch (ev) {
    case NS_HTTP_REQUEST:
      if (ns_vcmp(&hm->uri, "/api") == 0) {
        handle_sum_call(nc, hm);                    /* Handle RESTful call */
      } else {
        ns_serve_http(nc, hm, s_http_server_opts);  /* Serve static content */
      }
      break;
    default:
      break;
  }
}

template<typename T>
void addAttribute(IndexedMap &im, std::string attr_name, T attr_val) {
    im.addItem(attr_name,attr_val);
}

template<typename T>
void addAttribute(IndexedMap &im, std::string attr_name, T attr_val, std::string gid_str, std::string uid_str, std::string property) {
    std::ostringstream oss;
    oss << attr_name << "$" << gid_str << "$" << uid_str << "$" << property;
    addAttribute(im, oss.str(),attr_val);
}

template<typename T>
void addAttributes(IndexedMap &im, std::string attr_name, T attr_val, std::string grp, std::string usr, std::string property) {
    addAttribute(im, attr_name, attr_val, "*", "*", property);
    addAttribute(im, attr_name, attr_val, grp, "*", property);
    addAttribute(im, attr_name, attr_val, "*", usr, property);
    addAttribute(im, attr_name, attr_val, grp, usr, property);
}

int main(int argc, char **argv) {

    // get the filename argument
    if (argc != 3) {
        std::cerr << "Usage : lstatTree <port> <data.gz>" << std::endl;
        return 1;
    }
    
    // get the current timestamp in epoch seconds
    // and seconds in a year
    uint64_t now=time(0);
    uint64_t seconds_in_year=60*60*24*365;
    double cost_per_tib_year=150.0;

    // conversion into tebibytes...
    uint64_t TiB=1024;
    TiB *=1024;
    TiB *=1024;
    TiB *=1024;
        
    // set up the gzip streaming
    // (bzip2 compresses things a bit smaller but is much slower to decompress)
    std::ifstream file(argv[2], std::ios_base::in | std::ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::input> gz;
    gz.push(boost::iostreams::gzip_decompressor(15,256*1024*1024)); // set buffer to 16M, first parameter is default 'window bits'
    gz.push(file);
    std::istream in(&gz);

    // process lines to build the tree    
    tree=new Tree();
    uint64_t linecount=0;
    std::cout << "Building tree..." <<std::endl;
    for (std::string line; std::getline(in, line);) {
        linecount++;
        if (linecount % 10000 == 0) {
            std::cout << "Processed " << linecount << " lines" << std::endl;
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
        for (iter=path_property_regexes.begin(); iter != path_property_regexes.end(); iter++) {
            if(regex_match(path, iter->second)) {
                properties.push_back(iter->first);
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

        for (std::vector<std::string>::iterator iter = properties.begin(); iter != properties.end(); ++iter) {
            std::string property = *iter;

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
      return 1;
    }
    tree->finalize();
    
    std::cout << "Built tree in " << time(0)-now << " seconds" << std::endl;
#ifndef NDEBUG
        std::cout << "in debug section, printing out tree and exiting" << std::endl;
        std::cout << std::setw(2) << tree->toJSON() << std::endl;
    // tidy up and stop - want to bail out here to gperf the tree construction
    // top optimize it and to make sure it passes valgrind without issue
    delete tree;
    return 0;
#endif
    
    // print out json for the tree...
    //std::cout << tree->toJSON("lustre/scratch113/admin/hb5");
    //std::cout << tree->toJSON("lustre/scratch113/admin",2);
    //std::cout << tree->toJSON(4);
    
    // start the api server
    struct ns_mgr mgr;
    struct ns_connection *nc;
    int i;

    ns_mgr_init(&mgr, NULL);
    nc = ns_bind(&mgr, argv[1], ev_handler);
    ns_set_protocol_http_websocket(nc);
    s_http_server_opts.document_root = ".";


    std::cout << "Starting RESTful server on port " << argv[1] << std::endl;
    for (;;) {
      ns_mgr_poll(&mgr, 1000);
    }
    ns_mgr_free(&mgr);
  
    // clean up
    delete tree;

    return 0;
}
