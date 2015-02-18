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
    std::string result=tree->toJSON(std::string(path),d+1);

    /* Send headers */
    ns_printf(nc, "%s", "HTTP/1.1 200 OK\r\nAccess-Control-Allow-Origin: *\r\nTransfer-Encoding: chunked\r\n\r\n");
    
    // send json
    ns_printf_http_chunk(nc, "%s", result.c_str());
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
void addAttribute(IndexedMap &im, std::string attr_name, T attr_val, std::string uid_str) {
    std::ostringstream oss;
    oss << attr_name << uid_str;
    im.addItem(oss.str(),attr_val);
}

template<typename T>
void addAttribute(IndexedMap &im, std::string attr_name, T attr_val, std::string gid_str, std::string uid_str) {
    std::ostringstream oss;
    oss << attr_name << gid_str << "_" << uid_str;
    im.addItem(oss.str(),attr_val);
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
    gz.push(boost::iostreams::gzip_decompressor());
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

        // add atributes to the im...

        // size related
        addAttribute(im,"size",size);
        addAttribute(im,"size_by_uid_",size,owner);
        addAttribute(im,"size_by_gid_",size,grp);
        addAttribute(im,"size_by_gid_uid_",size,grp,owner);
        
        // atime related
        double atime_cost=cost_per_tib_year*tib*atime_years;
        addAttribute(im,"atime_cost",atime_cost);
        addAttribute(im,"atime_cost_by_uid_",atime_cost,owner);
        addAttribute(im,"atime_cost_by_gid_",atime_cost,grp);
        addAttribute(im,"atime_cost_by_gid_uid_",atime_cost,grp,owner);

        // mtime related
        double mtime_cost=cost_per_tib_year*tib*mtime_years;
        addAttribute(im,"mtime_cost",atime_cost);
        addAttribute(im,"mtime_cost_by_uid_",atime_cost,owner);
        addAttribute(im,"mtime_cost_by_gid_",atime_cost,grp);
        addAttribute(im,"mtime_cost_by_gid_uid_",atime_cost,grp,owner);

        // ctime related
        double ctime_cost=cost_per_tib_year*tib*ctime_years;
        addAttribute(im,"ctime_cost",ctime_cost);
        addAttribute(im,"ctime_cost_by_uid_",ctime_cost,owner);
        addAttribute(im,"ctime_cost_by_gid_",ctime_cost,grp);
        addAttribute(im,"ctime_cost_by_gid_uid_",ctime_cost,grp,owner);

        // TODO : file suffix related (bams, vcfs etc)

        // get the file type
        std::string file_type=tokens[8];

        if (file_type == "d") {
            tree->addNode(path,im);
        } else if (file_type == "f" || file_type == "l") {
            // find last / in the path
            size_t pos=path.find_last_of("/");
            path=path.substr(0,pos);
            tree->addNode(path,im);
        } 
    }
    tree->finalize();

    std::cout << "Built tree in " << time(0)-now << " seconds" << std::endl;
#ifndef NDEBUG
	std::cout << "in debug section, printing out tree and exiting" << std::endl;
    std::cout << tree->toJSON();
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
