// standard library headers
#include <iostream>
#include <string>
#include <cstdlib>
#include <fstream>
#include <cstring>
#include <cerrno>

// linux syscalls
#include <unistd.h>

// boost headers
#include <boost/algorithm/string.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/lexical_cast.hpp>

// 3rd party local headers

// fossa network library
#include "fossa.h"

// fast tokenizer
#include "strtk.hpp"

// my local headers
#include "Tree.hpp"
#include "base64.h"

Tree *tree;

static const char *s_http_port = "8000";
static struct ns_serve_http_opts s_http_server_opts;

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
    ns_printf(nc, "%s", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
    
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

int main(int argc, char **argv) {

    // get the filename argument
    if (argc != 2) {
        std::cerr << "Usage : lstatTree <data.gz>" << std::endl;
        return 1;
    }
    
    // get the current timestamp in epoch seconds
    uint64_t now=123456789;
    
    // set up the gzip streaming
    // (bzip2 compresses things a bit smaller but is much slower to decompress)
    std::ifstream file(argv[1], std::ios_base::in | std::ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::input> gz;
    gz.push(boost::iostreams::gzip_decompressor());
    gz.push(file);
    std::istream in(&gz);

    // process lines to build the tree    
    tree=new Tree();
    for (std::string line; std::getline(in, line);) {

        // tokenize the line
        std::vector<std::string> tokens;
        strtk::parse( line, "\t", tokens );
        //boost::split(tokens, line, boost::is_any_of("\t"));
        //tokenize(line,tokens,"\t",true);

        // get the path
        std::string path=base64_decode(tokens[1]);

        // get the size
        uint64_t size=boost::lexical_cast<uint64_t>(tokens[2]);
        
        // get the uid
        uint64_t uid=boost::lexical_cast<uint64_t>(tokens[3]);

        // get gid
        uint64_t gid=boost::lexical_cast<uint64_t>(tokens[4]);
                
        // get the ctime
        uint64_t ctime=boost::lexical_cast<uint64_t>(tokens[4]);

        // get the atime
        
        // get the mtime

        // get the file type
        std::string file_type=tokens[8];

        if (file_type == "d") {
            tree->addNode(path,size);
        } else if (file_type == "f") {
            // find last / in the path
            size_t pos=path.find_last_of("/");
            path=path.substr(0,pos);
            tree->addNode(path,size);
        } 
    }
    tree->finalize();

    // print out json for the tree...
    //std::cout << tree->toJSON("lustre/scratch113/admin/hb5");
    //std::cout << tree->toJSON("lustre/scratch113/admin",2);
    //std::cout << tree->toJSON(4);
    
    // start the api server
  struct ns_mgr mgr;
  struct ns_connection *nc;
  int i;

  ns_mgr_init(&mgr, NULL);
  nc = ns_bind(&mgr, s_http_port, ev_handler);
  ns_set_protocol_http_websocket(nc);
  s_http_server_opts.document_root = ".";


  printf("Starting RESTful server on port %s\n", s_http_port);
  for (;;) {
    ns_mgr_poll(&mgr, 1000);
  }
  ns_mgr_free(&mgr);
  
    // clean up
    delete tree;

    return 0;
}
