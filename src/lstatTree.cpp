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

// other libraries
#include <microhttpd.h>

// local headers
#include "Tree.hpp"
#include "base64.h"

#define PORT 6666

Tree *tree;

static int answer_to_connection (void *cls, struct MHD_Connection *connection,
                      const char *url, const char *method,
                      const char *version, const char *upload_data,
                      size_t *upload_data_size, void **con_cls)
{
    const char *page = "<html><body>Hello, browser!</body></html>";
    struct MHD_Response *response;
    int ret;

    response =
        MHD_create_response_from_buffer (strlen (page), (void *) page,
        MHD_RESPMEM_PERSISTENT);
    ret = MHD_queue_response (connection, MHD_HTTP_OK, response);
    MHD_destroy_response (response);

    return ret;
}

int main(int argc, char **argv) {

    // get the filename argument
    if (argc != 2) {
        std::cerr << "Usage : lstatTree <data.gz>" << std::endl;
        return 1;
    }
    
    // set up the gzip streaming
    // bzip2 compresses things a bit smaller but is much slowere
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
        boost::split(tokens, line, boost::is_any_of("\t"));

        // get the path
        std::string path=base64_decode(tokens[1]).substr(1);

        // get the size
        double size=atof(tokens[2].c_str())/(1024.0*1024.0*1024.0);

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

    // set up a httpd server listening on port 6666
    // to allow querying the in-memory tree via a
    // rest api
    struct MHD_Daemon *daemon;
    daemon = MHD_start_daemon (MHD_USE_THREAD_PER_CONNECTION, PORT, NULL, NULL,
                             &answer_to_connection, NULL, MHD_OPTION_END);
    if (NULL == daemon) {
        std::cerr << "failed to create server: " << strerror(errno) << std::endl;;
        return 1;
    }

    while(1) {
        pause;
    }
    MHD_stop_daemon (daemon);

    // clean up
    delete tree;

    return 0;
}
