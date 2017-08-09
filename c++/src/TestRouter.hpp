#ifndef SRC_TEST_ROUTER_HPP_
#define SRC_TEST_ROUTER_HPP_

#include <proxygen/httpserver/RequestHandlerFactory.h>
#include <proxygen/httpserver/HTTPServer.h>
#include <proxygen/lib/http/HTTPMessage.h>

class TestRouter : public proxygen::RequestHandlerFactory {
 public:
    // Invoked in each thread server is going to handle requests before we
    // start handling requests. Can be used to setup thread-local setup for
    // each thread (stats and such).
    void onServerStart() noexcept override;

    // Invoked in each handler thread after all the connections are drained
    // from that thread. Can be used to tear down thread-local setup.
    void onServerStop() noexcept override;

    // Invoked for each new request server handles. HTTPMessage is provided
    // so that user can potentially choose among several implementation of
    // handler based on URL or something. No, need to save/copy this
    // HTTPMessage. RequestHandler will be given the HTTPMessage in a separate
    // callback.
    proxygen::RequestHandler* onRequest(proxygen::RequestHandler *rh, proxygen::HTTPMessage *msg)
        noexcept override;
};
#endif  // SRC_TEST_ROUTER_HPP_
