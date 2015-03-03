#include <proxygen/httpserver/HTTPServer.h>
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/lib/http/HTTPMessage.h>

#include "TreeserveRouter.hpp"
#include "TreeserveHandler.hpp"

// Invoked in each thread server is going to handle requests before we
// start handling requests. Can be used to setup thread-local setup for
// each thread (stats and such).
void TreeserveRouter::onServerStart() noexcept {
}

// Invoked in each handler thread after all the connections are drained
// from that thread. Can be used to tear down thread-local setup.
void TreeserveRouter::onServerStop() noexcept {
}

// Invoked for each new request server handles. HTTPMessage is provided
// so that user can potentially choose among several implementation of
// handler based on URL or something. No, need to save/copy this
// HTTPMessage. RequestHandler will be given the HTTPMessage in a separate
// callback.
proxygen::RequestHandler* TreeserveRouter::onRequest(proxygen::RequestHandler*,
                proxygen::HTTPMessage*) noexcept {
    return new TreeserveHandler();
}
