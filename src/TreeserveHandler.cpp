// Copyright (C)  2015, Wellcome Trust Sanger Institute
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/ResponseBuilder.h>

#include <cstdint>

#include "globals.hpp"

#include "TreeserveHandler.hpp"

// Invoked when we have successfully fetched headers from client. This will
// always be the first callback invoked on your handler.
void TreeserveHandler::onRequest(std::unique_ptr<proxygen::HTTPMessage> r) noexcept {
    request_ = std::move(r);
}

// Invoked when we get part of body for the request.
void TreeserveHandler::onBody(std::unique_ptr<folly::IOBuf> b) noexcept {
    if (body_) {
        body_->prependChain(std::move(b));
    } else {
        body_ = std::move(b);
    }
}

// Invoked when we finish receiving the body.
void TreeserveHandler::onEOM() noexcept {
    LOG(INFO) << "got request " << request_->getQueryString() << std::endl;
    // get the URL path
    if (request_->getPath() == "/api") {
        LOG(INFO) << "URL path was /api" << std::endl;
        // get the path and depth parameters
        std::string path=request_->getQueryParam("path");
        uint64_t depth=static_cast<uint64_t>(request_->getIntQueryParam("depth",0));
        LOG(INFO) << "path parameter was " << path << std::endl;
        LOG(INFO) << "depth parameter was " << depth << std::endl;

        // get JSON
        // tree is a global tree pointer declared in globals.hpp
        // and defined in globals.cpp
        json result = global_tree->toJSON(std::string(path), depth+1);

        // send headers and body
        proxygen::ResponseBuilder(downstream_)
            .status(200, "OK")
            .header("Access-Control-Allow-Origin", "*")
            .header("Cache-Control","public,max-age=3600")
            .body(result.dump(2))
            .sendWithEOM();
    } else {
        LOG(INFO) << "unhandled URL path : " << request_->getPath() << std::endl;
        // if there is a problem
        proxygen::ResponseBuilder(downstream_)
            .status(500, "Server Error")
            .body("invalid request string")
            .sendWithEOM();
    }
}

// Invoked when request processing has been completed and nothing more
// needs to be done. This may be a good place to log some stats and clean
// up resources. This is distinct from onEOM() because it is invoked after
// the response is fully sent. Once this callback has been received,
// downstream_ should be considered invalid.
void TreeserveHandler::requestComplete() noexcept {
    LOG(INFO) << "finishing request " << request_->getQueryString() << std::endl;
    delete this;
}

// Invoked when the session has been upgraded to a different protocol
void TreeserveHandler::onUpgrade(proxygen::UpgradeProtocol) noexcept {
    // handler doesn't support upgrades
}

// Request failed. Maybe because of read/write error on socket or client
// not being able to send request in time.
// NOTE: Can be invoked at any time (except for before onRequest).
// No more callbacks will be invoked after this. You should clean up after
// yourself.
void TreeserveHandler::onError(proxygen::ProxygenError) noexcept {
    delete this;
}


