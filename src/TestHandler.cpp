// Copyright (C)  2015, Wellcome Trust Sanger Institute
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/ResponseBuilder.h>

#include "TestHandler.hpp"

// Invoked when we have successfully fetched headers from client. This will
// always be the first callback invoked on your handler.
void TestHandler::onRequest(std::unique_ptr<proxygen::HTTPMessage>
                r) noexcept {
    request_ = std::move(r);
}

// Invoked when we get part of body for the request.
void TestHandler::onBody(std::unique_ptr<folly::IOBuf> b) noexcept {
    if (body_) {
        body_->prependChain(std::move(b));
    } else {
        body_ = std::move(b);
    }
}

// Invoked when we finish receiving the body.
void TestHandler::onEOM() noexcept {
    // build the response body
    std::ostringstream oss;
    oss << "<html><title>TestHandler</title><body>";
    oss << "url was " << request_->getURL() << "<br/>";
    oss << "path was " << request_->getPath() << "<br/>";
    oss << "method was " << request_->getMethodString() << "<br/>";
    oss << "query string was " << request_->getQueryString() << "<br/>";
    oss << "depth parameter was " << request_->getIntQueryParam("depth",0) << "<br/>";
    oss << "path parameter was " << request_->getQueryParam("path") << "<br/>";
    oss << "</body></html>";

    // send headers and body
    proxygen::ResponseBuilder(downstream_)
        .status(200, "OK")
        .header("Access-Control-Allow-Origin", "*")
        .body(oss.str())
        .sendWithEOM();
}

// Invoked when the session has been upgraded to a different protocol
void TestHandler::onUpgrade(proxygen::UpgradeProtocol proto) noexcept {
    // handler doesn't support upgrades
}

// Invoked when request processing has been completed and nothing more
// needs to be done. This may be a good place to log some stats and clean
// up resources. This is distinct from onEOM() because it is invoked after
// the response is fully sent. Once this callback has been received,
// downstream_ should be considered invalid.
void TestHandler::requestComplete() noexcept {
    delete this;
}

// Request failed. Maybe because of read/write error on socket or client
// not being able to send request in time.
// NOTE: Can be invoked at any time (except for before onRequest).
// No more callbacks will be invoked after this. You should clean up after
// yourself.
void TestHandler::onError(proxygen::ProxygenError err) noexcept {
    delete this;
}


