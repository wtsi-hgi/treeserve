// Copyright (C)  2015, Wellcome Trust Sanger Institute
#ifndef SRC_TEST_HANDLER_HPP_
#define SRC_TEST_HANDLER_HPP_

#include <memory>

#include <proxygen/httpserver/RequestHandlerFactory.h>
#include <proxygen/httpserver/HTTPServer.h>
#include <proxygen/lib/http/HTTPMessage.h>

class TestHandler : public proxygen::RequestHandler {
 public:
    TestHandler() :body_(), request_() {}

    // Invoked when we have successfully fetched headers from client. This will
    // always be the first callback invoked on your handler.
    void onRequest(std::unique_ptr<proxygen::HTTPMessage> r)
        noexcept override;

    // Invoked when we get part of body for the request.
    void onBody(std::unique_ptr<folly::IOBuf> b) noexcept override;

    // Invoked when we finish receiving the body.
    void onEOM() noexcept override;

    // Invoked when the session has been upgraded to a different protocol
    void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;

    // Invoked when request processing has been completed and nothing more
    // needs to be done. This may be a good place to log some stats and clean
    // up resources. This is distinct from onEOM() because it is invoked after
    // the response is fully sent. Once this callback has been received,
    // downstream_ should be considered invalid.
    void requestComplete() noexcept override;


    // Request failed. Maybe because of read/write error on socket or client
    // not being able to send request in time.
    // NOTE: Can be invoked at any time (except for before onRequest).
    // No more callbacks will be invoked after this. You should clean up after
    // yourself.
    void onError(proxygen::ProxygenError err) noexcept override;

 private:
    std::unique_ptr<folly::IOBuf> body_;
    std::unique_ptr<proxygen::HTTPMessage> request_;
};
#endif  // SRC_TEST_HANDLER_HPP_
