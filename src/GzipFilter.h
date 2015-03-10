/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */
#pragma once

#include <proxygen/httpserver/Filters.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include <proxygen/httpserver/ResponseBuilder.h>

namespace proxygen {

/**
 * A filter that gzips the content if the relevant accept header
 * is set
 */
class GzipFilter : public Filter {
 public:
  explicit GzipFilter(RequestHandler* upstream): Filter(upstream) {
  }


 private:
  uint64_t gzip_content_length;
 
};

class GzipFilterFactory : public RequestHandlerFactory {
 public:
  RequestHandler* onRequest(RequestHandler* h, HTTPMessage* msg)
      noexcept override {
    // check if we have an Accept-Encoding: gzip header
    HTTPHeaders& hdrs=msg->getHeaders();
    if (msg->getHeaders().forEachValueOfHeader("Accept-Encoding",
            [&] (const string& val) { return val=="gzip"; })) {
        return new GzipFilter(h);  
    } else {
      // No need to insert this filter
      return h;
    }
  }
};

}
