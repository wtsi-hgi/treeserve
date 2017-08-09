// Copyright 2007 Timo Bingmann <tb@panthema.net>
// Distributed under the Boost Software License, Version 1.0.
// (See http://www.boost.org/LICENSE_1_0.txt)

#ifndef SRC_COMPRESS_HPP_
#define SRC_COMPRESS_HPP_

// Compress a STL string using zlib with given compression level and return
// the binary data
std::string compress_string(const std::string& str,
                            int compressionlevel = Z_BEST_COMPRESSION);

// Decompress an STL string using zlib and return the original data.
std::string decompress_string(const std::string& str);

#endif  // SRC_COMPRESS_HPP_
