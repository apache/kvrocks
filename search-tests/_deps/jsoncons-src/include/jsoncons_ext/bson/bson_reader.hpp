// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_BSON_BSON_READER_HPP
#define JSONCONS_EXT_BSON_BSON_READER_HPP

#include <cstddef>
#include <memory>
#include <system_error>
#include <utility> // std::move

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/json_exception.hpp>
#include <jsoncons/json_visitor.hpp>
#include <jsoncons/source.hpp>
#include <jsoncons_ext/bson/bson_parser.hpp>

namespace jsoncons { namespace bson {

template <typename Source,typename TempAllocator =std::allocator<char>>
class basic_bson_reader 
{
    basic_bson_parser<Source,TempAllocator> parser_;
    json_visitor& visitor_;
public:
    template <typename Sourceable>
    basic_bson_reader(Sourceable&& source, 
                      json_visitor& visitor, 
                      const TempAllocator& temp_alloc)
       : basic_bson_reader(std::forward<Sourceable>(source),
                           visitor,
                           bson_decode_options(),
                           temp_alloc)
    {
    }

    template <typename Sourceable>
    basic_bson_reader(Sourceable&& source, 
                      json_visitor& visitor, 
                      const bson_decode_options& options = bson_decode_options(),
                      const TempAllocator& temp_alloc=TempAllocator())
       : parser_(std::forward<Sourceable>(source), options, temp_alloc),
         visitor_(visitor)
    {
    }

    void read()
    {
        std::error_code ec;
        read(ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec,line(),column()));
        }
    }

    void read(std::error_code& ec)
    {
        parser_.reset();
        parser_.parse(visitor_, ec);
        if (ec)
        {
            return;
        }
    }

    std::size_t line() const
    {
        return parser_.line();
    }

    std::size_t column() const
    {
        return parser_.column();
    }
};

using bson_stream_reader = basic_bson_reader<jsoncons::binary_stream_source>;
using bson_bytes_reader = basic_bson_reader<jsoncons::bytes_source>;

} // namespace bson
} // namespace jsoncons

#endif // JSONCONS_EXT_BSON_BSON_READER_HPP
