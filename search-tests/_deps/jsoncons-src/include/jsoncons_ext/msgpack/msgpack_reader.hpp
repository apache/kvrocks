// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_MSGPACK_MSGPACK_READER_HPP
#define JSONCONS_EXT_MSGPACK_MSGPACK_READER_HPP

#include <cstddef>
#include <memory>
#include <system_error>
#include <utility> // std::move

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/item_event_visitor.hpp>
#include <jsoncons/json_exception.hpp>
#include <jsoncons/json_visitor.hpp>
#include <jsoncons/source.hpp>

#include <jsoncons_ext/msgpack/msgpack_parser.hpp>

namespace jsoncons { namespace msgpack {

template <typename Source,typename Allocator=std::allocator<char>>
class basic_msgpack_reader
{
    using char_type = char;

    basic_msgpack_parser<Source,Allocator> parser_;
    basic_item_event_visitor_to_json_visitor<char_type,Allocator> adaptor_;
    item_event_visitor& visitor_;
public:
    template <typename Sourceable>
    basic_msgpack_reader(Sourceable&& source, 
                      json_visitor& visitor, 
                      const Allocator& alloc)
       : basic_msgpack_reader(std::forward<Sourceable>(source),
                           visitor,
                           msgpack_decode_options(),
                           alloc)
    {
    }

    template <typename Sourceable>
    basic_msgpack_reader(Sourceable&& source, 
                      json_visitor& visitor, 
                      const msgpack_decode_options& options = msgpack_decode_options(),
                      const Allocator& alloc=Allocator())
       : parser_(std::forward<Sourceable>(source), options, alloc),
         adaptor_(visitor, alloc), visitor_(adaptor_)
    {
    }
    template <typename Sourceable>
    basic_msgpack_reader(Sourceable&& source, 
                      item_event_visitor& visitor, 
                      const Allocator& alloc)
       : basic_msgpack_reader(std::forward<Sourceable>(source),
                           visitor,
                           msgpack_decode_options(),
                           alloc)
    {
    }

    template <typename Sourceable>
    basic_msgpack_reader(Sourceable&& source, 
                      item_event_visitor& visitor, 
                      const msgpack_decode_options& options = msgpack_decode_options(),
                      const Allocator& alloc=Allocator())
       : parser_(std::forward<Sourceable>(source), options, alloc),
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

using msgpack_stream_reader = basic_msgpack_reader<jsoncons::binary_stream_source>;

using msgpack_bytes_reader = basic_msgpack_reader<jsoncons::bytes_source>;

} // namespace msgpack
} // namespace jsoncons

#endif // JSONCONS_EXT_MSGPACK_MSGPACK_READER_HPP
