// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_CBOR_CBOR_EVENT_READER_HPP
#define JSONCONS_EXT_CBOR_CBOR_EVENT_READER_HPP

#include <cstddef>
#include <memory>
#include <system_error>
#include <utility> // std::move

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/source.hpp>

#include <jsoncons_ext/cbor/cbor_detail.hpp>
#include <jsoncons_ext/cbor/cbor_encoder.hpp>
#include <jsoncons_ext/cbor/cbor_error.hpp>
#include <jsoncons_ext/cbor/cbor_parser.hpp>

namespace jsoncons { namespace cbor {

template <typename Source,typename Allocator=std::allocator<char>>
class basic_cbor_reader 
{
    using char_type = char;

    basic_cbor_parser<Source,Allocator> parser_;
    basic_item_event_visitor_to_json_visitor<char_type,Allocator> adaptor_;
    item_event_visitor& visitor_;
public:
    template <typename Sourceable>
    basic_cbor_reader(Sourceable&& source, 
                      json_visitor& visitor, 
                      const Allocator& alloc)
       : basic_cbor_reader(std::forward<Sourceable>(source),
                           visitor,
                           cbor_decode_options(),
                           alloc)
    {
    }

    template <typename Sourceable>
    basic_cbor_reader(Sourceable&& source, 
                      json_visitor& visitor, 
                      const cbor_decode_options& options = cbor_decode_options(),
                      const Allocator& alloc=Allocator())
       : parser_(std::forward<Sourceable>(source), options, alloc),
         adaptor_(visitor, alloc), visitor_(adaptor_)
    {
    }
    template <typename Sourceable>
    basic_cbor_reader(Sourceable&& source, 
                      item_event_visitor& visitor, 
                      const Allocator& alloc)
       : basic_cbor_reader(std::forward<Sourceable>(source),
                           visitor,
                           cbor_decode_options(),
                           alloc)
    {
    }

    template <typename Sourceable>
    basic_cbor_reader(Sourceable&& source, 
                      item_event_visitor& visitor, 
                      const cbor_decode_options& options = cbor_decode_options(),
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

using cbor_stream_reader = basic_cbor_reader<jsoncons::binary_stream_source>;

using cbor_bytes_reader = basic_cbor_reader<jsoncons::bytes_source>;

} // namespace cbor_reader
} // namespace jsoncons

#endif // JSONCONS_EXT_CBOR_CBOR_EVENT_READER_HPP
