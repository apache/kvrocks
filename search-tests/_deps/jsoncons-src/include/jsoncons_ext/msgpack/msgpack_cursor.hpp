// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_MSGPACK_MSGPACK_CURSOR_HPP
#define JSONCONS_EXT_MSGPACK_MSGPACK_CURSOR_HPP

#include <cstddef>
#include <functional>
#include <memory> // std::allocator
#include <system_error>

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/item_event_visitor.hpp>
#include <jsoncons/json_exception.hpp>
#include <jsoncons/json_visitor.hpp>
#include <jsoncons/ser_context.hpp>
#include <jsoncons/source.hpp>
#include <jsoncons/staj_cursor.hpp>

#include <jsoncons_ext/msgpack/msgpack_options.hpp>
#include <jsoncons_ext/msgpack/msgpack_parser.hpp>

namespace jsoncons { 
namespace msgpack {

template <typename Source=jsoncons::binary_stream_source,typename Allocator=std::allocator<char>>
class basic_msgpack_cursor : public basic_staj_cursor<char>, private virtual ser_context
{
public:
    using source_type = Source;
    using char_type = char;
    using allocator_type = Allocator;
private:
    basic_msgpack_parser<Source,Allocator> parser_;
    basic_staj_visitor<char_type> cursor_visitor_;
    basic_item_event_visitor_to_json_visitor<char_type,Allocator> cursor_handler_adaptor_;
    bool eof_;

public:
    using string_view_type = string_view;

    // Noncopyable and nonmoveable
    basic_msgpack_cursor(const basic_msgpack_cursor&) = delete;
    basic_msgpack_cursor(basic_msgpack_cursor&&) = delete;

    template <typename Sourceable>
    basic_msgpack_cursor(Sourceable&& source,
                         const msgpack_decode_options& options = msgpack_decode_options(),
                         const Allocator& alloc = Allocator())
        : parser_(std::forward<Sourceable>(source), options, alloc), 
          cursor_visitor_(accept_all),
          cursor_handler_adaptor_(cursor_visitor_, alloc),
          eof_(false)
    {
        if (!done())
        {
            next();
        }
    }

    // Constructors that set parse error codes

    template <typename Sourceable>
    basic_msgpack_cursor(Sourceable&& source,
                         std::error_code& ec)
       : basic_msgpack_cursor(std::allocator_arg, Allocator(),
                              std::forward<Sourceable>(source), 
                              msgpack_decode_options(), 
                              ec)
    {
    }

    template <typename Sourceable>
    basic_msgpack_cursor(Sourceable&& source,
                         const msgpack_decode_options& options,
                         std::error_code& ec)
       : basic_msgpack_cursor(std::allocator_arg, Allocator(),
                              std::forward<Sourceable>(source), 
                              options, 
                              ec)
    {
    }

    template <typename Sourceable>
    basic_msgpack_cursor(std::allocator_arg_t, const Allocator& alloc, 
                         Sourceable&& source,
                         const msgpack_decode_options& options,
                         std::error_code& ec)
       : parser_(std::forward<Sourceable>(source), options, alloc), 
         cursor_visitor_(accept_all),
         cursor_handler_adaptor_(cursor_visitor_, alloc),
         eof_(false)
    {
        if (!done())
        {
            next(ec);
        }
    }

    basic_msgpack_cursor& operator=(const basic_msgpack_cursor&) = delete;
    basic_msgpack_cursor& operator=(basic_msgpack_cursor&&) = delete;

    ~basic_msgpack_cursor() = default;
    
    void reset()
    {
        parser_.reset();
        cursor_visitor_.reset();
        cursor_handler_adaptor_.reset();
        eof_ = false;
        if (!done())
        {
            next();
        }
    }

    template <typename Sourceable>
    void reset(Sourceable&& source)
    {
        parser_.reset(std::forward<Sourceable>(source));
        cursor_visitor_.reset();
        cursor_handler_adaptor_.reset();
        eof_ = false;
        if (!done())
        {
            next();
        }
    }

    void reset(std::error_code& ec)
    {
        parser_.reset();
        cursor_visitor_.reset();
        cursor_handler_adaptor_.reset();
        eof_ = false;
        if (!done())
        {
            next(ec);
        }
    }

    template <typename Sourceable>
    void reset(Sourceable&& source, std::error_code& ec)
    {
        parser_.reset(std::forward<Sourceable>(source));
        cursor_visitor_.reset();
        cursor_handler_adaptor_.reset();
        eof_ = false;
        if (!done())
        {
            next(ec);
        }
    }

    bool done() const override
    {
        return parser_.done();
    }

    const staj_event& current() const override
    {
        return cursor_visitor_.event();
    }

    void read_to(basic_json_visitor<char_type>& visitor) override
    {
        std::error_code ec;
        read_to(visitor, ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec,parser_.line(),parser_.column()));
        }
    }

    void read_to(basic_json_visitor<char_type>& visitor,
                std::error_code& ec) override
    {
        if (cursor_visitor_.dump(visitor, *this, ec))
        {
            read_next(visitor, ec);
        }
    }

    void next() override
    {
        std::error_code ec;
        next(ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec,parser_.line(),parser_.column()));
        }
    }

    void next(std::error_code& ec) override
    {
        read_next(ec);
    }

    const ser_context& context() const override
    {
        return *this;
    }

    bool eof() const
    {
        return eof_;
    }

    std::size_t line() const override
    {
        return parser_.line();
    }

    std::size_t column() const override
    {
        return parser_.column();
    }

    friend
    staj_filter_view operator|(basic_msgpack_cursor& cursor, 
                               std::function<bool(const staj_event&, const ser_context&)> pred)
    {
        return staj_filter_view(cursor, pred);
    }
private:
    static bool accept_all(const staj_event&, const ser_context&) 
    {
        return true;
    }

    void read_next(std::error_code& ec)
    {
        if (cursor_visitor_.in_available())
        {
            cursor_visitor_.send_available(ec);
        }
        else
        {
            parser_.restart();
            while (!parser_.stopped())
            {
                parser_.parse(cursor_handler_adaptor_, ec);
                if (ec) {return;}
            }
        }
    }

    void read_next(basic_json_visitor<char_type>& visitor, std::error_code& ec)
    {
        {
            struct resource_wrapper
            {
                basic_item_event_visitor_to_json_visitor<char_type,Allocator>& adaptor;
                basic_json_visitor<char_type>& original;

                resource_wrapper(basic_item_event_visitor_to_json_visitor<char_type,Allocator>& adaptor,
                                 basic_json_visitor<char_type>& visitor)
                    : adaptor(adaptor), original(adaptor.destination())
                {
                    adaptor.destination(visitor);
                }

                ~resource_wrapper()
                {
                    adaptor.destination(original);
                }
            } wrapper(cursor_handler_adaptor_, visitor);

            parser_.restart();
            while (!parser_.stopped())
            {
                parser_.parse(cursor_handler_adaptor_, ec);
                if (ec) {return;}
            }
        }
    }
};

using msgpack_stream_cursor = basic_msgpack_cursor<jsoncons::binary_stream_source>;
using msgpack_bytes_cursor = basic_msgpack_cursor<jsoncons::bytes_source>;

} // namespace msgpack
} // namespace jsoncons

#endif // JSONCONS_EXT_MSGPACK_MSGPACK_CURSOR_HPP

