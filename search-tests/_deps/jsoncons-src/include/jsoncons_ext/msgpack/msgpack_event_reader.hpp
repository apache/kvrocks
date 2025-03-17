// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_MSGPACK_MSGPACK_EVENT_READER_HPP
#define JSONCONS_EXT_MSGPACK_MSGPACK_EVENT_READER_HPP

#include <cstddef>
#include <functional>
#include <memory> // std::allocator
#include <system_error>

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/item_event_visitor.hpp>
#include <jsoncons/json_exception.hpp>
#include <jsoncons/ser_context.hpp>
#include <jsoncons/source.hpp>
#include <jsoncons/staj_event.hpp>

#include <jsoncons/staj_event_reader.hpp>
#include <jsoncons_ext/msgpack/msgpack_parser.hpp>
#include <jsoncons_ext/msgpack/msgpack_options.hpp>

namespace jsoncons { 
namespace msgpack {

    template <typename Source=jsoncons::binary_stream_source,typename Allocator=std::allocator<char>>
    class msgpack_event_reader : public basic_staj_event_reader<char>, private virtual ser_context
    {
    public:
        using source_type = Source;
        using char_type = char;
        using allocator_type = Allocator;
    private:
        basic_msgpack_parser<Source,Allocator> parser_;
        basic_item_event_receiver<char_type> event_receiver_;
        bool eof_;

    public:
        using string_view_type = string_view;

        template <typename Sourceable>
        msgpack_event_reader(Sourceable&& source,
                             const msgpack_decode_options& options = msgpack_decode_options(),
                             const Allocator& alloc = Allocator())
            : parser_(std::forward<Sourceable>(source), options, alloc), 
              event_receiver_(accept_all),
              eof_(false)
        {
            if (!done())
            {
                next();
            }
        }

        // Noncopyable and nonmoveable
        msgpack_event_reader(const msgpack_event_reader&) = delete;
        msgpack_event_reader(msgpack_event_reader&&) = delete;

        // Constructors that set parse error codes

        template <typename Sourceable>
        msgpack_event_reader(Sourceable&& source,
                             std::error_code& ec)
           : msgpack_event_reader(std::allocator_arg, Allocator(),
                                  std::forward<Sourceable>(source), 
                                  msgpack_decode_options(), 
                                  ec)
        {
        }

        template <typename Sourceable>
        msgpack_event_reader(Sourceable&& source,
                             const msgpack_decode_options& options,
                             std::error_code& ec)
           : msgpack_event_reader(std::allocator_arg, Allocator(),
                                  std::forward<Sourceable>(source), 
                                  options, 
                                  ec)
        {
        }

        template <typename Sourceable>
        msgpack_event_reader(std::allocator_arg_t, const Allocator& alloc, 
                             Sourceable&& source,
                             const msgpack_decode_options& options,
                             std::error_code& ec)
           : parser_(std::forward<Sourceable>(source), options, alloc), 
             event_receiver_(accept_all),
             eof_(false)
        {
            if (!done())
            {
                next(ec);
            }
        }
        
        ~msgpack_event_reader() = default;

        msgpack_event_reader& operator=(const msgpack_event_reader&) = delete;
        msgpack_event_reader& operator=(msgpack_event_reader&&) = delete;

        void reset()
        {
            parser_.reset();
            event_receiver_.reset();
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
            event_receiver_.reset();
            eof_ = false;
            if (!done())
            {
                next();
            }
        }

        void reset(std::error_code& ec)
        {
            parser_.reset();
            event_receiver_.reset();
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
            event_receiver_.reset();
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

        const basic_staj_event<char_type>& current() const override
        {
            return event_receiver_.event();
        }

        void read_to(basic_item_event_visitor<char_type>& visitor) override
        {
            std::error_code ec;
            read_to(visitor, ec);
            if (ec)
            {
                JSONCONS_THROW(ser_error(ec,parser_.line(),parser_.column()));
            }
        }

        void read_to(basic_item_event_visitor<char_type>& visitor,
                    std::error_code& ec) override
        {
            if (event_receiver_.dump(visitor, *this, ec))
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
        staj2_filter_view operator|(msgpack_event_reader& cursor, 
                                   std::function<bool(const item_event&, const ser_context&)> pred)
        {
            return staj2_filter_view(cursor, pred);
        }

    private:
        static bool accept_all(const item_event&, const ser_context&) 
        {
            return true;
        }

        void read_next(std::error_code& ec)
        {
            if (event_receiver_.in_available())
            {
                event_receiver_.send_available(ec);
            }
            else
            {
                parser_.restart();
                while (!parser_.stopped())
                {
                    parser_.parse(event_receiver_, ec);
                    if (ec) {return;}
                }
            }
        }

        void read_next(basic_item_event_visitor<char_type>& visitor, std::error_code& ec)
        {
            {
                parser_.restart();
                while (!parser_.stopped())
                {
                    parser_.parse(visitor, ec);
                    if (ec) {return;}
                }
            }
        }
    };

} // namespace msgpack
} // namespace jsoncons

#endif // JSONCONS_EXT_MSGPACK_MSGPACK_EVENT_READER_HPP

