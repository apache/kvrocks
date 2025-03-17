// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSON_CURSOR_HPP
#define JSONCONS_JSON_CURSOR_HPP

#include <cstddef>
#include <functional>
#include <ios>
#include <memory> // std::allocator
#include <system_error>

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/utility/byte_string.hpp>
#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/conv_error.hpp>
#include <jsoncons/json_exception.hpp>
#include <jsoncons/json_parser.hpp>
#include <jsoncons/json_visitor.hpp>
#include <jsoncons/ser_context.hpp>
#include <jsoncons/source.hpp>
#include <jsoncons/source_adaptor.hpp>
#include <jsoncons/staj_cursor.hpp>
#include <jsoncons/utility/unicode_traits.hpp>

namespace jsoncons {

template <typename CharT,typename Source=jsoncons::stream_source<CharT>,typename Allocator=std::allocator<char>>
class basic_json_cursor : public basic_staj_cursor<CharT>, private virtual ser_context
{
public:
    using source_type = Source;
    using char_type = CharT;
    using allocator_type = Allocator;
    using string_view_type = jsoncons::basic_string_view<CharT>;
private:
    using char_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<CharT>;
    static constexpr size_t default_max_buffer_size = 16384;

    json_source_adaptor<Source> source_;
    basic_json_parser<CharT,Allocator> parser_;
    basic_staj_visitor<CharT> cursor_visitor_;
    bool done_;

public:

    // Constructors that throw parse exceptions

    template <typename Sourceable>
    basic_json_cursor(Sourceable&& source, 
                      const basic_json_decode_options<CharT>& options = basic_json_decode_options<CharT>(),
                      std::function<bool(json_errc,const ser_context&)> err_handler = default_json_parsing(),
                      const Allocator& alloc = Allocator(),
                      typename std::enable_if<!std::is_constructible<jsoncons::basic_string_view<CharT>,Sourceable>::value>::type* = 0)
       : source_(std::forward<Sourceable>(source)),
         parser_(options,err_handler,alloc),
         cursor_visitor_(accept_all),
         done_(false)
    {
        if (!done())
        {
            std::error_code local_ec;
            read_next(local_ec);
            if (local_ec)
            {
                if (local_ec == json_errc::unexpected_eof)
                {
                    done_ = true;
                }
                else
                {
                    JSONCONS_THROW(ser_error(local_ec, 1, 1));
                }
            }
        }
    }

    template <typename Sourceable>
    basic_json_cursor(Sourceable&& source, 
                      const basic_json_decode_options<CharT>& options = basic_json_decode_options<CharT>(),
                      std::function<bool(json_errc,const ser_context&)> err_handler = default_json_parsing(),
                      const Allocator& alloc = Allocator(),
                      typename std::enable_if<std::is_constructible<jsoncons::basic_string_view<CharT>,Sourceable>::value>::type* = 0)
       : source_(),
         parser_(options, err_handler, alloc),
         cursor_visitor_(accept_all),
         done_(false)
    {
        initialize_with_string_view(std::forward<Sourceable>(source));
    }


    // Constructors that set parse error codes
    template <typename Sourceable>
    basic_json_cursor(Sourceable&& source, 
                      std::error_code& ec)
        : basic_json_cursor(std::allocator_arg, Allocator(), 
                            std::forward<Sourceable>(source),
                            basic_json_decode_options<CharT>(),
                            default_json_parsing(),
                            ec)
    {
    }

    template <typename Sourceable>
    basic_json_cursor(Sourceable&& source, 
                      const basic_json_decode_options<CharT>& options,
                      std::error_code& ec)
        : basic_json_cursor(std::allocator_arg, Allocator(), 
                            std::forward<Sourceable>(source),
                            options,
                            default_json_parsing(),
                            ec)
    {
    }

    template <typename Sourceable>
    basic_json_cursor(Sourceable&& source, 
                      const basic_json_decode_options<CharT>& options,
                      std::function<bool(json_errc,const ser_context&)> err_handler,
                      std::error_code& ec)
        : basic_json_cursor(std::allocator_arg, Allocator(), 
                            std::forward<Sourceable>(source),
                            options,
                            err_handler,
                            ec)
    {
    }

    template <typename Sourceable>
    basic_json_cursor(std::allocator_arg_t, const Allocator& alloc,
                      Sourceable&& source, 
                      const basic_json_decode_options<CharT>& options,
                      std::function<bool(json_errc,const ser_context&)> err_handler,
                      std::error_code& ec,
                      typename std::enable_if<!std::is_constructible<jsoncons::basic_string_view<CharT>,Sourceable>::value>::type* = 0)
       : source_(std::forward<Sourceable>(source)),
         parser_(options,err_handler,alloc),
         cursor_visitor_(accept_all),
         done_(false)
    {
        if (!done())
        {
            std::error_code local_ec;
            read_next(local_ec);
            if (local_ec)
            {
                if (local_ec == json_errc::unexpected_eof)
                {
                    done_ = true;
                }
                else
                {
                    ec = local_ec;
                }
            }
        }
    }

    template <typename Sourceable>
    basic_json_cursor(std::allocator_arg_t, const Allocator& alloc,
                      Sourceable&& source, 
                      const basic_json_decode_options<CharT>& options,
                      std::function<bool(json_errc,const ser_context&)> err_handler,
                      std::error_code& ec,
                      typename std::enable_if<std::is_constructible<jsoncons::basic_string_view<CharT>,Sourceable>::value>::type* = 0)
       : source_(),
         parser_(options, err_handler, alloc),
         cursor_visitor_(accept_all),
         done_(false)
    {
        initialize_with_string_view(std::forward<Sourceable>(source), ec);
    }
    
    basic_json_cursor(const basic_json_cursor&) = delete;
    basic_json_cursor(basic_json_cursor&&) = default;
    
    ~basic_json_cursor() = default;

    // Noncopyable and nonmoveable

    basic_json_cursor& operator=(const basic_json_cursor&) = delete;
    basic_json_cursor& operator=(basic_json_cursor&&) = default;

    void reset()
    {
        parser_.reset();
        cursor_visitor_.reset();
        done_ = false;
        if (!done())
        {
            next();
        }
    }

    template <typename Sourceable>
    typename std::enable_if<!std::is_constructible<jsoncons::basic_string_view<CharT>,Sourceable>::value>::type
    reset(Sourceable&& source)
    {
        source_ = std::forward<Sourceable>(source);
        parser_.reinitialize();
        cursor_visitor_.reset();
        done_ = false;
        if (!done())
        {
            next();
        }
    }

    template <typename Sourceable>
    typename std::enable_if<std::is_constructible<jsoncons::basic_string_view<CharT>,Sourceable>::value>::type
    reset(Sourceable&& source)
    {
        source_ = {};
        parser_.reinitialize();
        cursor_visitor_.reset();
        done_ = false;
        initialize_with_string_view(std::forward<Sourceable>(source));
    }

    void reset(std::error_code& ec)
    {
        parser_.reset();
        cursor_visitor_.reset();
        done_ = false;
        if (!done())
        {
            next(ec);
        }
    }

    template <typename Sourceable>
    typename std::enable_if<!std::is_constructible<jsoncons::basic_string_view<CharT>,Sourceable>::value>::type
    reset(Sourceable&& source, std::error_code& ec)
    {
        source_ = std::forward<Sourceable>(source);
        parser_.reinitialize();
        cursor_visitor_.reset();
        done_ = false;
        if (!done())
        {
            next(ec);
        }
    }

    template <typename Sourceable>
    typename std::enable_if<std::is_constructible<jsoncons::basic_string_view<CharT>,Sourceable>::value>::type
    reset(Sourceable&& source, std::error_code& ec)
    {
        source_ = {};
        parser_.reinitialize();
        done_ = false;
        initialize_with_string_view(std::forward<Sourceable>(source), ec);
    }

    bool done() const override
    {
        return parser_.done() || done_;
    }

    const basic_staj_event<CharT>& current() const override
    {
        return cursor_visitor_.event();
    }

    void read_to(basic_json_visitor<CharT>& visitor) override
    {
        std::error_code ec;
        read_to(visitor, ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec,parser_.line(),parser_.column()));
        }
    }

    void read_to(basic_json_visitor<CharT>& visitor,
                 std::error_code& ec) override
    {
        if (cursor_visitor_.event().send_json_event(visitor, *this, ec))
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

    void check_done()
    {
        std::error_code ec;
        check_done(ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec,parser_.line(),parser_.column()));
        }
    }

    const ser_context& context() const override
    {
        return *this;
    }

    void check_done(std::error_code& ec)
    {
        if (source_.is_error())
        {
            ec = json_errc::source_error;
            return;
        }   
        if (source_.eof())
        {
            parser_.check_done(ec);
            if (ec) {return;}
        }
        else
        {
            do
            {
                if (parser_.source_exhausted())
                {
                    auto s = source_.read_buffer(ec);
                    if (ec) {return;}
                    if (s.size() > 0)
                    {
                        parser_.update(s.data(),s.size());
                    }
                }
                if (!parser_.source_exhausted())
                {
                    parser_.check_done(ec);
                    if (ec) {return;}
                }
            }
            while (!eof());
        }
    }

    bool eof() const
    {
        return parser_.source_exhausted() && source_.eof();
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
    basic_staj_filter_view<CharT> operator|(basic_json_cursor& cursor, 
                                      std::function<bool(const basic_staj_event<CharT>&, const ser_context&)> pred)
    {
        return basic_staj_filter_view<CharT>(cursor, pred);
    }

private:

    static bool accept_all(const basic_staj_event<CharT>&, const ser_context&) 
    {
        return true;
    }

    void initialize_with_string_view(string_view_type sv)
    {
        std::error_code local_ec;
        initialize_with_string_view(sv, local_ec);
        if (local_ec)
        {
            JSONCONS_THROW(ser_error(local_ec, 1, 1));
        }
    }

    void initialize_with_string_view(string_view_type sv, std::error_code& ec)
    {
        auto r = unicode_traits::detect_json_encoding(sv.data(), sv.size());
        if (!(r.encoding == unicode_traits::encoding_kind::utf8 || r.encoding == unicode_traits::encoding_kind::undetected))
        {
            ec = json_errc::illegal_unicode_character;
            return;
        }
        std::size_t offset = (r.ptr - sv.data());
        parser_.update(sv.data()+offset,sv.size()-offset);
        bool is_done = parser_.done() || done_;
        if (!is_done)
        {
            std::error_code local_ec;
            read_next(local_ec);
            if (local_ec)
            {
                if (local_ec == json_errc::unexpected_eof)
                {
                    done_ = true;
                }
                else
                {
                    ec = local_ec;
                }
            }
        }
    }

    void read_next()
    {
        std::error_code ec;
        read_next(cursor_visitor_, ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec,parser_.line(),parser_.column()));
        }
    }

    void read_next(std::error_code& ec)
    {
        read_next(cursor_visitor_, ec);
    }

    void read_next(basic_json_visitor<CharT>& visitor, std::error_code& ec)
    {
        parser_.restart();
        while (!parser_.stopped())
        {
            if (parser_.source_exhausted())
            {
                auto s = source_.read_buffer(ec);
                if (ec) {return;}
                if (s.size() > 0)
                {
                    parser_.update(s.data(),s.size());
                    if (ec) {return;}
                }
            }
            bool eof = parser_.source_exhausted() && source_.eof();
            parser_.parse_some(visitor, ec);
            if (ec) {return;}
            if (eof)
            {
                if (parser_.enter())
                {
                    done_ = true;
                    break;
                }
                else if (!parser_.accept())
                {
                    ec = json_errc::unexpected_eof;
                    return;
                }
            }
        }
    }
};

using json_stream_cursor = basic_json_cursor<char,jsoncons::stream_source<char>>;
using json_string_cursor = basic_json_cursor<char,jsoncons::string_source<char>>;
using wjson_stream_cursor = basic_json_cursor<wchar_t,jsoncons::stream_source<wchar_t>>;
using wjson_string_cursor = basic_json_cursor<wchar_t,jsoncons::string_source<wchar_t>>;

} // namespace jsoncons

#endif // JSONCONS_JSON_CURSOR_HPP

