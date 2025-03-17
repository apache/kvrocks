// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_BSON_BSON_PARSER_HPP
#define JSONCONS_EXT_BSON_BSON_PARSER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <system_error>
#include <utility> // std::move
#include <vector>

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/json_visitor.hpp>
#include <jsoncons/ser_context.hpp>
#include <jsoncons/source.hpp>
#include <jsoncons/tag_type.hpp>
#include <jsoncons/utility/binary.hpp>
#include <jsoncons/utility/unicode_traits.hpp>

#include <jsoncons_ext/bson/bson_decimal128.hpp>
#include <jsoncons_ext/bson/bson_error.hpp>
#include <jsoncons_ext/bson/bson_oid.hpp>
#include <jsoncons_ext/bson/bson_options.hpp>
#include <jsoncons_ext/bson/bson_type.hpp>

namespace jsoncons { namespace bson {

enum class parse_mode {root,accept,document,array,value};

struct parse_state 
{
    parse_mode mode; 
    std::size_t length{0};
    std::size_t pos{0};
    uint8_t type;
    std::size_t index{0};

    parse_state(parse_mode mode_, std::size_t length_, std::size_t pos_, uint8_t type_ = 0) noexcept
        : mode(mode_), length(length_), pos(pos_), type(type_)
    {
    }

    parse_state(const parse_state&) = default;
    parse_state(parse_state&&) = default;
    parse_state& operator=(const parse_state&) = default;
    parse_state& operator=(parse_state&&) = default;
};

template <typename Source,typename TempAllocator =std::allocator<char>>
class basic_bson_parser : public ser_context
{
    using char_type = char;
    using char_traits_type = std::char_traits<char>;
    using temp_allocator_type = TempAllocator;
    using char_allocator_type = typename std::allocator_traits<temp_allocator_type>:: template rebind_alloc<char_type>;                  
    using byte_allocator_type = typename std::allocator_traits<temp_allocator_type>:: template rebind_alloc<uint8_t>;                  
    using parse_state_allocator_type = typename std::allocator_traits<temp_allocator_type>:: template rebind_alloc<parse_state>;
    using string_type = std::basic_string<char,std::char_traits<char>,char_allocator_type>;

    Source source_;
    bson_decode_options options_;
    bool more_;
    bool done_;
    std::vector<uint8_t,byte_allocator_type> bytes_buffer_;
    string_type name_buffer_;
    string_type text_buffer_;
    std::vector<parse_state,parse_state_allocator_type> state_stack_;
public:
    template <typename Sourceable>
    basic_bson_parser(Sourceable&& source,
                      const bson_decode_options& options = bson_decode_options(),
                      const TempAllocator& temp_alloc = TempAllocator())
       : source_(std::forward<Sourceable>(source)), 
         options_(options),
         more_(true), 
         done_(false),
         bytes_buffer_(temp_alloc),
         name_buffer_(temp_alloc),
         text_buffer_(temp_alloc),
         state_stack_(temp_alloc)
    {
        state_stack_.emplace_back(parse_mode::root,0,0);
    }

    void restart()
    {
        more_ = true;
    }

    void reset()
    {
        more_ = true;
        done_ = false;
        bytes_buffer_.clear();
        name_buffer_.clear();
        text_buffer_.clear();
        state_stack_.clear();
        state_stack_.emplace_back(parse_mode::root,0,0);
    }

    template <typename Sourceable>
    void reset(Sourceable&& source)
    {
        source_ = std::forward<Sourceable>(source);
        reset();
    }

    bool done() const
    {
        return done_;
    }

    bool stopped() const
    {
        return !more_;
    }

    std::size_t line() const override
    {
        return 0;
    }

    std::size_t column() const override
    {
        return source_.position();
    }

    void array_expected(json_visitor& visitor, std::error_code& ec)
    {
        if (state_stack_.size() == 2 && state_stack_.back().mode == parse_mode::document)
        {
            state_stack_.back().mode = parse_mode::array;
            more_ = visitor.begin_array(semantic_tag::none, *this, ec);
        }
    }

    void parse(json_visitor& visitor, std::error_code& ec)
    {
        if (JSONCONS_UNLIKELY(source_.is_error()))
        {
            ec = bson_errc::source_error;
            more_ = false;
            return;
        }
        
        while (!done_ && more_)
        {
            switch (state_stack_.back().mode)
            {
                case parse_mode::root:
                    state_stack_.back().mode = parse_mode::accept;
                    begin_document(visitor, ec);
                    break;
                case parse_mode::document:
                {
                    uint8_t type;
                    std::size_t n = source_.read(&type, 1);
                    state_stack_.back().pos += n;
                    if (JSONCONS_UNLIKELY(n != 1))
                    {
                        ec = bson_errc::unexpected_eof;
                        more_ = false;
                        return;
                    }
                    if (type != 0x00)
                    {
                        read_e_name(visitor,jsoncons::bson::bson_container_type::document,ec);
                        state_stack_.back().mode = parse_mode::value;
                        state_stack_.back().type = type;
                    }
                    else
                    {
                        end_document(visitor,ec);
                    }
                    break;
                }
                case parse_mode::array:
                {
                    uint8_t type;
                    std::size_t n = source_.read(&type, 1);
                    state_stack_.back().pos += n;
                    if (JSONCONS_UNLIKELY(n != 1))
                    {
                        ec = bson_errc::unexpected_eof;
                        more_ = false;
                        return;
                    }
                    if (type != 0x00)
                    {
                        read_e_name(visitor,jsoncons::bson::bson_container_type::array,ec);
                        read_value(visitor, type, ec);
                    }
                    else
                    {
                        end_array(visitor,ec);
                    }
                    break;
                }
                case parse_mode::value:
                    state_stack_.back().mode = parse_mode::document;
                    read_value(visitor,state_stack_.back().type,ec);
                    break;
                case parse_mode::accept:
                {
                    JSONCONS_ASSERT(state_stack_.size() == 1);
                    state_stack_.clear();
                    more_ = false;
                    done_ = true;
                    visitor.flush();
                    break;
                }
            }
        }
    }

private:

    void begin_document(json_visitor& visitor, std::error_code& ec)
    {
        if (JSONCONS_UNLIKELY(static_cast<int>(state_stack_.size()) > options_.max_nesting_depth()))
        {
            ec = bson_errc::max_nesting_depth_exceeded;
            more_ = false;
            return;
        } 

        uint8_t buf[sizeof(int32_t)]; 
        std::size_t n = source_.read(buf, sizeof(int32_t));
        if (JSONCONS_UNLIKELY(n != sizeof(int32_t)))
        {
            ec = bson_errc::unexpected_eof;
            more_ = false;
            return;
        }

        auto length = binary::little_to_native<int32_t>(buf, sizeof(buf));

        more_ = visitor.begin_object(semantic_tag::none, *this, ec);
        state_stack_.emplace_back(parse_mode::document,length,n);
    }

    void end_document(json_visitor& visitor, std::error_code& ec)
    {
        JSONCONS_ASSERT(state_stack_.size() >= 2);

        more_ = visitor.end_object(*this,ec);
        if (JSONCONS_UNLIKELY(state_stack_.back().pos != state_stack_.back().length))
        {
            ec = bson_errc::size_mismatch;
            more_ = false;
            return;
        }
        std::size_t pos = state_stack_.back().pos;
        state_stack_.pop_back();
        state_stack_.back().pos += pos;
    }

    void begin_array(json_visitor& visitor, std::error_code& ec)
    {
        if (JSONCONS_UNLIKELY(static_cast<int>(state_stack_.size()) > options_.max_nesting_depth()))
        {
            ec = bson_errc::max_nesting_depth_exceeded;
            more_ = false;
            return;
        } 
        uint8_t buf[sizeof(int32_t)]; 
        std::size_t n = source_.read(buf, sizeof(int32_t));
        if (JSONCONS_UNLIKELY(n != sizeof(int32_t)))
        {
            ec = bson_errc::unexpected_eof;
            more_ = false;
            return;
        }
        auto length = binary::little_to_native<int32_t>(buf, sizeof(buf));

        more_ = visitor.begin_array(semantic_tag::none, *this, ec);
        if (ec)
        {
            return;
        }
        state_stack_.emplace_back(parse_mode::array, length, n);
    }

    void end_array(json_visitor& visitor, std::error_code& ec)
    {
        JSONCONS_ASSERT(state_stack_.size() >= 2);

        more_ = visitor.end_array(*this, ec);
        if (JSONCONS_UNLIKELY(state_stack_.back().pos != state_stack_.back().length))
        {
            ec = bson_errc::size_mismatch;
            more_ = false;
            return;
        }
        std::size_t pos = state_stack_.back().pos;
        state_stack_.pop_back();
        state_stack_.back().pos += pos;
    }

    void read_e_name(json_visitor& visitor, jsoncons::bson::bson_container_type type, std::error_code& ec)
    {
        name_buffer_.clear();
        read_cstring(name_buffer_, ec);
        if (ec)
        {
            return;
        }
        if (type == jsoncons::bson::bson_container_type::document)
        {
            auto result = unicode_traits::validate(name_buffer_.data(),name_buffer_.size());
            if (JSONCONS_UNLIKELY(result.ec != unicode_traits::conv_errc()))
            {
                ec = bson_errc::invalid_utf8_text_string;
                more_ = false;
                return;
            }
            more_ = visitor.key(jsoncons::basic_string_view<char>(name_buffer_.data(),name_buffer_.length()), *this, ec);
        }
    }

    void read_value(json_visitor& visitor, uint8_t type, std::error_code& ec)
    {
        switch (type)
        {
            case jsoncons::bson::bson_type::double_type:
            {
                uint8_t buf[sizeof(double)]; 
                std::size_t n = source_.read(buf, sizeof(double));
                state_stack_.back().pos += n;
                if (JSONCONS_UNLIKELY(n != sizeof(double)))
                {
                    ec = bson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                double res = binary::little_to_native<double>(buf, sizeof(buf));
                more_ = visitor.double_value(res, semantic_tag::none, *this, ec);
                break;
            }
            case jsoncons::bson::bson_type::symbol_type:
            case jsoncons::bson::bson_type::min_key_type:
            case jsoncons::bson::bson_type::max_key_type:
            case jsoncons::bson::bson_type::string_type:
            {
                text_buffer_.clear();
                read_string(text_buffer_, ec);
                if (ec)
                {
                    return;
                }
                auto result = unicode_traits::validate(text_buffer_.data(), text_buffer_.size());
                if (JSONCONS_UNLIKELY(result.ec != unicode_traits::conv_errc()))
                {
                    ec = bson_errc::invalid_utf8_text_string;
                    more_ = false;
                    return;
                }
                more_ = visitor.string_value(text_buffer_, semantic_tag::none, *this, ec);
                break;
            }
            case jsoncons::bson::bson_type::javascript_type:
            {
                text_buffer_.clear();
                read_string(text_buffer_, ec);
                if (ec)
                {
                    return;
                }
                auto result = unicode_traits::validate(text_buffer_.data(), text_buffer_.size());
                if (JSONCONS_UNLIKELY(result.ec != unicode_traits::conv_errc()))
                {
                    ec = bson_errc::invalid_utf8_text_string;
                    more_ = false;
                    return;
                }
                more_ = visitor.string_value(text_buffer_, semantic_tag::code, *this, ec);
                break;
            }
            case jsoncons::bson::bson_type::regex_type:
            {
                text_buffer_.clear();
                text_buffer_.push_back('/');
                read_cstring(text_buffer_, ec);
                if (ec)
                {
                    return;
                }
                text_buffer_.push_back('/');
                read_cstring(text_buffer_, ec);
                if (ec)
                {
                    return;
                }
                more_ = visitor.string_value(text_buffer_, semantic_tag::regex, *this, ec);
                break;
            }
            case jsoncons::bson::bson_type::document_type: 
            {
                begin_document(visitor,ec);
                break;
            }

            case jsoncons::bson::bson_type::array_type: 
            {
                begin_array(visitor,ec);
                break;
            }
            case jsoncons::bson::bson_type::undefined_type: 
                {
                    more_ = visitor.null_value(semantic_tag::undefined, *this, ec);
                    break;
                }
            case jsoncons::bson::bson_type::null_type: 
            {
                more_ = visitor.null_value(semantic_tag::none, *this, ec);
                break;
            }
            case jsoncons::bson::bson_type::bool_type:
            {
                uint8_t c;
                std::size_t n = source_.read(&c, 1);
                state_stack_.back().pos += n;
                if (JSONCONS_UNLIKELY(n != 1))
                {
                    ec = bson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                more_ = visitor.bool_value(c != 0, semantic_tag::none, *this, ec);
                break;
            }
            case jsoncons::bson::bson_type::int32_type: 
            {
                uint8_t buf[sizeof(int32_t)]; 
                std::size_t n = source_.read(buf, sizeof(int32_t));
                state_stack_.back().pos += n;
                if (JSONCONS_UNLIKELY(n != sizeof(int32_t)))
                {
                    ec = bson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                auto val = binary::little_to_native<int32_t>(buf, sizeof(buf));
                more_ = visitor.int64_value(val, semantic_tag::none, *this, ec);
                break;
            }

            case jsoncons::bson::bson_type::timestamp_type: 
            {
                uint8_t buf[sizeof(uint64_t)]; 
                std::size_t n = source_.read(buf, sizeof(uint64_t));
                state_stack_.back().pos += n;
                if (JSONCONS_UNLIKELY(n != sizeof(uint64_t)))
                {
                    ec = bson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                auto val = binary::little_to_native<uint64_t>(buf, sizeof(buf));
                more_ = visitor.uint64_value(val, semantic_tag::none, *this, ec);
                break;
            }

            case jsoncons::bson::bson_type::int64_type: 
            {
                uint8_t buf[sizeof(int64_t)]; 
                std::size_t n = source_.read(buf, sizeof(int64_t));
                state_stack_.back().pos += n;
                if (JSONCONS_UNLIKELY(n != sizeof(int64_t)))
                {
                    ec = bson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                auto val = binary::little_to_native<int64_t>(buf, sizeof(buf));
                more_ = visitor.int64_value(val, semantic_tag::none, *this, ec);
                break;
            }

            case jsoncons::bson::bson_type::datetime_type: 
            {
                uint8_t buf[sizeof(int64_t)]; 
                std::size_t n = source_.read(buf, sizeof(int64_t));
                state_stack_.back().pos += n;
                if (JSONCONS_UNLIKELY(n != sizeof(int64_t)))
                {
                    ec = bson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                auto val = binary::little_to_native<int64_t>(buf, sizeof(buf));
                more_ = visitor.int64_value(val, semantic_tag::epoch_milli, *this, ec);
                break;
            }
            case jsoncons::bson::bson_type::binary_type: 
            {
                uint8_t buf[sizeof(int32_t)]; 
                std::size_t n = source_.read(buf, sizeof(int32_t));
                state_stack_.back().pos += n;
                if (JSONCONS_UNLIKELY(n != sizeof(int32_t)))
                {
                    ec = bson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                const auto len = binary::little_to_native<int32_t>(buf, sizeof(buf));
                if (JSONCONS_UNLIKELY(len < 0))
                {
                    ec = bson_errc::length_is_negative;
                    more_ = false;
                    return;
                }
                uint8_t subtype;
                n = source_.read(&subtype, 1);
                state_stack_.back().pos += n;
                if (JSONCONS_UNLIKELY(n != 1))
                {
                    ec = bson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }

                bytes_buffer_.clear();
                n = source_reader<Source>::read(source_, bytes_buffer_, len);
                state_stack_.back().pos += n;
                if (JSONCONS_UNLIKELY(n != static_cast<std::size_t>(len)))
                {
                    ec = bson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }

                more_ = visitor.byte_string_value(bytes_buffer_, 
                                                  subtype, 
                                                  *this,
                                                  ec);
                break;
            }
            case jsoncons::bson::bson_type::decimal128_type: 
            {
                uint8_t buf[sizeof(uint64_t)*2]; 
                std::size_t n = source_.read(buf, sizeof(buf));
                state_stack_.back().pos += n;
                if (JSONCONS_UNLIKELY(n != sizeof(buf)))
                {
                    ec = bson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }

                decimal128_t dec;
                dec.low = binary::little_to_native<uint64_t>(buf, sizeof(uint64_t));
                dec.high = binary::little_to_native<uint64_t>(buf+sizeof(uint64_t), sizeof(uint64_t));

                text_buffer_.clear();
                text_buffer_.resize(bson::decimal128_limits::buf_size);
                auto r = bson::decimal128_to_chars(&text_buffer_[0], &text_buffer_[0]+text_buffer_.size(), dec);
                more_ = visitor.string_value(string_view(text_buffer_.data(),static_cast<std::size_t>(r.ptr-text_buffer_.data())), semantic_tag::float128, *this, ec);
                break;
            }
            case jsoncons::bson::bson_type::object_id_type: 
            {
                uint8_t buf[12]; 
                std::size_t n = source_.read(buf, sizeof(buf));
                state_stack_.back().pos += n;
                if (JSONCONS_UNLIKELY(n != sizeof(buf)))
                {
                    ec = bson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }

                oid_t oid(buf);
                to_string(oid, text_buffer_);

                more_ = visitor.string_value(text_buffer_, semantic_tag::id, *this, ec);
                break;
            }
            default:
            {
                ec = bson_errc::unknown_type;
                more_ = false;
                return;
            }
        }
    }

    void read_cstring(string_type& buffer, std::error_code& ec)
    {
        uint8_t c = 0xff;
        while (true)
        {
            std::size_t n = source_.read(&c, 1);
            state_stack_.back().pos += n;
            if (JSONCONS_UNLIKELY(n != 1))
            {
                ec = bson_errc::unexpected_eof;
                more_ = false;
                return;
            }
            if (c == 0)
            {
                break;
            }
            buffer.push_back(c);
        }
    }

    void read_string(string_type& buffer, std::error_code& ec)
    {
        uint8_t buf[sizeof(int32_t)]; 
        std::size_t n = source_.read(buf, sizeof(int32_t));
        state_stack_.back().pos += n;
        if (JSONCONS_UNLIKELY(n != sizeof(int32_t)))
        {
            ec = bson_errc::unexpected_eof;
            more_ = false;
            return;
        }
        auto len = binary::little_to_native<int32_t>(buf, sizeof(buf));
        if (JSONCONS_UNLIKELY(len < 1))
        {
            ec = bson_errc::string_length_is_non_positive;
            more_ = false;
            return;
        }

        std::size_t size = static_cast<std::size_t>(len) - static_cast<std::size_t>(1);
        n = source_reader<Source>::read(source_, buffer, size);
        state_stack_.back().pos += n;

        if (JSONCONS_UNLIKELY(n != size))
        {
            ec = bson_errc::unexpected_eof;
            more_ = false;
            return;
        }
        uint8_t c;
        n = source_.read(&c, 1);
        state_stack_.back().pos += n;
        if (JSONCONS_UNLIKELY(n != 1))
        {
            ec = bson_errc::unexpected_eof;
            more_ = false;
            return;
        }
    }
};

} // namespace bson
} // namespace jsoncons

#endif // JSONCONS_EXT_BSON_BSON_PARSER_HPP
