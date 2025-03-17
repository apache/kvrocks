// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_UBJSON_UBJSON_PARSER_HPP
#define JSONCONS_EXT_UBJSON_UBJSON_PARSER_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <system_error>
#include <vector>
#include <utility> // std::move

#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/detail/parse_number.hpp>
#include <jsoncons/json_visitor.hpp>
#include <jsoncons/ser_context.hpp>
#include <jsoncons/source.hpp>
#include <jsoncons/tag_type.hpp>
#include <jsoncons/utility/binary.hpp>
#include <jsoncons/utility/unicode_traits.hpp>

#include <jsoncons_ext/ubjson/ubjson_error.hpp>
#include <jsoncons_ext/ubjson/ubjson_options.hpp>
#include <jsoncons_ext/ubjson/ubjson_type.hpp>

namespace jsoncons { namespace ubjson {

enum class parse_mode {root,accept,array,indefinite_array,strongly_typed_array,map_key,map_value,strongly_typed_map_key,strongly_typed_map_value,indefinite_map_key,indefinite_map_value};

struct parse_state 
{
    parse_mode mode; 
    std::size_t length{0};
    uint8_t type;
    std::size_t index{0};

    parse_state(parse_mode mode, std::size_t length, uint8_t type = 0) noexcept
        : mode(mode), length(length), type(type)
    {
    }

    parse_state(const parse_state&) = default;
    parse_state(parse_state&&) = default;
};

template <typename Source,typename Allocator=std::allocator<char>>
class basic_ubjson_parser : public ser_context
{
    using char_type = char;
    using char_traits_type = std::char_traits<char>;
    using temp_allocator_type = Allocator;
    using char_allocator_type = typename std::allocator_traits<temp_allocator_type>:: template rebind_alloc<char_type>;                  
    using byte_allocator_type = typename std::allocator_traits<temp_allocator_type>:: template rebind_alloc<uint8_t>;                  
    using parse_state_allocator_type = typename std::allocator_traits<temp_allocator_type>:: template rebind_alloc<parse_state>;                         

    Source source_;
    ubjson_decode_options options_;
    bool more_;
    bool done_;
    std::basic_string<char,std::char_traits<char>,char_allocator_type> text_buffer_;
    std::vector<parse_state,parse_state_allocator_type> state_stack_;
    int nesting_depth_;
public:
    template <typename Sourceable>
        basic_ubjson_parser(Sourceable&& source,
                          const ubjson_decode_options& options = ubjson_decode_options(),
                          const Allocator& alloc = Allocator())
       : source_(std::forward<Sourceable>(source)), 
         options_(options),
         more_(true), 
         done_(false),
         text_buffer_(alloc),
         state_stack_(alloc),
         nesting_depth_(0)
    {
        state_stack_.emplace_back(parse_mode::root,0);
    }

    void restart()
    {
        more_ = true;
    }

    void reset()
    {
        more_ = true;
        done_ = false;
        text_buffer_.clear();
        state_stack_.clear();
        state_stack_.emplace_back(parse_mode::root,0,uint8_t(0));
        nesting_depth_ = 0;
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

    void parse(json_visitor& visitor, std::error_code& ec)
    {
        while (!done_ && more_)
        {
            switch (state_stack_.back().mode)
            {
                case parse_mode::array:
                {
                    if (state_stack_.back().index < state_stack_.back().length)
                    {
                        ++state_stack_.back().index;
                        read_type_and_value(visitor, ec);
                        if (ec)
                        {
                            return;
                        }
                    }
                    else
                    {
                        end_array(visitor, ec);
                    }
                    break;
                }
                case parse_mode::strongly_typed_array:
                {
                    if (state_stack_.back().index < state_stack_.back().length)
                    {
                        ++state_stack_.back().index;
                        read_value(visitor, state_stack_.back().type, ec);
                        if (ec)
                        {
                            return;
                        }
                    }
                    else
                    {
                        end_array(visitor, ec);
                    }
                    break;
                }
                case parse_mode::indefinite_array:
                {
                    auto c = source_.peek();
                    if (c.eof)
                    {
                        ec = ubjson_errc::unexpected_eof;
                        more_ = false;
                        return;
                    }
                    if (c.value == jsoncons::ubjson::ubjson_type::end_array_marker)
                    {
                        source_.ignore(1);
                        end_array(visitor, ec);
                        if (ec)
                        {
                            return;
                        }
                    }
                    else
                    {
                        if (++state_stack_.back().index > options_.max_items())
                        {
                            ec = ubjson_errc::max_items_exceeded;
                            more_ = false;
                            return;
                        }
                        read_type_and_value(visitor, ec);
                        if (ec)
                        {
                            return;
                        }
                    }
                    break;
                }
                case parse_mode::map_key:
                {
                    if (state_stack_.back().index < state_stack_.back().length)
                    {
                        ++state_stack_.back().index;
                        read_key(visitor, ec);
                        if (ec)
                        {
                            return;
                        }
                        state_stack_.back().mode = parse_mode::map_value;
                    }
                    else
                    {
                        end_object(visitor, ec);
                    }
                    break;
                }
                case parse_mode::map_value:
                {
                    state_stack_.back().mode = parse_mode::map_key;
                    read_type_and_value(visitor, ec);
                    if (ec)
                    {
                        return;
                    }
                    break;
                }
                case parse_mode::strongly_typed_map_key:
                {
                    if (state_stack_.back().index < state_stack_.back().length)
                    {
                        ++state_stack_.back().index;
                        read_key(visitor, ec);
                        if (ec)
                        {
                            return;
                        }
                        state_stack_.back().mode = parse_mode::strongly_typed_map_value;
                    }
                    else
                    {
                        end_object(visitor, ec);
                    }
                    break;
                }
                case parse_mode::strongly_typed_map_value:
                {
                    state_stack_.back().mode = parse_mode::strongly_typed_map_key;
                    read_value(visitor, state_stack_.back().type, ec);
                    if (ec)
                    {
                        return;
                    }
                    break;
                }
                case parse_mode::indefinite_map_key:
                {
                    auto c = source_.peek();
                    if (c.eof)
                    {
                        ec = ubjson_errc::unexpected_eof;
                        more_ = false;
                        return;
                    }
                    if (c.value == jsoncons::ubjson::ubjson_type::end_object_marker)
                    {
                        source_.ignore(1);
                        end_object(visitor, ec);
                        if (ec)
                        {
                            return;
                        }
                    }
                    else
                    {
                        if (++state_stack_.back().index > options_.max_items())
                        {
                            ec = ubjson_errc::max_items_exceeded;
                            more_ = false;
                            return;
                        }
                        read_key(visitor, ec);
                        if (ec)
                        {
                            return;
                        }
                        state_stack_.back().mode = parse_mode::indefinite_map_value;
                    }
                    break;
                }
                case parse_mode::indefinite_map_value:
                {
                    state_stack_.back().mode = parse_mode::indefinite_map_key;
                    read_type_and_value(visitor, ec);
                    if (ec)
                    {
                        return;
                    }
                    break;
                }
                case parse_mode::root:
                {
                    state_stack_.back().mode = parse_mode::accept;
                    read_type_and_value(visitor, ec);
                    if (ec)
                    {
                        return;
                    }
                    break;
                }
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
    void read_type_and_value(json_visitor& visitor, std::error_code& ec)
    {
        if (source_.is_error())
        {
            ec = ubjson_errc::source_error;
            more_ = false;
            return;
        }   

        uint8_t b;
        if (source_.read(&b, 1) == 0)
        {
            ec = ubjson_errc::unexpected_eof;
            more_ = false;
            return;
        }
        read_value(visitor, b, ec);
    }

    void read_value(json_visitor& visitor, uint8_t type, std::error_code& ec)
    {
        switch (type)
        {
            case jsoncons::ubjson::ubjson_type::null_type: 
            {
                more_ = visitor.null_value(semantic_tag::none, *this, ec);
                break;
            }
            case jsoncons::ubjson::ubjson_type::no_op_type: 
            {
                break;
            }
            case jsoncons::ubjson::ubjson_type::true_type:
            {
                more_ = visitor.bool_value(true, semantic_tag::none, *this, ec);
                break;
            }
            case jsoncons::ubjson::ubjson_type::false_type:
            {
                more_ = visitor.bool_value(false, semantic_tag::none, *this, ec);
                break;
            }
            case jsoncons::ubjson::ubjson_type::int8_type: 
            {
                uint8_t buf[sizeof(int8_t)];
                if (source_.read(buf, sizeof(int8_t)) != sizeof(int8_t))
                {
                    ec = ubjson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                int8_t val = binary::big_to_native<int8_t>(buf, sizeof(buf));
                more_ = visitor.int64_value(val, semantic_tag::none, *this, ec);
                break;
            }
            case jsoncons::ubjson::ubjson_type::uint8_type: 
            {
                uint8_t b;
                if (source_.read(&b, 1) == 0)
                {
                    ec = ubjson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                more_ = visitor.uint64_value(b, semantic_tag::none, *this, ec);
                break;
            }
            case jsoncons::ubjson::ubjson_type::int16_type: 
            {
                uint8_t buf[sizeof(int16_t)];
                if (source_.read(buf, sizeof(int16_t)) != sizeof(int16_t))
                {
                    ec = ubjson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                int16_t val = binary::big_to_native<int16_t>(buf, sizeof(buf));
                more_ = visitor.int64_value(val, semantic_tag::none, *this, ec);
                break;
            }
            case jsoncons::ubjson::ubjson_type::int32_type: 
            {
                uint8_t buf[sizeof(int32_t)];
                if (source_.read(buf, sizeof(int32_t)) != sizeof(int32_t))
                {
                    ec = ubjson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                int32_t val = binary::big_to_native<int32_t>(buf, sizeof(buf));
                more_ = visitor.int64_value(val, semantic_tag::none, *this, ec);
                break;
            }
            case jsoncons::ubjson::ubjson_type::int64_type: 
            {
                uint8_t buf[sizeof(int64_t)];
                if (source_.read(buf, sizeof(int64_t)) != sizeof(int64_t))
                {
                    ec = ubjson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                int64_t val = binary::big_to_native<int64_t>(buf, sizeof(buf));
                more_ = visitor.int64_value(val, semantic_tag::none, *this, ec);
                break;
            }
            case jsoncons::ubjson::ubjson_type::float32_type: 
            {
                uint8_t buf[sizeof(float)];
                if (source_.read(buf, sizeof(float)) != sizeof(float))
                {
                    ec = ubjson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                float val = binary::big_to_native<float>(buf, sizeof(buf));
                more_ = visitor.double_value(val, semantic_tag::none, *this, ec);
                break;
            }
            case jsoncons::ubjson::ubjson_type::float64_type: 
            {
                uint8_t buf[sizeof(double)];
                if (source_.read(buf, sizeof(double)) != sizeof(double))
                {
                    ec = ubjson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                double val = binary::big_to_native<double>(buf, sizeof(buf));
                more_ = visitor.double_value(val, semantic_tag::none, *this, ec);
                break;
            }
            case jsoncons::ubjson::ubjson_type::char_type: 
            {
                text_buffer_.clear();
                if (source_reader<Source>::read(source_,text_buffer_,1) != 1)
                {
                    ec = ubjson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                auto result = unicode_traits::validate(text_buffer_.data(),text_buffer_.size());
                if (result.ec != unicode_traits::conv_errc())
                {
                    ec = ubjson_errc::invalid_utf8_text_string;
                    more_ = false;
                    return;
                }
                more_ = visitor.string_value(text_buffer_, semantic_tag::none, *this, ec);
                break;
            }
            case jsoncons::ubjson::ubjson_type::string_type: 
            {
                std::size_t length = get_length(ec);
                if (ec)
                {
                    return;
                }
                text_buffer_.clear();
                if (source_reader<Source>::read(source_,text_buffer_,length) != length)
                {
                    ec = ubjson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                auto result = unicode_traits::validate(text_buffer_.data(),text_buffer_.size());
                if (result.ec != unicode_traits::conv_errc())
                {
                    ec = ubjson_errc::invalid_utf8_text_string;
                    more_ = false;
                    return;
                }
                more_ = visitor.string_value(jsoncons::basic_string_view<char>(text_buffer_.data(),text_buffer_.length()), semantic_tag::none, *this, ec);
                break;
            }
            case jsoncons::ubjson::ubjson_type::high_precision_number_type: 
            {
                std::size_t length = get_length(ec);
                if (ec)
                {
                    return;
                }
                text_buffer_.clear();
                if (source_reader<Source>::read(source_,text_buffer_,length) != length)
                {
                    ec = ubjson_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                if (jsoncons::detail::is_base10(text_buffer_.data(),text_buffer_.length()))
                {
                    more_ = visitor.string_value(jsoncons::basic_string_view<char>(text_buffer_.data(),text_buffer_.length()), semantic_tag::bigint, *this, ec);
                }
                else
                {
                    more_ = visitor.string_value(jsoncons::basic_string_view<char>(text_buffer_.data(),text_buffer_.length()), semantic_tag::bigdec, *this, ec);
                }
                break;
            }
            case jsoncons::ubjson::ubjson_type::start_array_marker: 
            {
                begin_array(visitor,ec);
                break;
            }
            case jsoncons::ubjson::ubjson_type::start_object_marker: 
            {
                begin_object(visitor, ec);
                break;
            }
            default:
            {
                ec = ubjson_errc::unknown_type;
                break;
            }
        }
        if (ec)
        {
            more_ = false;
        }
    }

    void begin_array(json_visitor& visitor, std::error_code& ec)
    {
        if (JSONCONS_UNLIKELY(++nesting_depth_ > options_.max_nesting_depth()))
        {
            ec = ubjson_errc::max_nesting_depth_exceeded;
            more_ = false;
            return;
        } 

        auto c = source_.peek();
        if (c.eof)
        {
            ec = ubjson_errc::unexpected_eof;
            more_ = false;
            return;
        }
        if (c.value == jsoncons::ubjson::ubjson_type::type_marker)
        {
            source_.ignore(1);
            uint8_t b;
            if (source_.read(&b, 1) == 0)
            {
                ec = ubjson_errc::unexpected_eof;
                more_ = false;
                return;
            }
            c = source_.peek();
            if (c.eof)
            {
                ec = ubjson_errc::unexpected_eof;
                more_ = false;
                return;
            }
            if (c.value == jsoncons::ubjson::ubjson_type::count_marker)
            {
                source_.ignore(1);
                std::size_t length = get_length(ec);
                if (ec)
                {
                    return;
                }
                if (length > options_.max_items())
                {
                    ec = ubjson_errc::max_items_exceeded;
                    more_ = false;
                    return;
                }
                state_stack_.emplace_back(parse_mode::strongly_typed_array,length,b);
                more_ = visitor.begin_array(length, semantic_tag::none, *this, ec);
            }
            else
            {
                ec = ubjson_errc::count_required_after_type;
                more_ = false;
                return;
            }
        }
        else if (c.value == jsoncons::ubjson::ubjson_type::count_marker)
        {
            source_.ignore(1);
            std::size_t length = get_length(ec);
            if (ec)
            {
                return;
            }
            if (length > options_.max_items())
            {
                ec = ubjson_errc::max_items_exceeded;
                more_ = false;
                return;
            }
            state_stack_.emplace_back(parse_mode::array,length);
            more_ = visitor.begin_array(length, semantic_tag::none, *this, ec);
        }
        else
        {
            state_stack_.emplace_back(parse_mode::indefinite_array,0);
            more_ = visitor.begin_array(semantic_tag::none, *this, ec);
        }
    }

    void end_array(json_visitor& visitor, std::error_code& ec)
    {
        --nesting_depth_;

        more_ = visitor.end_array(*this, ec);
        state_stack_.pop_back();
    }

    void begin_object(json_visitor& visitor, std::error_code& ec)
    {
        if (JSONCONS_UNLIKELY(++nesting_depth_ > options_.max_nesting_depth()))
        {
            ec = ubjson_errc::max_nesting_depth_exceeded;
            more_ = false;
            return;
        } 

        auto c = source_.peek();
        if (c.eof)
        {
            ec = ubjson_errc::unexpected_eof;
            more_ = false;
            return;
        }
        if (c.value == jsoncons::ubjson::ubjson_type::type_marker)
        {
            source_.ignore(1);
            uint8_t b;
            if (source_.read(&b, 1) == 0)
            {
                ec = ubjson_errc::unexpected_eof;
                more_ = false;
                return;
            }
            c = source_.peek();
            if (c.eof)
            {
                ec = ubjson_errc::unexpected_eof;
                more_ = false;
                return;
            }
            if (c.value == jsoncons::ubjson::ubjson_type::count_marker)
            {
                source_.ignore(1);
                std::size_t length = get_length(ec);
                if (ec)
                {
                    return;
                }
                if (length > options_.max_items())
                {
                    ec = ubjson_errc::max_items_exceeded;
                    more_ = false;
                    return;
                }
                state_stack_.emplace_back(parse_mode::strongly_typed_map_key,length,b);
                more_ = visitor.begin_object(length, semantic_tag::none, *this, ec);
            }
            else
            {
                ec = ubjson_errc::count_required_after_type;
                more_ = false;
                return;
            }
        }
        else
        {
            c = source_.peek();
            if (c.eof)
            {
                ec = ubjson_errc::unexpected_eof;
                more_ = false;
                return;
            }
            if (c.value == jsoncons::ubjson::ubjson_type::count_marker)
            {
                source_.ignore(1);
                std::size_t length = get_length(ec);
                if (ec)
                {
                    return;
                }
                if (length > options_.max_items())
                {
                    ec = ubjson_errc::max_items_exceeded;
                    more_ = false;
                    return;
                }
                state_stack_.emplace_back(parse_mode::map_key,length);
                more_ = visitor.begin_object(length, semantic_tag::none, *this, ec);
            }
            else
            {
                state_stack_.emplace_back(parse_mode::indefinite_map_key,0);
                more_ = visitor.begin_object(semantic_tag::none, *this, ec);
            }
        }
    }

    void end_object(json_visitor& visitor, std::error_code& ec)
    {
        --nesting_depth_;
        more_ = visitor.end_object(*this, ec);
        state_stack_.pop_back();
    }

    std::size_t get_length(std::error_code& ec)
    {
        std::size_t length = 0;
        uint8_t type;
        if (source_.read(&type, 1) == 0)
        {
            ec = ubjson_errc::unexpected_eof;
            more_ = false;
            return length;
        }
        switch (type)
        {
            case jsoncons::ubjson::ubjson_type::int8_type: 
            {
                uint8_t buf[sizeof(int8_t)];
                if (source_.read(buf, sizeof(int8_t)) != sizeof(int8_t))
                {
                    ec = ubjson_errc::unexpected_eof;
                    more_ = false;
                    return length;
                }
                int8_t val = binary::big_to_native<int8_t>(buf, sizeof(buf));
                if (val >= 0)
                {
                    length = val;
                }
                else
                {
                    ec = ubjson_errc::length_is_negative;
                    more_ = false;
                    return length;
                }
                break;
            }
            case jsoncons::ubjson::ubjson_type::uint8_type: 
            {
                uint8_t b;
                if (source_.read(&b, 1) == 0)
                {
                    ec = ubjson_errc::unexpected_eof;
                    more_ = false;
                    return length;
                }
                length = b;
                break;
            }
            case jsoncons::ubjson::ubjson_type::int16_type: 
            {
                uint8_t buf[sizeof(int16_t)];
                if (source_.read(buf, sizeof(int16_t)) != sizeof(int16_t))
                {
                    ec = ubjson_errc::unexpected_eof;
                    more_ = false;
                    return length;
                }
                int16_t val = binary::big_to_native<int16_t>(buf, sizeof(buf));
                if (val >= 0)
                {
                    length = val;
                }
                else
                {
                    ec = ubjson_errc::length_is_negative;
                    more_ = false;
                    return length;
                }
                break;
            }
            case jsoncons::ubjson::ubjson_type::int32_type: 
            {
                uint8_t buf[sizeof(int32_t)];
                if (source_.read(buf, sizeof(int32_t)) != sizeof(int32_t))
                {
                    ec = ubjson_errc::unexpected_eof;
                    more_ = false;
                    return length;
                }
                int32_t val = binary::big_to_native<int32_t>(buf, sizeof(buf));
                if (val >= 0)
                {
                    length = static_cast<std::size_t>(val);
                }
                else
                {
                    ec = ubjson_errc::length_is_negative;
                    more_ = false;
                    return length;
                }
                break;
            }
            case jsoncons::ubjson::ubjson_type::int64_type: 
            {
                uint8_t buf[sizeof(int64_t)];
                if (source_.read(buf, sizeof(int64_t)) != sizeof(int64_t))
                {
                    ec = ubjson_errc::unexpected_eof;
                    more_ = false;
                    return length;
                }
                int64_t val = binary::big_to_native<int64_t>(buf, sizeof(buf));
                if (val >= 0)
                {
                    length = (std::size_t)val;
                    if (length != (uint64_t)val)
                    {
                        ec = ubjson_errc::number_too_large;
                        more_ = false;
                        return length;
                    }
                }
                else
                {
                    ec = ubjson_errc::length_is_negative;
                    more_ = false;
                    return length;
                }
                break;
            }
            default:
            {
                ec = ubjson_errc::length_must_be_integer;
                more_ = false;
                return length;
            }
        }
        return length;
    }

    void read_key(json_visitor& visitor, std::error_code& ec)
    {
        std::size_t length = get_length(ec);
        if (ec)
        {
            ec = ubjson_errc::key_expected;
            more_ = false;
            return;
        }
        text_buffer_.clear();
        if (source_reader<Source>::read(source_,text_buffer_,length) != length)
        {
            ec = ubjson_errc::unexpected_eof;
            more_ = false;
            return;
        }

        auto result = unicode_traits::validate(text_buffer_.data(),text_buffer_.size());
        if (result.ec != unicode_traits::conv_errc())
        {
            ec = ubjson_errc::invalid_utf8_text_string;
            more_ = false;
            return;
        }
        more_ = visitor.key(jsoncons::basic_string_view<char>(text_buffer_.data(),text_buffer_.length()), *this, ec);
    }
};

} // namespace ubjson
} // namespace jsoncons

#endif
