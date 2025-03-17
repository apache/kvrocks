// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_CBOR_CBOR_PARSER_HPP
#define JSONCONS_EXT_CBOR_CBOR_PARSER_HPP

#include <bitset> // std::bitset
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <system_error>
#include <utility> // std::move
#include <vector>

#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/item_event_visitor.hpp>
#include <jsoncons/json_visitor.hpp>
#include <jsoncons/ser_context.hpp>
#include <jsoncons/source.hpp>
#include <jsoncons/tag_type.hpp>
#include <jsoncons/utility/binary.hpp>
#include <jsoncons/utility/unicode_traits.hpp>

#include <jsoncons_ext/cbor/cbor_detail.hpp>
#include <jsoncons_ext/cbor/cbor_error.hpp>
#include <jsoncons_ext/cbor/cbor_options.hpp>

namespace jsoncons { namespace cbor {

enum class parse_mode {root,accept,array,indefinite_array,map_key,map_value,indefinite_map_key,indefinite_map_value,multi_dim};

struct parse_state 
{
    parse_mode mode; 
    std::size_t length;
    bool pop_stringref_map_stack;
    std::size_t index{0};

    parse_state(parse_mode mode, std::size_t length, bool pop_stringref_map_stack = false) noexcept
        : mode(mode), length(length), pop_stringref_map_stack(pop_stringref_map_stack)
    {
    }

    parse_state(const parse_state&) = default;
    parse_state(parse_state&&) = default;
    parse_state& operator=(const parse_state&) = default;
    parse_state& operator=(parse_state&&) = default;
    
    ~parse_state() = default;
};

template <typename Source,typename Allocator=std::allocator<char>>
class basic_cbor_parser : public ser_context
{
    using char_type = char;
    using char_traits_type = std::char_traits<char>;
    using allocator_type = Allocator;
    using char_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<char_type>;
    using byte_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<uint8_t>;                  
    using tag_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<uint64_t>;                 
    using parse_state_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<parse_state>;                         
    using string_type = std::basic_string<char_type,char_traits_type,char_allocator_type>;
    using byte_string_type = std::vector<uint8_t,byte_allocator_type>;

    struct mapped_string
    {
        using allocator_type = Allocator;

        jsoncons::cbor::detail::cbor_major_type type;
        string_type str;
        byte_string_type bytes;

        mapped_string(const string_type& str, const allocator_type& alloc = allocator_type())
            : type(jsoncons::cbor::detail::cbor_major_type::text_string), str(str.c_str(), str.size(), alloc), bytes(alloc)
        {
        }

        mapped_string(string_type&& str, const allocator_type& alloc = allocator_type())
            : type(jsoncons::cbor::detail::cbor_major_type::text_string), str(std::move(str), alloc), bytes(alloc)
        {
        }

        mapped_string(const byte_string_type& bytes, 
            const allocator_type& alloc = allocator_type())
            : type(jsoncons::cbor::detail::cbor_major_type::byte_string), str(alloc), bytes(bytes,alloc)
        {
        }

        mapped_string(byte_string_type&& bytes, 
            const allocator_type& alloc = allocator_type())
            : type(jsoncons::cbor::detail::cbor_major_type::byte_string), str(alloc), bytes(std::move(bytes),alloc)
        {
        }

        mapped_string(const mapped_string&) = default;
        mapped_string(mapped_string&&) = default;

        mapped_string& operator=(const mapped_string&) = default;
        mapped_string& operator=(mapped_string&&) = default;

        mapped_string(const mapped_string& other, const allocator_type& alloc) 
            :  type(other.type), str(other.str,alloc), bytes(other.bytes,alloc)
        {
        }

        mapped_string(mapped_string&& other, const allocator_type& alloc) noexcept
            : type(other.type), str(std::move(other.str), alloc), bytes(std::move(other.bytes), alloc)
        {
        }

        ~mapped_string() = default;
    };

    using mapped_string_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<mapped_string>;                           
    using stringref_map = std::vector<mapped_string, mapped_string_allocator_type>;
    using stringref_map_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<stringref_map>;                           

    enum {stringref_tag, // 25
          stringref_namespace_tag, // 256
          item_tag,
          num_of_tags};

    std::bitset<num_of_tags> other_tags_;

    allocator_type alloc_;
    Source source_;
    cbor_decode_options options_;

    bool more_{true};
    bool done_{false};
    string_type text_buffer_;
    byte_string_type bytes_buffer_;
    uint64_t raw_tag_;
    std::vector<parse_state,parse_state_allocator_type> state_stack_;
    byte_string_type typed_array_;
    std::vector<std::size_t> shape_;
    std::size_t index_; // TODO: Never used!
    std::vector<stringref_map,stringref_map_allocator_type> stringref_map_stack_;
    int nesting_depth_;

    struct read_byte_string_from_buffer
    {
        byte_string_view bytes;

        read_byte_string_from_buffer(const byte_string_view& b)
            : bytes(b)
        {
        }
        template <typename Container>
        void operator()(Container& c, std::error_code&)
        {
            c.clear();
            c.reserve(bytes.size());
            for (auto b : bytes)
            {
                c.push_back(b);
            }
        }
    };

    struct read_byte_string_from_source
    {
        basic_cbor_parser<Source,Allocator>* source;

        read_byte_string_from_source(basic_cbor_parser<Source,Allocator>* source)
            : source(source)
        {
        }
        template <typename Container>
        void operator()(Container& cont, std::error_code& ec)
        {
            source->read_byte_string(cont,ec);
        }
    };

public:
    template <typename Sourceable>
    basic_cbor_parser(Sourceable&& source,
                      const cbor_decode_options& options = cbor_decode_options(),
                      const Allocator& alloc = Allocator())
       : alloc_(alloc),
         source_(std::forward<Sourceable>(source)),
         options_(options),
         text_buffer_(alloc),
         bytes_buffer_(alloc),
         raw_tag_(0),
         state_stack_(alloc),
         typed_array_(alloc),
         index_(0),
         stringref_map_stack_(alloc),
         nesting_depth_(0)
    {
        state_stack_.emplace_back(parse_mode::root,0);
    }
    
    basic_cbor_parser(const basic_cbor_parser&) = delete;
    basic_cbor_parser(basic_cbor_parser&&) = delete;
    basic_cbor_parser& operator=(const basic_cbor_parser&) = delete;
    basic_cbor_parser& operator=(basic_cbor_parser&&) = delete;
    
    ~basic_cbor_parser() = default;

    void restart()
    {
        more_ = true;
    }

    void reset()
    {
        more_ = true;
        done_ = false;
        text_buffer_.clear();
        bytes_buffer_.clear();
        raw_tag_ = 0;
        state_stack_.clear();
        state_stack_.emplace_back(parse_mode::root,0);
        typed_array_.clear();
        stringref_map_stack_.clear();
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
    
    uint64_t raw_tag() const
    {
        return raw_tag_;
    }

    void parse(item_event_visitor& visitor, std::error_code& ec)
    {
        while (!done_ && more_)
        {
            switch (state_stack_.back().mode)
            {
                case parse_mode::multi_dim:
                {
                    if (state_stack_.back().index == 0)
                    {
                        ++state_stack_.back().index;
                        read_item(visitor, ec);
                    }
                    else
                    {
                        produce_end_multi_dim(visitor, ec);
                    }
                    break;
                }
                case parse_mode::array:
                {
                    if (state_stack_.back().index < state_stack_.back().length)
                    {
                        ++state_stack_.back().index;
                        read_item(visitor, ec);
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
                        ec = cbor_errc::unexpected_eof;
                        more_ = false;
                        return;
                    }
                    if (c.value == 0xff)
                    {
                        source_.ignore(1);
                        end_array(visitor, ec);
                    }
                    else
                    {
                        read_item(visitor, ec);
                    }
                    break;
                }
                case parse_mode::map_key:
                {
                    if (state_stack_.back().index < state_stack_.back().length)
                    {
                        ++state_stack_.back().index;
                        state_stack_.back().mode = parse_mode::map_value;
                        read_item(visitor, ec);
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
                    read_item(visitor, ec);
                    break;
                }
                case parse_mode::indefinite_map_key:
                {
                    auto c = source_.peek();
                    if (c.eof)
                    {
                        ec = cbor_errc::unexpected_eof;
                        more_ = false;
                        return;
                    }
                    if (c.value == 0xff)
                    {
                        source_.ignore(1);
                        end_object(visitor, ec);
                    }
                    else
                    {
                        state_stack_.back().mode = parse_mode::indefinite_map_value;
                        read_item(visitor, ec);
                    }
                    break;
                }
                case parse_mode::indefinite_map_value:
                {
                    state_stack_.back().mode = parse_mode::indefinite_map_key;
                    read_item(visitor, ec);
                    break;
                }
                case parse_mode::root:
                {
                    state_stack_.back().mode = parse_mode::accept;
                    read_item(visitor, ec);
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
    void read_item(item_event_visitor& visitor, std::error_code& ec)
    {
        read_tags(ec);
        if (!more_)
        {
            return;
        }
        auto c = source_.peek();
        if (c.eof)
        {
            ec = cbor_errc::unexpected_eof;
            more_ = false;
            return;
        }
        jsoncons::cbor::detail::cbor_major_type major_type = get_major_type(c.value);
        uint8_t info = get_additional_information_value(c.value);

        switch (major_type)
        {
            case jsoncons::cbor::detail::cbor_major_type::unsigned_integer:
            {
                uint64_t val = get_uint64_value(ec);
                if (ec)
                {
                    return;
                }
                if (!stringref_map_stack_.empty() && other_tags_[stringref_tag])
                {
                    other_tags_[stringref_tag] = false;
                    if (val >= stringref_map_stack_.back().size())
                    {
                        ec = cbor_errc::stringref_too_large;
                        more_ = false;
                        return;
                    }
                    auto index = static_cast<typename stringref_map::size_type>(val);
                    if (index != val)
                    {
                        ec = cbor_errc::number_too_large;
                        more_ = false;
                        return;
                    }
                    auto& str = stringref_map_stack_.back().at(index);
                    switch (str.type)
                    {
                        case jsoncons::cbor::detail::cbor_major_type::text_string:
                        {
                            handle_string(visitor, jsoncons::basic_string_view<char>(str.str.data(),str.str.length()),ec);
                            if (ec)
                            {
                                return;
                            }
                            break;
                        }
                        case jsoncons::cbor::detail::cbor_major_type::byte_string:
                        {
                            read_byte_string_from_buffer read(byte_string_view(str.bytes));
                            write_byte_string(read, visitor, ec);
                            if (ec)
                            {
                                return;
                            }
                            break;
                        }
                        default:
                            JSONCONS_UNREACHABLE();
                            break;
                    }
                }
                else
                {
                    semantic_tag tag = semantic_tag::none;
                    if (other_tags_[item_tag])
                    {
                        if (raw_tag_ == 1)
                        {
                            tag = semantic_tag::epoch_second;
                        }
                        other_tags_[item_tag] = false;
                    }
                    more_ = visitor.uint64_value(val, tag, *this, ec);
                }
                break;
            }
            case jsoncons::cbor::detail::cbor_major_type::negative_integer:
            {
                int64_t val = get_int64_value(ec);
                if (ec)
                {
                    return;
                }
                semantic_tag tag = semantic_tag::none;
                if (other_tags_[item_tag])
                {
                    if (raw_tag_ == 1)
                    {
                        tag = semantic_tag::epoch_second;
                    }
                    other_tags_[item_tag] = false;
                }
                more_ = visitor.int64_value(val, tag, *this, ec);
                break;
            }
            case jsoncons::cbor::detail::cbor_major_type::byte_string:
            {
                read_byte_string_from_source read(this);
                write_byte_string(read, visitor, ec);
                if (ec)
                {
                    return;
                }
                break;
            }
            case jsoncons::cbor::detail::cbor_major_type::text_string:
            {
                text_buffer_.clear();

                read_text_string(text_buffer_, ec);
                if (ec)
                {
                    return;
                }
                auto result = unicode_traits::validate(text_buffer_.data(),text_buffer_.size());
                if (result.ec != unicode_traits::conv_errc())
                {
                    ec = cbor_errc::invalid_utf8_text_string;
                    more_ = false;
                    return;
                }
                handle_string(visitor, jsoncons::basic_string_view<char>(text_buffer_.data(),text_buffer_.length()),ec);
                if (ec)
                {
                    return;
                }
                break;
            }
            case jsoncons::cbor::detail::cbor_major_type::semantic_tag:
            {
                JSONCONS_UNREACHABLE();
                break;
            }
            case jsoncons::cbor::detail::cbor_major_type::simple:
            {
                switch (info)
                {
                    case 0x14:
                        more_ = visitor.bool_value(false, semantic_tag::none, *this, ec);
                        source_.ignore(1);
                        break;
                    case 0x15:
                        more_ = visitor.bool_value(true, semantic_tag::none, *this, ec);
                        source_.ignore(1);
                        break;
                    case 0x16:
                        more_ = visitor.null_value(semantic_tag::none, *this, ec);
                        source_.ignore(1);
                        break;
                    case 0x17:
                        more_ = visitor.null_value(semantic_tag::undefined, *this, ec);
                        source_.ignore(1);
                        break;
                    case 0x19: // Half-Precision Float (two-byte IEEE 754)
                    {
                        uint64_t val = get_uint64_value(ec);
                        if (ec)
                        {
                            return;
                        }
                        more_ = visitor.half_value(static_cast<uint16_t>(val), semantic_tag::none, *this, ec);
                        break;
                    }
                    case 0x1a: // Single-Precision Float (four-byte IEEE 754)
                    case 0x1b: // Double-Precision Float (eight-byte IEEE 754)
                    {
                        double val = get_double(ec);
                        if (ec)
                        {
                            return;
                        }
                        semantic_tag tag = semantic_tag::none;
                        if (other_tags_[item_tag])
                        {
                            if (raw_tag_ == 1)
                            {
                                tag = semantic_tag::epoch_second;
                            }
                            other_tags_[item_tag] = false;
                        }
                        more_ = visitor.double_value(val, tag, *this, ec);
                        break;
                    }
                    default:
                    {
                        ec = cbor_errc::unknown_type;
                        more_ = false;
                        return;
                    }
                }
                break;
            }
            case jsoncons::cbor::detail::cbor_major_type::array:
            {
                if (other_tags_[item_tag])
                {
                    switch (raw_tag_)
                    {
                        case 0x04:
                            text_buffer_.clear();
                            read_decimal_fraction(text_buffer_, ec);
                            if (ec)
                            {
                                return;
                            }
                            more_ = visitor.string_value(text_buffer_, semantic_tag::bigdec, *this, ec);
                            break;
                        case 0x05:
                            text_buffer_.clear();
                            read_bigfloat(text_buffer_, ec);
                            if (ec)
                            {
                                return;
                            }
                            more_ = visitor.string_value(text_buffer_, semantic_tag::bigfloat, *this, ec);
                            break;
                        case 40: // row major storage
                            produce_begin_multi_dim(visitor, semantic_tag::multi_dim_row_major, ec);
                            break;
                        case 1040: // column major storage
                            produce_begin_multi_dim(visitor, semantic_tag::multi_dim_column_major, ec);
                            break;
                        default:
                            begin_array(visitor, info, ec);
                            break;
                    }
                    other_tags_[item_tag] = false;
                }
                else
                {
                    begin_array(visitor, info, ec);
                }
                break;
            }
            case jsoncons::cbor::detail::cbor_major_type::map:
            {
                begin_object(visitor, info, ec);
                break;
            }
            default:
                break;
        }
        other_tags_[item_tag] = false;
    }

    void begin_array(item_event_visitor& visitor, uint8_t info, std::error_code& ec)
    {
        if (JSONCONS_UNLIKELY(++nesting_depth_ > options_.max_nesting_depth()))
        {
            ec = cbor_errc::max_nesting_depth_exceeded;
            more_ = false;
            return;
        } 
        semantic_tag tag = semantic_tag::none;
        bool pop_stringref_map_stack = false;
        if (other_tags_[stringref_namespace_tag])
        {
            stringref_map_stack_.emplace_back();
            other_tags_[stringref_namespace_tag] = false;
            pop_stringref_map_stack = true;
        }
        switch (info)
        {
            case jsoncons::cbor::detail::additional_info::indefinite_length:
            {
                state_stack_.emplace_back(parse_mode::indefinite_array,0,pop_stringref_map_stack);
                more_ = visitor.begin_array(tag, *this, ec);
                source_.ignore(1);
                break;
            }
            default: // definite length
            {
                std::size_t len = get_size(ec);
                if (!more_)
                {
                    return;
                }
                state_stack_.emplace_back(parse_mode::array,len,pop_stringref_map_stack);
                more_ = visitor.begin_array(len, tag, *this, ec);
                break;
            }
        }
    }

    void end_array(item_event_visitor& visitor, std::error_code& ec)
    {
        --nesting_depth_;

        more_ = visitor.end_array(*this, ec);
        if (state_stack_.back().pop_stringref_map_stack)
        {
            stringref_map_stack_.pop_back();
        }
        state_stack_.pop_back();
    }

    void begin_object(item_event_visitor& visitor, uint8_t info, std::error_code& ec)
    {
        if (JSONCONS_UNLIKELY(++nesting_depth_ > options_.max_nesting_depth()))
        {
            ec = cbor_errc::max_nesting_depth_exceeded;
            more_ = false;
            return;
        } 
        bool pop_stringref_map_stack = false;
        if (other_tags_[stringref_namespace_tag])
        {
            stringref_map_stack_.emplace_back();
            other_tags_[stringref_namespace_tag] = false;
            pop_stringref_map_stack = true;
        }
        switch (info)
        {
            case jsoncons::cbor::detail::additional_info::indefinite_length: 
            {
                state_stack_.emplace_back(parse_mode::indefinite_map_key,0,pop_stringref_map_stack);
                more_ = visitor.begin_object(semantic_tag::none, *this, ec);
                source_.ignore(1);
                break;
            }
            default: // definite_length
            {
                std::size_t len = get_size(ec);
                if (!more_)
                {
                    return;
                }
                state_stack_.emplace_back(parse_mode::map_key,len,pop_stringref_map_stack);
                more_ = visitor.begin_object(len, semantic_tag::none, *this, ec);
                break;
            }
        }
    }

    void end_object(item_event_visitor& visitor, std::error_code& ec)
    {
        --nesting_depth_;
        more_ = visitor.end_object(*this, ec);
        if (state_stack_.back().pop_stringref_map_stack)
        {
            stringref_map_stack_.pop_back();
        }
        state_stack_.pop_back();
    }

    void read_text_string(string_type& str, std::error_code& ec)
    {
        auto c = source_.peek();
        if (c.eof)
        {
            ec = cbor_errc::unexpected_eof;
            more_ = false;
            return;
        }
        jsoncons::cbor::detail::cbor_major_type major_type = get_major_type(c.value);
        uint8_t info = get_additional_information_value(c.value);

        JSONCONS_ASSERT(major_type == jsoncons::cbor::detail::cbor_major_type::text_string);
        auto func = [&str](Source& source, std::size_t length, std::error_code& ec) -> bool
        {
            if (source_reader<Source>::read(source, str, length) != length)
            {
                ec = cbor_errc::unexpected_eof;
                return false;
            }
            return true;
        };
        iterate_string_chunks(func, major_type, ec);

        if (!stringref_map_stack_.empty() && 
            info != jsoncons::cbor::detail::additional_info::indefinite_length &&
            str.length() >= jsoncons::cbor::detail::min_length_for_stringref(stringref_map_stack_.back().size()))
        {
            stringref_map_stack_.back().emplace_back(mapped_string(str,alloc_));
        }

    }

    std::size_t get_size(std::error_code& ec)
    {
        uint64_t u = get_uint64_value(ec);
        if (!more_)
        {
            return 0;
        }
        std::size_t len = static_cast<std::size_t>(u);
        if (len != u)
        {
            ec = cbor_errc::number_too_large;
            more_ = false;
        }
        return len;
    }

    bool read_byte_string(byte_string_type& v, std::error_code& ec)
    {
        bool more = true;
        v.clear();
        auto c = source_.peek();
        if (c.eof)
        {
            ec = cbor_errc::unexpected_eof;
            more = false;
            return more;
        }
        jsoncons::cbor::detail::cbor_major_type major_type = get_major_type(c.value);
        uint8_t info = get_additional_information_value(c.value);

        JSONCONS_ASSERT(major_type == jsoncons::cbor::detail::cbor_major_type::byte_string);

        switch(info)
        {
            case jsoncons::cbor::detail::additional_info::indefinite_length:
            {
                auto func = [&v,&more](Source& source, std::size_t length, std::error_code& ec) -> bool
                {
                    if (source_reader<Source>::read(source, v, length) != length)
                    {
                        ec = cbor_errc::unexpected_eof;
                        more = false;
                        return more;
                    }
                    return true;
                };
                iterate_string_chunks(func, major_type, ec);
                break;
            }
            default:
            {
                std::size_t length = get_size(ec);
                if (ec)
                {
                    more = false;
                    return more;
                }
                if (source_reader<Source>::read(source_, v, length) != length)
                {
                    ec = cbor_errc::unexpected_eof;
                    more = false;
                    return more;
                }
                if (!stringref_map_stack_.empty() &&
                    v.size() >= jsoncons::cbor::detail::min_length_for_stringref(stringref_map_stack_.back().size()))
                {
                    stringref_map_stack_.back().emplace_back(mapped_string(v, alloc_));
                }
                break;
            }

        }
        return more;
    }

    template <typename Function>
    void iterate_string_chunks(Function& func, jsoncons::cbor::detail::cbor_major_type type, std::error_code& ec)
    {
        int nesting_level = 0;

        bool done = false;
        while (!done)
        {
            auto c = source_.peek();
            if (c.eof)
            {
                ec = cbor_errc::unexpected_eof;
                more_ = false;
                return;
            }
            if (nesting_level > 0 && c.value == 0xff)
            {
                --nesting_level;
                if (nesting_level == 0)
                {
                    done = true;
                }
                source_.ignore(1);
                continue;
            }

            jsoncons::cbor::detail::cbor_major_type major_type = get_major_type(c.value);
            if (major_type != type)
            {
                ec = cbor_errc::illegal_chunked_string;
                more_ = false;
                return;
            }
            uint8_t info = get_additional_information_value(c.value);

            switch (info)
            {
                case jsoncons::cbor::detail::additional_info::indefinite_length:
                {
                    ++nesting_level;
                    source_.ignore(1);
                    break;
                }
                default: // definite length
                {
                    std::size_t length = get_size(ec);
                    if (!more_)
                    {
                        return;
                    }
                    more_ = func(source_, length, ec);
                    if (!more_)
                    {
                        return;
                    }
                    if (nesting_level == 0)
                    {
                        done = true;
                    }
                    break;
                }
            }
        } 
    }

    uint64_t get_uint64_value(std::error_code& ec)
    {
        uint64_t val = 0;

        uint8_t initial_b;
        if (source_.read(&initial_b, 1) == 0)
        {
            ec = cbor_errc::unexpected_eof;
            more_ = false;
            return 0;
        }
        uint8_t info = get_additional_information_value(initial_b);
        switch (info)
        {
            case JSONCONS_EXT_CBOR_0x00_0x17: // Integer 0x00..0x17 (0..23)
            {
                val = info;
                break;
            }

            case 0x18: // Unsigned integer (one-byte uint8_t follows)
            {
                uint8_t b;
                if (source_.read(&b, 1) == 0)
                {
                    ec = cbor_errc::unexpected_eof;
                    more_ = false;
                    return val;
                }
                val = b;
                break;
            }

            case 0x19: // Unsigned integer (two-byte uint16_t follows)
            {
                uint8_t buf[sizeof(uint16_t)];
                source_.read(buf, sizeof(uint16_t));
                val = binary::big_to_native<uint16_t>(buf, sizeof(buf));
                break;
            }

            case 0x1a: // Unsigned integer (four-byte uint32_t follows)
            {
                uint8_t buf[sizeof(uint32_t)];
                source_.read(buf, sizeof(uint32_t));
                val = binary::big_to_native<uint32_t>(buf, sizeof(buf));
                break;
            }

            case 0x1b: // Unsigned integer (eight-byte uint64_t follows)
            {
                uint8_t buf[sizeof(uint64_t)];
                source_.read(buf, sizeof(uint64_t));
                val = binary::big_to_native<uint64_t>(buf, sizeof(buf));
                break;
            }
            default:
                break;
        }
        return val;
    }

    int64_t get_int64_value(std::error_code& ec)
    {
        int64_t val = 0;

        auto ch = source_.peek();
        if (ch.eof)
        {
            ec = cbor_errc::unexpected_eof;
            more_ = false;
            return val;
        }

        jsoncons::cbor::detail::cbor_major_type major_type = get_major_type(ch.value);
        uint8_t info = get_additional_information_value(ch.value);
        switch (major_type)
        {
            case jsoncons::cbor::detail::cbor_major_type::negative_integer:
                source_.ignore(1);
                switch (info)
                {
                    case JSONCONS_EXT_CBOR_0x00_0x17: // 0x00..0x17 (0..23)
                    {
                        val = static_cast<int8_t>(- 1 - info);
                        break;
                    }
                    case 0x18: // Negative integer (one-byte uint8_t follows)
                        {
                            uint8_t b;
                            if (source_.read(&b, 1) == 0)
                            {
                                ec = cbor_errc::unexpected_eof;
                                more_ = false;
                                return val;
                            }
                            val = static_cast<int64_t>(-1) - static_cast<int64_t>(b);
                            break;
                        }

                    case 0x19: // Negative integer -1-n (two-byte uint16_t follows)
                        {
                            uint8_t buf[sizeof(uint16_t)];
                            if (source_.read(buf, sizeof(uint16_t)) != sizeof(uint16_t))
                            {
                                ec = cbor_errc::unexpected_eof;
                                more_ = false;
                                return val;
                            }
                            auto x = binary::big_to_native<uint16_t>(buf, sizeof(buf));
                            val = static_cast<int64_t>(-1)- x;
                            break;
                        }

                    case 0x1a: // Negative integer -1-n (four-byte uint32_t follows)
                        {
                            uint8_t buf[sizeof(uint32_t)];
                            if (source_.read(buf, sizeof(uint32_t)) != sizeof(uint32_t))
                            {
                                ec = cbor_errc::unexpected_eof;
                                more_ = false;
                                return val;
                            }
                            auto x = binary::big_to_native<uint32_t>(buf, sizeof(buf));
                            val = static_cast<int64_t>(-1)- x;
                            break;
                        }

                    case 0x1b: // Negative integer -1-n (eight-byte uint64_t follows)
                        {
                            uint8_t buf[sizeof(uint64_t)];
                            if (source_.read(buf, sizeof(uint64_t)) != sizeof(uint64_t))
                            {
                                ec = cbor_errc::unexpected_eof;
                                more_ = false;
                                return val;
                            }
                            auto x = binary::big_to_native<uint64_t>(buf, sizeof(buf));
                            val = static_cast<int64_t>(-1)- static_cast<int64_t>(x);
                            break;
                        }
                }
                break;

                case jsoncons::cbor::detail::cbor_major_type::unsigned_integer:
                {
                    uint64_t x = get_uint64_value(ec);
                    if (ec)
                    {
                        return 0;
                    }
                    if (x <= static_cast<uint64_t>((std::numeric_limits<int64_t>::max)()))
                    {
                        val = x;
                    }
                    else
                    {
                        // error;
                    }
                    
                    break;
                }
                break;
            default:
                break;
        }

        return val;
    }

    double get_double(std::error_code& ec)
    {
        double val = 0;

        uint8_t b;
        if (source_.read(&b, 1) == 0)
        {
            ec = cbor_errc::unexpected_eof;
            more_ = false;
            return 0;
        }
        uint8_t info = get_additional_information_value(b);
        switch (info)
        {
        case 0x1a: // Single-Precision Float (four-byte IEEE 754)
            {
                uint8_t buf[sizeof(float)];
                if (source_.read(buf, sizeof(float)) !=sizeof(float)) 
                {
                    ec = cbor_errc::unexpected_eof;
                    more_ = false;
                    return 0;
                }
                val = binary::big_to_native<float>(buf, sizeof(buf));
                break;
            }

        case 0x1b: //  Double-Precision Float (eight-byte IEEE 754)
            {
                uint8_t buf[sizeof(double)];
                if (source_.read(buf, sizeof(double)) != sizeof(double))
                {
                    ec = cbor_errc::unexpected_eof;
                    more_ = false;
                    return 0;
                }
                val = binary::big_to_native<double>(buf, sizeof(buf));
                break;
            }
            default:
                break;
        }
        
        return val;
    }

    void read_decimal_fraction(string_type& result, std::error_code& ec)
    {
        std::size_t size = get_size(ec);
        if (!more_)
        {
            return;
        }
        if (size != 2)
        {
            ec = cbor_errc::invalid_decimal_fraction;
            more_ = false;
            return;
        }

        auto c = source_.peek();
        if (c.eof)
        {
            ec = cbor_errc::unexpected_eof;
            more_ = false;
            return;
        }
        int64_t exponent = 0;
        switch (get_major_type(c.value))
        {
            case jsoncons::cbor::detail::cbor_major_type::unsigned_integer:
            {
                exponent = get_uint64_value(ec);
                if (ec)
                {
                    return;
                }
                break;
            }
            case jsoncons::cbor::detail::cbor_major_type::negative_integer:
            {
                exponent = get_int64_value(ec);
                if (ec)
                {
                    return;
                }
                break;
            }
            default:
            {
                ec = cbor_errc::invalid_decimal_fraction;
                more_ = false;
                return;
            }
        }

        string_type str(alloc_);

        c = source_.peek();
        if (c.eof)
        {
            ec = cbor_errc::unexpected_eof;
            more_ = false;
            return;
        }

        switch (get_major_type(c.value))
        {
            case jsoncons::cbor::detail::cbor_major_type::unsigned_integer:
            {
                uint64_t val = get_uint64_value(ec);
                if (ec)
                {
                    return;
                }
                jsoncons::detail::from_integer(val, str);
                break;
            }
            case jsoncons::cbor::detail::cbor_major_type::negative_integer:
            {
                int64_t val = get_int64_value(ec);
                if (ec)
                {
                    return;
                }
                jsoncons::detail::from_integer(val, str);
                break;
            }
            case jsoncons::cbor::detail::cbor_major_type::semantic_tag:
            {
                uint8_t b;
                if (source_.read(&b, 1) == 0)
                {
                    ec = cbor_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                uint8_t tag = get_additional_information_value(b);
                c = source_.peek();
                if (c.eof)
                {
                    ec = cbor_errc::unexpected_eof;
                    more_ = false;
                    return;
                }

                if (get_major_type(c.value) == jsoncons::cbor::detail::cbor_major_type::byte_string)
                {
                    bytes_buffer_.clear();
                    read_byte_string(bytes_buffer_, ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    if (tag == 2)
                    {
                        bigint n = bigint::from_bytes_be(1, bytes_buffer_.data(), bytes_buffer_.size());
                        n.write_string(str);
                    }
                    else if (tag == 3)
                    {
                        bigint n = bigint::from_bytes_be(1, bytes_buffer_.data(), bytes_buffer_.size());
                        n = -1 - n;
                        n.write_string(str);
                    }
                }
                break;
            }
            default:
            {
                ec = cbor_errc::invalid_decimal_fraction;
                more_ = false;
                return;
            }
        }

        if (str.size() >= static_cast<std::size_t>((std::numeric_limits<int32_t>::max)()) || 
            exponent >= (std::numeric_limits<int32_t>::max)() || 
            exponent <= (std::numeric_limits<int32_t>::min)())
        {
            ec = cbor_errc::invalid_decimal_fraction;
            more_ = false;
            return;
        }
        else if (str.size() > 0)
        {
            if (str[0] == '-')
            {
                result.push_back('-');
                jsoncons::detail::prettify_string(str.c_str()+1, str.size()-1, (int)exponent, -4, 17, result);
            }
            else
            {
                jsoncons::detail::prettify_string(str.c_str(), str.size(), (int)exponent, -4, 17, result);
            }
        }
        else
        {
            ec = cbor_errc::invalid_decimal_fraction;
            more_ = false;
            return;
        }
    }

    void read_bigfloat(string_type& str, std::error_code& ec)
    {
        std::size_t size = get_size(ec);
        if (!more_)
        {
            return;
        }
        if (size != 2)
        {
            ec = cbor_errc::invalid_bigfloat;
            more_ = false;
            return;
        }

        auto c = source_.peek();
        if (c.eof)
        {
            ec = cbor_errc::unexpected_eof;
            more_ = false;
            return;
        }
        int64_t exponent = 0;
        switch (get_major_type(c.value))
        {
            case jsoncons::cbor::detail::cbor_major_type::unsigned_integer:
            {
                exponent = get_uint64_value(ec);
                if (ec)
                {
                    return;
                }
                break;
            }
            case jsoncons::cbor::detail::cbor_major_type::negative_integer:
            {
                exponent = get_int64_value(ec);
                if (ec)
                {
                    return;
                }
                break;
            }
            default:
            {
                ec = cbor_errc::invalid_bigfloat;
                more_ = false;
                return;
            }
        }

        c = source_.peek();
        if (c.eof)
        {
            ec = cbor_errc::unexpected_eof;
            more_ = false;
            return;
        }
        switch (get_major_type(c.value))
        {
            case jsoncons::cbor::detail::cbor_major_type::unsigned_integer:
            {
                uint64_t val = get_uint64_value(ec);
                if (ec)
                {
                    return;
                }
                str.push_back('0');
                str.push_back('x');
                jsoncons::detail::integer_to_hex(val, str);
                break;
            }
            case jsoncons::cbor::detail::cbor_major_type::negative_integer:
            {
                int64_t val = get_int64_value(ec);
                if (ec)
                {
                    return;
                }
                str.push_back('-');
                str.push_back('0');
                str.push_back('x');
                jsoncons::detail::integer_to_hex(static_cast<uint64_t>(-val), str);
                break;
            }
            case jsoncons::cbor::detail::cbor_major_type::semantic_tag:
            {
                uint8_t b;
                if (source_.read(&b, 1) == 0)
                {
                    ec = cbor_errc::unexpected_eof;
                    more_ = false;
                    return;
                }
                uint8_t tag = get_additional_information_value(b);

                c = source_.peek();
                if (c.eof)
                {
                    ec = cbor_errc::unexpected_eof;
                    more_ = false;
                    return;
                }

                if (get_major_type(c.value) == jsoncons::cbor::detail::cbor_major_type::byte_string)
                {
                    bytes_buffer_.clear(); 
                    more_ = read_byte_string(bytes_buffer_, ec);
                    if (!more_)
                    {
                        return;
                    }
                    if (tag == 2)
                    {
                        str.push_back('0');
                        str.push_back('x');
                        bigint n = bigint::from_bytes_be(1, bytes_buffer_.data(), bytes_buffer_.size());
                        n.write_string_hex(str);
                    }
                    else if (tag == 3)
                    {
                        str.push_back('-');
                        str.push_back('0');
                        bigint n = bigint::from_bytes_be(1, bytes_buffer_.data(), bytes_buffer_.size());
                        n = -1 - n;
                        n.write_string_hex(str);
                        str[2] = 'x'; // overwrite minus
                    }
                }
                break;
            }
            default:
            {
                ec = cbor_errc::invalid_bigfloat;
                more_ = false;
                return;
            }
        }

        str.push_back('p');
        if (exponent >=0)
        {
            jsoncons::detail::integer_to_hex(static_cast<uint64_t>(exponent), str);
        }
        else
        {
            str.push_back('-');
            jsoncons::detail::integer_to_hex(static_cast<uint64_t>(-exponent), str);
        }
    }

    static jsoncons::cbor::detail::cbor_major_type get_major_type(uint8_t type)
    {
        static constexpr uint8_t major_type_shift = 0x05;
        uint8_t value = type >> major_type_shift;
        return static_cast<jsoncons::cbor::detail::cbor_major_type>(value);
    }

    static uint8_t get_additional_information_value(uint8_t type)
    {
        static constexpr uint8_t additional_information_mask = (1U << 5) - 1;
        uint8_t value = type & additional_information_mask;
        return value;
    }

    void read_tags(std::error_code& ec)
    {
        auto c = source_.peek();
        if (c.eof)
        {
            ec = cbor_errc::unexpected_eof;
            more_ = false;
            return;
        }
        jsoncons::cbor::detail::cbor_major_type major_type = get_major_type(c.value);

        while (major_type == jsoncons::cbor::detail::cbor_major_type::semantic_tag)
        {
            uint64_t val = get_uint64_value(ec);
            if (!more_)
            {
                return;
            }
            switch(val)
            {
                case 25: // stringref
                    other_tags_[stringref_tag] = true;
                    break;
                case 256: // stringref-namespace
                    other_tags_[stringref_namespace_tag] = true;
                    break;
                default:
                    other_tags_[item_tag] = true;
                    raw_tag_ = val;
                    break;
            }
            c = source_.peek();
            if (c.eof)
            {
                ec = cbor_errc::unexpected_eof;
                more_ = false;
                return;
            }
            major_type = get_major_type(c.value);
        }
    }

    void handle_string(item_event_visitor& visitor, const jsoncons::basic_string_view<char>& v, std::error_code& ec)
    {
        semantic_tag tag = semantic_tag::none;
        if (other_tags_[item_tag])
        {
            switch (raw_tag_)
            {
                case 0:
                    tag = semantic_tag::datetime;
                    break;
                case 32:
                    tag = semantic_tag::uri;
                    break;
                case 33:
                    tag = semantic_tag::base64url;
                    break;
                case 34:
                    tag = semantic_tag::base64;
                    break;
                default:
                    break;
            }
            other_tags_[item_tag] = false;
        }
        more_ = visitor.string_value(v, tag, *this, ec);
    }

    static jsoncons::endian get_typed_array_endianness(const uint8_t tag)
    {
        return ((tag & detail::cbor_array_tags_e_mask) >> detail::cbor_array_tags_e_shift) == 0 ? jsoncons::endian::big : jsoncons::endian::little; 
    }

    static std::size_t get_typed_array_bytes_per_element(const uint8_t tag)
    {
        const uint8_t f = (tag & detail::cbor_array_tags_f_mask) >> detail::cbor_array_tags_f_shift; 
        const uint8_t ll = (tag & detail::cbor_array_tags_ll_mask) >> detail::cbor_array_tags_ll_shift; 

        return std::size_t(1) << (f + ll); 
    }

    template <typename Read>
    void write_byte_string(Read read, item_event_visitor& visitor, std::error_code& ec)
    {
        if (other_tags_[item_tag])
        {
            switch (raw_tag_)
            {
                case 0x2:
                {
                    bytes_buffer_.clear();
                    read(bytes_buffer_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    bigint n = bigint::from_bytes_be(1, bytes_buffer_.data(), bytes_buffer_.size());
                    text_buffer_.clear();
                    n.write_string(text_buffer_);
                    more_ = visitor.string_value(text_buffer_, semantic_tag::bigint, *this, ec);
                    break;
                }
                case 0x3:
                {
                    bytes_buffer_.clear();
                    read(bytes_buffer_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    bigint n = bigint::from_bytes_be(1, bytes_buffer_.data(), bytes_buffer_.size());
                    n = -1 - n;
                    text_buffer_.clear();
                    n.write_string(text_buffer_);
                    more_ = visitor.string_value(text_buffer_, semantic_tag::bigint, *this, ec);
                    break;
                }
                case 0x15:
                {
                    read(bytes_buffer_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    more_ = visitor.byte_string_value(bytes_buffer_, semantic_tag::base64url, *this, ec);
                    break;
                }
                case 0x16:
                {
                    read(bytes_buffer_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    more_ = visitor.byte_string_value(bytes_buffer_, semantic_tag::base64, *this, ec);
                    break;
                }
                case 0x17:
                {
                    read(bytes_buffer_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    more_ = visitor.byte_string_value(bytes_buffer_, semantic_tag::base16, *this, ec);
                    break;
                }
                case 0x40:
                {
                    typed_array_.clear();
                    read(typed_array_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    uint8_t* data = reinterpret_cast<uint8_t*>(typed_array_.data());
                    std::size_t size = typed_array_.size();
                    more_ = visitor.typed_array(jsoncons::span<const uint8_t>(data,size), semantic_tag::none, *this, ec);
                    break;
                }
                case 0x44:
                {
                    typed_array_.clear();
                    read(typed_array_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    uint8_t* data = reinterpret_cast<uint8_t*>(typed_array_.data());
                    std::size_t size = typed_array_.size();
                    more_ = visitor.typed_array(jsoncons::span<const uint8_t>(data,size), semantic_tag::clamped, *this, ec);
                    break;
                }
                case 0x41:
                case 0x45:
                {
                    typed_array_.clear();
                    read(typed_array_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    const uint8_t tag = (uint8_t)raw_tag_;
                    jsoncons::endian e = get_typed_array_endianness(tag); 
                    const size_t bytes_per_elem = get_typed_array_bytes_per_element(tag);

                    uint16_t* data = reinterpret_cast<uint16_t*>(typed_array_.data());
                    std::size_t size = typed_array_.size()/bytes_per_elem;

                    if (e != jsoncons::endian::native)
                    {
                        for (std::size_t i = 0; i < size; ++i)
                        {
                            data[i] = binary::byte_swap<uint16_t>(data[i]);
                        }
                    }
                    more_ = visitor.typed_array(jsoncons::span<const uint16_t>(data,size), semantic_tag::none, *this, ec);
                    break;
                }
                case 0x42:
                case 0x46:
                {
                    typed_array_.clear();
                    read(typed_array_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    const uint8_t tag = (uint8_t)raw_tag_;
                    jsoncons::endian e = get_typed_array_endianness(tag);
                    const size_t bytes_per_elem = get_typed_array_bytes_per_element(tag);

                    uint32_t* data = reinterpret_cast<uint32_t*>(typed_array_.data());
                    std::size_t size = typed_array_.size()/bytes_per_elem;
                    if (e != jsoncons::endian::native)
                    {
                        for (std::size_t i = 0; i < size; ++i)
                        {
                            data[i] = binary::byte_swap<uint32_t>(data[i]);
                        }
                    }
                    more_ = visitor.typed_array(jsoncons::span<const uint32_t>(data,size), semantic_tag::none, *this, ec);
                    break;
                }
                case 0x43:
                case 0x47:
                {
                    typed_array_.clear();
                    read(typed_array_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    const uint8_t tag = (uint8_t)raw_tag_;
                    jsoncons::endian e = get_typed_array_endianness(tag); 
                    const size_t bytes_per_elem = get_typed_array_bytes_per_element(tag);

                    uint64_t* data = reinterpret_cast<uint64_t*>(typed_array_.data());
                    std::size_t size = typed_array_.size()/bytes_per_elem;
                    if (e != jsoncons::endian::native)
                    {
                        for (std::size_t i = 0; i < size; ++i)
                        {
                            data[i] = binary::byte_swap<uint64_t>(data[i]);
                        }
                    }
                    more_ = visitor.typed_array(jsoncons::span<const uint64_t>(data,size), semantic_tag::none, *this, ec);
                    break;
                }
                case 0x48:
                {
                    typed_array_.clear();
                    read(typed_array_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    int8_t* data = reinterpret_cast<int8_t*>(typed_array_.data());
                    std::size_t size = typed_array_.size();
                    more_ = visitor.typed_array(jsoncons::span<const int8_t>(data,size), semantic_tag::none, *this, ec);
                    break;
                }
                case 0x49:
                case 0x4d:
                {
                    typed_array_.clear();
                    read(typed_array_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    const uint8_t tag = (uint8_t)raw_tag_;
                    jsoncons::endian e = get_typed_array_endianness(tag); 
                    const size_t bytes_per_elem = get_typed_array_bytes_per_element(tag);

                    int16_t* data = reinterpret_cast<int16_t*>(typed_array_.data());
                    std::size_t size = typed_array_.size()/bytes_per_elem;
                    if (e != jsoncons::endian::native)
                    {
                        for (std::size_t i = 0; i < size; ++i)
                        {
                            data[i] = binary::byte_swap<int16_t>(data[i]);
                        }
                    }
                    more_ = visitor.typed_array(jsoncons::span<const int16_t>(data,size), semantic_tag::none, *this, ec);
                    break;
                }
                case 0x4a:
                case 0x4e:
                {
                    typed_array_.clear();
                    read(typed_array_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    const uint8_t tag = (uint8_t)raw_tag_;
                    jsoncons::endian e = get_typed_array_endianness(tag); 
                    const size_t bytes_per_elem = get_typed_array_bytes_per_element(tag);

                    int32_t* data = reinterpret_cast<int32_t*>(typed_array_.data());
                    std::size_t size = typed_array_.size()/bytes_per_elem;
                    if (e != jsoncons::endian::native)
                    {
                        for (std::size_t i = 0; i < size; ++i)
                        {
                            data[i] = binary::byte_swap<int32_t>(data[i]);
                        }
                    }
                    more_ = visitor.typed_array(jsoncons::span<const int32_t>(data,size), semantic_tag::none, *this, ec);
                    break;
                }
                case 0x4b:
                case 0x4f:
                {
                    typed_array_.clear();
                    read(typed_array_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    const uint8_t tag = (uint8_t)raw_tag_;
                    jsoncons::endian e = get_typed_array_endianness(tag); 
                    const size_t bytes_per_elem = get_typed_array_bytes_per_element(tag);

                    int64_t* data = reinterpret_cast<int64_t*>(typed_array_.data());
                    std::size_t size = typed_array_.size()/bytes_per_elem;
                    if (e != jsoncons::endian::native)
                    {
                        for (std::size_t i = 0; i < size; ++i)
                        {
                            data[i] = binary::byte_swap<int64_t>(data[i]);
                        }
                    }
                    more_ = visitor.typed_array(jsoncons::span<const int64_t>(data,size), semantic_tag::none, *this, ec);
                    break;
                }
                case 0x50:
                case 0x54:
                {
                    typed_array_.clear();
                    read(typed_array_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    const uint8_t tag = (uint8_t)raw_tag_;
                    jsoncons::endian e = get_typed_array_endianness(tag); 
                    const size_t bytes_per_elem = get_typed_array_bytes_per_element(tag);

                    uint16_t* data = reinterpret_cast<uint16_t*>(typed_array_.data());
                    std::size_t size = typed_array_.size()/bytes_per_elem;
                    if (e != jsoncons::endian::native)
                    {
                        for (std::size_t i = 0; i < size; ++i)
                        {
                            data[i] = binary::byte_swap<uint16_t>(data[i]);
                        }
                    }
                    more_ = visitor.typed_array(half_arg, jsoncons::span<const uint16_t>(data,size), semantic_tag::none, *this, ec);
                    break;
                }
                case 0x51:
                case 0x55:
                {
                    typed_array_.clear();
                    read(typed_array_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    const uint8_t tag = (uint8_t)raw_tag_;
                    jsoncons::endian e = get_typed_array_endianness(tag); 
                    const size_t bytes_per_elem = get_typed_array_bytes_per_element(tag);

                    float* data = reinterpret_cast<float*>(typed_array_.data());
                    std::size_t size = typed_array_.size()/bytes_per_elem;
                    if (e != jsoncons::endian::native)
                    {
                        for (std::size_t i = 0; i < size; ++i)
                        {
                            data[i] = binary::byte_swap<float>(data[i]);
                        }
                    }
                    more_ = visitor.typed_array(jsoncons::span<const float>(data,size), semantic_tag::none, *this, ec);
                    break;
                }
                case 0x52:
                case 0x56:
                {
                    typed_array_.clear();
                    read(typed_array_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    const uint8_t tag = (uint8_t)raw_tag_;
                    jsoncons::endian e = get_typed_array_endianness(tag); 
                    const size_t bytes_per_elem = get_typed_array_bytes_per_element(tag);

                    double* data = reinterpret_cast<double*>(typed_array_.data());
                    std::size_t size = typed_array_.size()/bytes_per_elem;

                    if (e != jsoncons::endian::native)
                    {
                        for (std::size_t i = 0; i < size; ++i)
                        {
                            data[i] = binary::byte_swap<double>(data[i]);
                        }
                    }
                    more_ = visitor.typed_array(jsoncons::span<const double>(data,size), semantic_tag::none, *this, ec);
                    break;
                }
                default:
                {
                    read(bytes_buffer_,ec);
                    if (ec)
                    {
                        more_ = false;
                        return;
                    }
                    more_ = visitor.byte_string_value(bytes_buffer_, raw_tag_, *this, ec);
                    break;
                }
            }
            other_tags_[item_tag] = false;
        }
        else
        {
            read(bytes_buffer_,ec);
            if (ec)
            {
                return;
            }
            more_ = visitor.byte_string_value(bytes_buffer_, semantic_tag::none, *this, ec);
        }
    }

    void produce_begin_multi_dim(item_event_visitor& visitor, 
                                 semantic_tag tag,
                                 std::error_code& ec)
    {
        uint8_t b;
        if (source_.read(&b, 1) == 0)
        {
            ec = cbor_errc::unexpected_eof;
            more_ = false;
            return;
        }
        jsoncons::cbor::detail::cbor_major_type major_type = get_major_type(b);
        JSONCONS_ASSERT(major_type == jsoncons::cbor::detail::cbor_major_type::array);
        uint8_t info = get_additional_information_value(b);
       
        read_shape(info, ec);   
        if (ec)
        {
            return;
        }

        state_stack_.emplace_back(parse_mode::multi_dim, 0);
        more_ = visitor.begin_multi_dim(shape_, tag, *this, ec);
    }

    void produce_end_multi_dim(item_event_visitor& visitor, std::error_code& ec)
    {
        more_ = visitor.end_multi_dim(*this, ec);
        state_stack_.pop_back();
    }

    void read_shape(uint8_t info, std::error_code& ec)
    {
        shape_.clear();
        switch (info)
        {
            case jsoncons::cbor::detail::additional_info::indefinite_length:
            {
                while (true)
                {
                    auto c = source_.peek();
                    if (c.eof)
                    {
                        ec = cbor_errc::unexpected_eof;
                        more_ = false;
                        return;
                    }
                    if (c.value == 0xff)
                    {
                        source_.ignore(1);
                    }
                    else
                    {
                        std::size_t dim = get_size(ec);
                        if (!more_)
                        {
                            return;
                        }
                        shape_.push_back(dim);
                    }
                }
                break;
            }
            default:
            {
                std::size_t size = get_size(ec);
                if (!more_)
                {
                    return;
                }
                for (std::size_t i = 0; more_ && i < size; ++i)
                {
                    std::size_t dim = get_size(ec);
                    if (!more_)
                    {
                        return;
                    }
                    shape_.push_back(dim);
                }
                break;
            }
        }
    }
};

} // namespace cbor
} // namespace jsoncons

#endif
