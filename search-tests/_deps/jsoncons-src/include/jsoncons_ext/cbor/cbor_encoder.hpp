// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_CBOR_CBOR_ENCODER_HPP
#define JSONCONS_EXT_CBOR_CBOR_ENCODER_HPP

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits> // std::numeric_limits
#include <map>
#include <memory>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility> // std::move
#include <vector>

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/detail/parse_number.hpp>
#include <jsoncons/json_exception.hpp> // jsoncons::ser_error
#include <jsoncons/json_visitor.hpp>
#include <jsoncons/ser_context.hpp>
#include <jsoncons/sink.hpp>
#include <jsoncons/tag_type.hpp>
#include <jsoncons/utility/binary.hpp>
#include <jsoncons/utility/unicode_traits.hpp>

#include <jsoncons_ext/cbor/cbor_error.hpp>
#include <jsoncons_ext/cbor/cbor_options.hpp>

namespace jsoncons { namespace cbor {

enum class cbor_container_type {object, indefinite_length_object, array, indefinite_length_array};

template <typename Sink=jsoncons::binary_stream_sink,typename Allocator=std::allocator<char>>
class basic_cbor_encoder final : public basic_json_visitor<char>
{
    using super_type = basic_json_visitor<char>;

    enum class decimal_parse_state { start, integer, exp1, exp2, fraction1 };
    enum class hexfloat_parse_state { start, expect_0, expect_x, integer, exp1, exp2, fraction1 };

    static constexpr int64_t nanos_in_second = 1000000000;
    static constexpr int64_t millis_in_second = 1000;

public:
    using allocator_type = Allocator;
    using sink_type = Sink;
    using typename super_type::char_type;
    using typename super_type::string_view_type;

private:
    using char_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<char_type>;
    using byte_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<uint8_t>;                  

    using string_type = std::basic_string<char_type,std::char_traits<char_type>,char_allocator_type>;
    using byte_string_type = basic_byte_string<byte_allocator_type>;

    struct stack_item
    {
        cbor_container_type type_;
        std::size_t length_{0};
        std::size_t index_{0};

        stack_item(cbor_container_type type, std::size_t length = 0) noexcept
           : type_(type), length_(length)
        {
        }
        
        ~stack_item() = default; 

        std::size_t length() const
        {
            return length_;
        }

        std::size_t count() const
        {
            return is_object() ? index_/2 : index_;
        }

        bool is_object() const
        {
            return type_ == cbor_container_type::object || type_ == cbor_container_type::indefinite_length_object;
        }

        bool is_indefinite_length() const
        {
            return type_ == cbor_container_type::indefinite_length_array || type_ == cbor_container_type::indefinite_length_object;
        }

    };

    using string_size_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<std::pair<const string_type,size_t>>;
    using byte_string_size_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<std::pair<const byte_string_type,size_t>>;
    using stack_item_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<stack_item>;

    Sink sink_;
    const cbor_encode_options options_;
    allocator_type alloc_;

    std::vector<stack_item,stack_item_allocator_type> stack_;
    std::map<string_type,size_t,std::less<string_type>,string_size_allocator_type> stringref_map_;
    std::map<byte_string_type,size_t,std::less<byte_string_type>,byte_string_size_allocator_type> bytestringref_map_;
    std::size_t next_stringref_ = 0;
    int nesting_depth_;
public:

    // Noncopyable and nonmoveable
    basic_cbor_encoder(const basic_cbor_encoder&) = delete;
    basic_cbor_encoder(basic_cbor_encoder&&) = delete;

    explicit basic_cbor_encoder(Sink&& sink, 
                                const Allocator& alloc = Allocator())
       : basic_cbor_encoder(std::forward<Sink>(sink), cbor_encode_options(), alloc)
    {
    }
    basic_cbor_encoder(Sink&& sink, 
                       const cbor_encode_options& options, 
                       const Allocator& alloc = Allocator())
       : sink_(std::forward<Sink>(sink)), 
         options_(options), 
         alloc_(alloc),
         stack_(alloc),
         stringref_map_(alloc),
         bytestringref_map_(alloc),
         nesting_depth_(0)        
    {
        if (options.pack_strings())
        {
            write_tag(256);
        }
    }

    ~basic_cbor_encoder() noexcept
    {
        JSONCONS_TRY
        {
            sink_.flush();
        }
        JSONCONS_CATCH(...)
        {
        }
    }

    basic_cbor_encoder& operator=(const basic_cbor_encoder&) = delete;
    basic_cbor_encoder& operator=(basic_cbor_encoder&&) = delete;

    void reset()
    {
        stack_.clear();
        stringref_map_.clear();
        bytestringref_map_.clear();
        next_stringref_ = 0;
        nesting_depth_ = 0;
    }

    void reset(Sink&& sink)
    {
        sink_ = std::move(sink);
        reset();
    }

    void begin_object_with_tag(uint64_t raw_tag)
    {
        write_tag(raw_tag);
        begin_object();
    }

    void begin_object_with_tag(std::size_t length, uint64_t raw_tag)
    {
        write_tag(raw_tag);
        begin_object(length);
    }

    void begin_array_with_tag(uint64_t raw_tag)
    {
        write_tag(raw_tag);
        begin_array();
    }

    void begin_array_with_tag(std::size_t length, uint64_t raw_tag)
    {
        write_tag(raw_tag);
        begin_array(length);
    }

    void null_value_with_tag(uint64_t raw_tag)
    {
        write_tag(raw_tag);
        sink_.push_back(0xf6);
        end_value();
    }  

    void bool_value_with_tag(bool value, uint64_t raw_tag)
    {
        write_tag(raw_tag);
        if (value)
        {
            sink_.push_back(0xf5);
        }
        else
        {
            sink_.push_back(0xf4);
        }

        end_value();
    }  

    void string_value_with_tag(const string_view_type& value, uint64_t raw_tag) 
    {
        write_tag(raw_tag);
        write_string(value);
        end_value();
    }

    template <typename ByteStringLike>
    void byte_string_value_with_tag(const ByteStringLike& value, uint64_t raw_tag,
        typename std::enable_if<extension_traits::is_byte_sequence<ByteStringLike>::value,int>::type = 0) 
    {
        write_tag(raw_tag);
        write_byte_string(byte_string_view(reinterpret_cast<const uint8_t*>(value.data()),value.size()));
        end_value();
    }

    void double_value_with_tag(double value, uint64_t raw_tag) 
    {
        write_tag(raw_tag);
        double_value(value);
    }
    
    void uint64_value_with_tag(uint64_t value, uint64_t raw_tag) 
    {
        write_tag(raw_tag);
        write_uint64_value(value);
        end_value();
    }

    void int64_value_with_tag(int64_t value, uint64_t raw_tag) 
    {
        write_tag(raw_tag);
        write_int64_value(value);
        end_value();
    }

private:
    // Implementing methods

    void visit_flush() override
    {
        sink_.flush();
    }

    bool visit_begin_object(semantic_tag, const ser_context&, std::error_code& ec) override
    {
        if (JSONCONS_UNLIKELY(++nesting_depth_ > options_.max_nesting_depth()))
        {
            ec = cbor_errc::max_nesting_depth_exceeded;
            return false;
        } 
        stack_.emplace_back(cbor_container_type::indefinite_length_object);
        
        sink_.push_back(0xbf);
        return true;
    }

    bool visit_begin_object(std::size_t length, semantic_tag, const ser_context&, std::error_code& ec) override
    {
        if (JSONCONS_UNLIKELY(++nesting_depth_ > options_.max_nesting_depth()))
        {
            ec = cbor_errc::max_nesting_depth_exceeded;
            return false;
        } 
        stack_.emplace_back(cbor_container_type::object, length);

        if (length <= 0x17)
        {
            binary::native_to_big(static_cast<uint8_t>(0xa0 + length), 
                                  std::back_inserter(sink_));
        } 
        else if (length <= 0xff)
        {
            binary::native_to_big(static_cast<uint8_t>(0xb8), 
                                  std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint8_t>(length), 
                                  std::back_inserter(sink_));
        } 
        else if (length <= 0xffff)
        {
            binary::native_to_big(static_cast<uint8_t>(0xb9), 
                                  std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint16_t>(length), 
                                  std::back_inserter(sink_));
        } 
        else if (length <= 0xffffffff)
        {
            binary::native_to_big(static_cast<uint8_t>(0xba), 
                                  std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint32_t>(length), 
                                  std::back_inserter(sink_));
        } 
        else if (uint64_t(length) <= (std::numeric_limits<std::uint64_t>::max)())
        {
            binary::native_to_big(static_cast<uint8_t>(0xbb), 
                                  std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint64_t>(length), 
                                  std::back_inserter(sink_));
        }

        return true;
    }

    bool visit_end_object(const ser_context&, std::error_code& ec) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        --nesting_depth_;

        if (stack_.back().is_indefinite_length())
        {
            sink_.push_back(0xff);
        }
        else
        {
            if (stack_.back().count() < stack_.back().length())
            {
                ec = cbor_errc::too_few_items;
                return false;
            }
            if (stack_.back().count() > stack_.back().length())
            {
                ec = cbor_errc::too_many_items;
                return false;
            }
        }

        stack_.pop_back();
        end_value();

        return true;
    }

    bool visit_begin_array(semantic_tag, const ser_context&, std::error_code& ec) override
    {
        if (JSONCONS_UNLIKELY(++nesting_depth_ > options_.max_nesting_depth()))
        {
            ec = cbor_errc::max_nesting_depth_exceeded;
            return false;
        } 
        stack_.emplace_back(cbor_container_type::indefinite_length_array);
        sink_.push_back(0x9f);
        return true;
    }

    bool visit_begin_array(std::size_t length, semantic_tag, const ser_context&, std::error_code& ec) override
    {
        if (JSONCONS_UNLIKELY(++nesting_depth_ > options_.max_nesting_depth()))
        {
            ec = cbor_errc::max_nesting_depth_exceeded;
            return false;
        } 
        stack_.emplace_back(cbor_container_type::array, length);
        if (length <= 0x17)
        {
            binary::native_to_big(static_cast<uint8_t>(0x80 + length), 
                                  std::back_inserter(sink_));
        } 
        else if (length <= 0xff)
        {
            binary::native_to_big(static_cast<uint8_t>(0x98), 
                                  std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint8_t>(length), 
                                  std::back_inserter(sink_));
        } 
        else if (length <= 0xffff)
        {
            binary::native_to_big(static_cast<uint8_t>(0x99), 
                                  std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint16_t>(length), 
                                  std::back_inserter(sink_));
        } 
        else if (length <= 0xffffffff)
        {
            binary::native_to_big(static_cast<uint8_t>(0x9a), 
                                  std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint32_t>(length), 
                                  std::back_inserter(sink_));
        } 
        else if (uint64_t(length) <= (std::numeric_limits<std::uint64_t>::max)())
        {
            binary::native_to_big(static_cast<uint8_t>(0x9b), 
                                  std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint64_t>(length), 
                                  std::back_inserter(sink_));
        }
        return true;
    }

    bool visit_end_array(const ser_context&, std::error_code& ec) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        --nesting_depth_;

        if (stack_.back().is_indefinite_length())
        {
            sink_.push_back(0xff);
        }
        else
        {
            if (stack_.back().count() < stack_.back().length())
            {
                ec = cbor_errc::too_few_items;
                return false;
            }
            if (stack_.back().count() > stack_.back().length())
            {
                ec = cbor_errc::too_many_items;
                return false;
            }
        }

        stack_.pop_back();
        end_value();

        return true;
    }

    bool visit_key(const string_view_type& name, const ser_context& context, std::error_code& ec) override
    {
        return visit_string(name, semantic_tag::none, context, ec);
    }

    bool visit_null(semantic_tag tag, const ser_context&, std::error_code&) override
    {
        if (tag == semantic_tag::undefined)
        {
            sink_.push_back(0xf7);
        }
        else
        {
            sink_.push_back(0xf6);
        }

        end_value();
        return true;
    }

    void write_string(const string_view& sv)
    {
        auto sink = unicode_traits::validate(sv.data(), sv.size());
        if (sink.ec != unicode_traits::conv_errc())
        {
            JSONCONS_THROW(ser_error(cbor_errc::invalid_utf8_text_string));
        }

        if (options_.pack_strings() && sv.size() >= jsoncons::cbor::detail::min_length_for_stringref(next_stringref_))
        {
            string_type s(sv.data(), sv.size(), alloc_);
            auto it = stringref_map_.find(s);
            if (it == stringref_map_.end())
            {
                stringref_map_.emplace(std::make_pair(std::move(s), next_stringref_++));
                write_utf8_string(sv);
            }
            else
            {
                write_tag(25);
                write_uint64_value((*it).second);
            }
        }
        else
        {
            write_utf8_string(sv);
        }
    }

    void write_utf8_string(const string_view& sv)
    {
        const size_t length = sv.size();

        if (length <= 0x17)
        {
            // fixstr stores a byte array whose length is upto 31 bytes
            binary::native_to_big(static_cast<uint8_t>(0x60 + length), 
                                            std::back_inserter(sink_));
        }
        else if (length <= 0xff)
        {
            binary::native_to_big(static_cast<uint8_t>(0x78), 
                                            std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint8_t>(length), 
                                            std::back_inserter(sink_));
        }
        else if (length <= 0xffff)
        {
            binary::native_to_big(static_cast<uint8_t>(0x79), 
                                            std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint16_t>(length), 
                                            std::back_inserter(sink_));
        }
        else if (length <= 0xffffffff)
        {
            binary::native_to_big(static_cast<uint8_t>(0x7a), 
                                            std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint32_t>(length), 
                                            std::back_inserter(sink_));
        }
        else if (uint64_t(length) <= (std::numeric_limits<std::uint64_t>::max)())
        {
            binary::native_to_big(static_cast<uint8_t>(0x7b), 
                                            std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint64_t>(length), 
                                            std::back_inserter(sink_));
        }

        for (auto c : sv)
        {
            sink_.push_back(c);
        }
    }

    void write_bignum(bigint& n)
    {
        bool is_neg = n < 0;
        if (is_neg)
        {
            n = - n -1;
        }

        int signum;
        std::vector<uint8_t> data;
        n.write_bytes_be(signum, data);
        std::size_t length = data.size();

        if (is_neg)
        {
            write_tag(3);
        }
        else
        {
            write_tag(2);
        }

        if (length <= 0x17)
        {
            // fixstr stores a byte array whose length is upto 31 bytes
            binary::native_to_big(static_cast<uint8_t>(0x40 + length), 
                                  std::back_inserter(sink_));
        }
        else if (length <= 0xff)
        {
            binary::native_to_big(static_cast<uint8_t>(0x58), 
                                  std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint8_t>(length), 
                                  std::back_inserter(sink_));
        }
        else if (length <= 0xffff)
        {
            binary::native_to_big(static_cast<uint8_t>(0x59), 
                                  std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint16_t>(length), 
                                  std::back_inserter(sink_));
        }
        else if (length <= 0xffffffff)
        {
            binary::native_to_big(static_cast<uint8_t>(0x5a), 
                                  std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint32_t>(length), 
                                  std::back_inserter(sink_));
        }
        else if (uint64_t(length) <= (std::numeric_limits<std::uint64_t>::max)())
        {
            binary::native_to_big(static_cast<uint8_t>(0x5b), 
                                  std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint64_t>(length), 
                                  std::back_inserter(sink_));
        }

        for (auto c : data)
        {
            sink_.push_back(c);
        }
    }

    bool write_decimal_value(const string_view_type& sv, const ser_context& context, std::error_code& ec)
    {
        bool more = true;

        decimal_parse_state state = decimal_parse_state::start;
        std::basic_string<char> s;
        std::basic_string<char> exponent;
        int64_t scale = 0;
        for (auto c : sv)
        {
            switch (state)
            {
                case decimal_parse_state::start:
                {
                    switch (c)
                    {
                        case '-':
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                            s.push_back(c);
                            state = decimal_parse_state::integer;
                            break;
                        default:
                        {
                            ec = cbor_errc::invalid_decimal_fraction;
                            return false;
                        }
                    }
                    break;
                }
                case decimal_parse_state::integer:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                            s.push_back(c);
                            break;
                        case 'e': case 'E':
                            state = decimal_parse_state::exp1;
                            break;
                        case '.':
                            state = decimal_parse_state::fraction1;
                            break;
                        default:
                        {
                            ec = cbor_errc::invalid_decimal_fraction;
                            return false;
                        }
                    }
                    break;
                }
                case decimal_parse_state::exp1:
                {
                    switch (c)
                    {
                        case '+':
                            state = decimal_parse_state::exp2;
                            break;
                        case '-':
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                            exponent.push_back(c);
                            state = decimal_parse_state::exp2;
                            break;
                        default:
                        {
                            ec = cbor_errc::invalid_decimal_fraction;
                            return false;
                        }
                    }
                    break;
                }
                case decimal_parse_state::exp2:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                            exponent.push_back(c);
                            break;
                        default:
                        {
                            ec = cbor_errc::invalid_decimal_fraction;
                            return false;
                        }
                    }
                    break;
                }
                case decimal_parse_state::fraction1:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                            s.push_back(c);
                            --scale;
                            break;
                        default:
                        {
                            ec = cbor_errc::invalid_decimal_fraction;
                            return false;
                        }
                    }
                    break;
                }
            }
        }

        write_tag(4);
        more = visit_begin_array((std::size_t)2, semantic_tag::none, context, ec);
        if (!more) {return more;}
        if (exponent.length() > 0)
        {
            int64_t val{};
            auto r = jsoncons::detail::to_integer(exponent.data(), exponent.length(), val);
            if (!r)
            {
                ec = r.error_code();
                return false;
            }
            scale += val;
        }
        more = visit_int64(scale, semantic_tag::none, context, ec);
        if (!more) {return more;}

        int64_t val{ 0 };
        auto r = jsoncons::detail::to_integer(s.data(),s.length(), val);
        if (r)
        {
            more = visit_int64(val, semantic_tag::none, context, ec);
            if (!more) {return more;}
        }
        else if (r.error_code() == jsoncons::detail::to_integer_errc::overflow)
        {
            bigint n = bigint::from_string(s.data(), s.length());
            write_bignum(n);
            end_value();
        }
        else
        {
            ec = r.error_code();
            return false;
        }
        more = visit_end_array(context, ec);

        return more;
    }

    bool write_hexfloat_value(const string_view_type& sv, const ser_context& context, std::error_code& ec)
    {
        bool more = true;

        hexfloat_parse_state state = hexfloat_parse_state::start;
        std::basic_string<char> s;
        std::basic_string<char> exponent;
        int64_t scale = 0;

        for (auto c : sv)
        {
            switch (state)
            {
                case hexfloat_parse_state::start:
                {
                    switch (c)
                    {
                        case '-':
                            s.push_back(c);
                            state = hexfloat_parse_state::expect_0;
                            break;
                        case '0':
                            state = hexfloat_parse_state::expect_x;
                            break;
                        default:
                        {
                            ec = cbor_errc::invalid_bigfloat;
                            return false;
                        }
                    }
                    break;
                }
                case hexfloat_parse_state::expect_0:
                {
                    switch (c)
                    {
                        case '0':
                            state = hexfloat_parse_state::expect_x;
                            break;
                        default:
                        {
                            ec = cbor_errc::invalid_bigfloat;
                            return false;
                        }
                    }
                    break;
                }
                case hexfloat_parse_state::expect_x:
                {
                    switch (c)
                    {
                        case 'x':
                        case 'X':
                            state = hexfloat_parse_state::integer;
                            break;
                        default:
                        {
                            ec = cbor_errc::invalid_bigfloat;
                            return false;
                        }
                    }
                    break;
                }
                case hexfloat_parse_state::integer:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':case 'a':case 'b':case 'c':case 'd':case 'e':case 'f':case 'A':case 'B':case 'C':case 'D':case 'E':case 'F':
                            s.push_back(c);
                            break;
                        case 'p': case 'P':
                            state = hexfloat_parse_state::exp1;
                            break;
                        case '.':
                            state = hexfloat_parse_state::fraction1;
                            break;
                        default:
                        {
                            ec = cbor_errc::invalid_bigfloat;
                            return false;
                        }
                    }
                    break;
                }
                case hexfloat_parse_state::exp1:
                {
                    switch (c)
                    {
                        case '+':
                            state = hexfloat_parse_state::exp2;
                            break;
                        case '-':
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':case 'a':case 'b':case 'c':case 'd':case 'e':case 'f':case 'A':case 'B':case 'C':case 'D':case 'E':case 'F':
                            exponent.push_back(c);
                            state = hexfloat_parse_state::exp2;
                            break;
                        default:
                        {
                            ec = cbor_errc::invalid_bigfloat;
                            return false;
                        }
                    }
                    break;
                }
                case hexfloat_parse_state::exp2:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':case 'a':case 'b':case 'c':case 'd':case 'e':case 'f':case 'A':case 'B':case 'C':case 'D':case 'E':case 'F':
                            exponent.push_back(c);
                            break;
                        default:
                        {
                            ec = cbor_errc::invalid_bigfloat;
                            return false;
                        }
                    }
                    break;
                }
                case hexfloat_parse_state::fraction1:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':case 'a':case 'b':case 'c':case 'd':case 'e':case 'f':case 'A':case 'B':case 'C':case 'D':case 'E':case 'F':
                            s.push_back(c);
                            scale -= 4;
                            break;
                        default:
                        {
                            ec = cbor_errc::invalid_bigfloat;
                            return false;
                        }
                    }
                    break;
                }
            }
        }

        write_tag(5);
        more = visit_begin_array((std::size_t)2, semantic_tag::none, context, ec);
        if (!more) return more;

        if (exponent.length() > 0)
        {
            int64_t val{ 0 };
            auto r = jsoncons::detail::hex_to_integer(exponent.data(), exponent.length(), val);
            if (!r)
            {
                ec = r.error_code();
                return false;
            }
            scale += val;
        }
        more = visit_int64(scale, semantic_tag::none, context, ec);
        if (!more) return more;

        int64_t val{ 0 };
        auto r = jsoncons::detail::hex_to_integer(s.data(),s.length(), val);
        if (r)
        {
            more = visit_int64(val, semantic_tag::none, context, ec);
            if (!more) return more;
        }
        else if (r.error_code() == jsoncons::detail::to_integer_errc::overflow)
        {
            bigint n = bigint::from_string_radix(s.data(), s.length(), 16);
            write_bignum(n);
            end_value();
        }
        else
        {
            JSONCONS_THROW(json_runtime_error<std::invalid_argument>(r.error_code().message()));
        }
        return visit_end_array(context, ec);
    }

    bool visit_string(const string_view_type& sv, semantic_tag tag, const ser_context& context, std::error_code& ec) override
    {
        switch (tag)
        {
            case semantic_tag::bigint:
            {
                bigint n = bigint::from_string(sv.data(), sv.length());
                write_bignum(n);
                end_value();
                break;
            }
            case semantic_tag::bigdec:
            {
                return write_decimal_value(sv, context, ec);
            }
            case semantic_tag::bigfloat:
            {
                return write_hexfloat_value(sv, context, ec);
            }
            case semantic_tag::datetime:
            {
                write_tag(0);

                write_string(sv);
                end_value();
                break;
            }
            case semantic_tag::uri:
            {
                write_tag(32);
                write_string(sv);
                end_value();
                break;
            }
            case semantic_tag::base64url:
            {
                write_tag(33);
                write_string(sv);
                end_value();
                break;
            }
            case semantic_tag::base64:
            {
                write_tag(34);
                write_string(sv);
                end_value();
                break;
            }
            default:
            {
                write_string(sv);
                end_value();
                break;
            }
        }
        return true;
    }

    bool visit_byte_string(const byte_string_view& b, 
                           semantic_tag tag, 
                           const ser_context&,
                           std::error_code&) override
    {
        byte_string_chars_format encoding_hint;
        switch (tag)
        {
            case semantic_tag::base16:
                encoding_hint = byte_string_chars_format::base16;
                break;
            case semantic_tag::base64:
                encoding_hint = byte_string_chars_format::base64;
                break;
            case semantic_tag::base64url:
                encoding_hint = byte_string_chars_format::base64url;
                break;
            default:
                encoding_hint = byte_string_chars_format::none;
                break;
        }
        switch (encoding_hint)
        {
            case byte_string_chars_format::base64url:
                write_tag(21);
                break;
            case byte_string_chars_format::base64:
                write_tag(22);
                break;
            case byte_string_chars_format::base16:
                write_tag(23);
                break;
            default:
                break;
        }
        if (options_.pack_strings() && b.size() >= jsoncons::cbor::detail::min_length_for_stringref(next_stringref_))
        {
            byte_string_type bs(b.data(), b.size(), alloc_);
            auto it = bytestringref_map_.find(bs);
            if (it == bytestringref_map_.end())
            {
                bytestringref_map_.emplace(std::make_pair(bs, next_stringref_++));
                write_byte_string(bs);
            }
            else
            {
                write_tag(25);
                write_uint64_value((*it).second);
            }
        }
        else
        {
            write_byte_string(b);
        }

        end_value();
        return true;
    }

    bool visit_byte_string(const byte_string_view& b, 
                           uint64_t ext_tag, 
                           const ser_context&,
                           std::error_code&) override
    {
        if (options_.pack_strings() && b.size() >= jsoncons::cbor::detail::min_length_for_stringref(next_stringref_))
        {
            byte_string_type bs(b.data(), b.size(), alloc_);
            auto it = bytestringref_map_.find(bs);
            if (it == bytestringref_map_.end())
            {
                bytestringref_map_.emplace(std::make_pair(bs, next_stringref_++));
                write_tag(ext_tag);
                write_byte_string(bs);
            }
            else
            {
                write_tag(25);
                write_uint64_value((*it).second);
            }
        }
        else
        {
            write_tag(ext_tag);
            write_byte_string(b);
        }

        end_value();
        return true;
    }

    void write_byte_string(const byte_string_view& b) 
    {
        if (b.size() <= 0x17)
        {
            // fixstr stores a byte array whose length is upto 31 bytes
            binary::native_to_big(static_cast<uint8_t>(0x40 + b.size()), 
                                            std::back_inserter(sink_));
        }
        else if (b.size() <= 0xff)
        {
            binary::native_to_big(static_cast<uint8_t>(0x58), 
                                            std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint8_t>(b.size()), 
                                            std::back_inserter(sink_));
        }
        else if (b.size() <= 0xffff)
        {
            binary::native_to_big(static_cast<uint8_t>(0x59), 
                                            std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint16_t>(b.size()), 
                                            std::back_inserter(sink_));
        }
        else if (b.size() <= 0xffffffff)
        {
            binary::native_to_big(static_cast<uint8_t>(0x5a), 
                                            std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint32_t>(b.size()), 
                                            std::back_inserter(sink_));
        }
        else // if (b.size() <= 0xffffffffffffffff)
        {
            binary::native_to_big(static_cast<uint8_t>(0x5b), 
                                            std::back_inserter(sink_));
            binary::native_to_big(static_cast<uint64_t>(b.size()), 
                                            std::back_inserter(sink_));
        }

        for (auto c : b)
        {
            sink_.push_back(c);
        }
    }

    bool visit_double(double val, 
                      semantic_tag tag,
                      const ser_context&,
                      std::error_code&) override
    {
        switch (tag)
        {
            case semantic_tag::epoch_second:
                write_tag(1);
                break;
            case semantic_tag::epoch_milli:
                write_tag(1);
                if (val != 0)
                {
                    val /= millis_in_second;
                }
                break;
            case semantic_tag::epoch_nano:
                write_tag(1);
                if (val != 0)
                {
                    val /= nanos_in_second;
                }
                break;
            default:
                break;
        }

        float valf = (float)val;
        if ((double)valf == val)
        {
            binary::native_to_big(static_cast<uint8_t>(0xfa), 
                                  std::back_inserter(sink_));
            binary::native_to_big(valf, std::back_inserter(sink_));
        }
        else
        {
            binary::native_to_big(static_cast<uint8_t>(0xfb), 
                                  std::back_inserter(sink_));
            binary::native_to_big(val, std::back_inserter(sink_));
        }

        // write double

        end_value();
        return true;
    }

    bool visit_int64(int64_t value, 
                        semantic_tag tag, 
                        const ser_context& context,
                        std::error_code& ec) override
    {
        switch (tag)
        {
            case semantic_tag::epoch_milli:
            case semantic_tag::epoch_nano:
                return visit_double(static_cast<double>(value), tag, context, ec);
            case semantic_tag::epoch_second:
                write_tag(1);
                break;
            default:
                break;
        }
        write_int64_value(value);
        end_value();
        return true;
    }

    bool visit_uint64(uint64_t value, 
                      semantic_tag tag, 
                      const ser_context& context,
                      std::error_code& ec) override
    {
        switch (tag)
        {
            case semantic_tag::epoch_milli:
            case semantic_tag::epoch_nano:
                return visit_double(static_cast<double>(value), tag, context, ec);
            case semantic_tag::epoch_second:
                write_tag(1);
                break;
            default:
                break;
        }

        write_uint64_value(value);
        end_value();
        return true;
    }

    void write_tag(uint64_t value)
    {
        if (value <= 0x17)
        {
            sink_.push_back(0xc0 | static_cast<uint8_t>(value)); 
        } 
        else if (value <=(std::numeric_limits<uint8_t>::max)())
        {
            sink_.push_back(0xd8);
            sink_.push_back(static_cast<uint8_t>(value));
        } 
        else if (value <=(std::numeric_limits<uint16_t>::max)())
        {
            sink_.push_back(0xd9);
            binary::native_to_big(static_cast<uint16_t>(value), 
                                            std::back_inserter(sink_));
        }
        else if (value <=(std::numeric_limits<uint32_t>::max)())
        {
            sink_.push_back(0xda);
            binary::native_to_big(static_cast<uint32_t>(value), 
                                            std::back_inserter(sink_));
        }
        else 
        {
            sink_.push_back(0xdb);
            binary::native_to_big(static_cast<uint64_t>(value), 
                                            std::back_inserter(sink_));
        }
    }

    void write_uint64_value(uint64_t value) 
    {
        if (value <= 0x17)
        {
            sink_.push_back(static_cast<uint8_t>(value));
        } 
        else if (value <=(std::numeric_limits<uint8_t>::max)())
        {
            sink_.push_back(static_cast<uint8_t>(0x18));
            sink_.push_back(static_cast<uint8_t>(value));
        } 
        else if (value <=(std::numeric_limits<uint16_t>::max)())
        {
            sink_.push_back(static_cast<uint8_t>(0x19));
            binary::native_to_big(static_cast<uint16_t>(value), 
                                            std::back_inserter(sink_));
        } 
        else if (value <=(std::numeric_limits<uint32_t>::max)())
        {
            sink_.push_back(static_cast<uint8_t>(0x1a));
            binary::native_to_big(static_cast<uint32_t>(value), 
                                            std::back_inserter(sink_));
        } 
        else if (value <=(std::numeric_limits<uint64_t>::max)())
        {
            sink_.push_back(static_cast<uint8_t>(0x1b));
            binary::native_to_big(static_cast<uint64_t>(value), 
                                            std::back_inserter(sink_));
        }
    }

    void write_int64_value(int64_t value) 
    {
        if (value >= 0)
        {
            if (value <= 0x17)
            {
                binary::native_to_big(static_cast<uint8_t>(value), 
                                  std::back_inserter(sink_));
            } 
            else if (value <= (std::numeric_limits<uint8_t>::max)())
            {
                binary::native_to_big(static_cast<uint8_t>(0x18), 
                                  std::back_inserter(sink_));
                binary::native_to_big(static_cast<uint8_t>(value), 
                                  std::back_inserter(sink_));
            } 
            else if (value <= (std::numeric_limits<uint16_t>::max)())
            {
                binary::native_to_big(static_cast<uint8_t>(0x19), 
                                  std::back_inserter(sink_));
                binary::native_to_big(static_cast<uint16_t>(value), 
                                  std::back_inserter(sink_));
            } 
            else if (value <= (std::numeric_limits<uint32_t>::max)())
            {
                binary::native_to_big(static_cast<uint8_t>(0x1a), 
                                  std::back_inserter(sink_));
                binary::native_to_big(static_cast<uint32_t>(value), 
                                  std::back_inserter(sink_));
            } 
            else if (value <= (std::numeric_limits<int64_t>::max)())
            {
                binary::native_to_big(static_cast<uint8_t>(0x1b), 
                                  std::back_inserter(sink_));
                binary::native_to_big(static_cast<int64_t>(value), 
                                  std::back_inserter(sink_));
            }
        } else
        {
            const auto posnum = -1 - value;
            if (value >= -24)
            {
                binary::native_to_big(static_cast<uint8_t>(0x20 + posnum), 
                                  std::back_inserter(sink_));
            } 
            else if (posnum <= (std::numeric_limits<uint8_t>::max)())
            {
                binary::native_to_big(static_cast<uint8_t>(0x38), 
                                  std::back_inserter(sink_));
                binary::native_to_big(static_cast<uint8_t>(posnum), 
                                  std::back_inserter(sink_));
            } 
            else if (posnum <= (std::numeric_limits<uint16_t>::max)())
            {
                binary::native_to_big(static_cast<uint8_t>(0x39), 
                                  std::back_inserter(sink_));
                binary::native_to_big(static_cast<uint16_t>(posnum), 
                                  std::back_inserter(sink_));
            } 
            else if (posnum <= (std::numeric_limits<uint32_t>::max)())
            {
                binary::native_to_big(static_cast<uint8_t>(0x3a), 
                                  std::back_inserter(sink_));
                binary::native_to_big(static_cast<uint32_t>(posnum), 
                                  std::back_inserter(sink_));
            } 
            else if (posnum <= (std::numeric_limits<int64_t>::max)())
            {
                binary::native_to_big(static_cast<uint8_t>(0x3b), 
                                  std::back_inserter(sink_));
                binary::native_to_big(static_cast<int64_t>(posnum), 
                                  std::back_inserter(sink_));
            }
        }
    }

    bool visit_bool(bool value, semantic_tag, const ser_context&, std::error_code&) override
    {
        if (value)
        {
            sink_.push_back(0xf5);
        }
        else
        {
            sink_.push_back(0xf4);
        }

        end_value();
        return true;
    }

    bool visit_typed_array(const jsoncons::span<const uint8_t>& v, 
                        semantic_tag tag,
                        const ser_context& context, 
                        std::error_code& ec) override
    {
        if (options_.use_typed_arrays())
        {
            switch (tag)
            {
                case semantic_tag::clamped:
                    write_tag(0x44);
                    break;
                default:
                    write_tag(0x40);
                    break;
            }
            write_byte_string(byte_string_view(v));
            return true;
        }
        else
        {
            bool more = this->begin_array(v.size(), semantic_tag::none, context, ec);
            for (auto p = v.begin(); more && p != v.end(); ++p)
            {
                more = this->uint64_value(*p, tag, context, ec);
            }
            if (more)
            {
                more = this->end_array(context, ec);
            }
            return more;
        }
    }

    bool visit_typed_array(const jsoncons::span<const uint16_t>& data,  
                        semantic_tag tag,
                        const ser_context& context, 
                        std::error_code& ec) override
    {
        if (options_.use_typed_arrays())
        {
            write_typed_array_tag(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>(), 
                                  uint16_t(), 
                                  tag);
            std::vector<uint8_t> v(data.size()*sizeof(uint16_t));
            std::memcpy(v.data(),data.data(),data.size()*sizeof(uint16_t));
            write_byte_string(byte_string_view(v));
            return true;
        }
        else
        {
            bool more = this->begin_array(data.size(), semantic_tag::none, context, ec);
            for (auto p = data.begin(); more && p != data.end(); ++p)
            {
                more = this->uint64_value(*p, tag, context, ec);
            }
            if (more)
            {
                more = this->end_array(context, ec);
            }
            return more;
        }
    }

    bool visit_typed_array(const jsoncons::span<const uint32_t>& data,  
                        semantic_tag tag,
                        const ser_context& context, 
                        std::error_code& ec) override
    {
        if (options_.use_typed_arrays())
        {
            write_typed_array_tag(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>(), 
                                  uint32_t(), 
                                  tag);
            std::vector<uint8_t> v(data.size()*sizeof(uint32_t));
            std::memcpy(v.data(), data.data(), data.size()*sizeof(uint32_t));
            write_byte_string(byte_string_view(v));
            return true;
        }
        else
        {
            bool more = this->begin_array(data.size(), semantic_tag::none, context, ec);
            for (auto p = data.begin(); more && p != data.end(); ++p)
            {
                more = this->uint64_value(*p, semantic_tag::none, context, ec);
            }
            if (more)
            {
                more = this->end_array(context, ec);
            }
            return more;
        }
    }

    bool visit_typed_array(const jsoncons::span<const uint64_t>& data,  
                        semantic_tag tag,
                        const ser_context& context, 
                        std::error_code& ec) override
    {
        if (options_.use_typed_arrays())
        {
            write_typed_array_tag(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>(), 
                                  uint64_t(), 
                                  tag);
            std::vector<uint8_t> v(data.size()*sizeof(uint64_t));
            std::memcpy(v.data(), data.data(), data.size()*sizeof(uint64_t));
            write_byte_string(byte_string_view(v));
            return true;
        }
        else
        {
            bool more = this->begin_array(data.size(), semantic_tag::none, context, ec);
            for (auto p = data.begin(); more && p != data.end(); ++p)
            {
                more = this->uint64_value(*p,semantic_tag::none,context, ec);
            }
            if (more)
            {
                more = this->end_array(context, ec);
            }
            return more;
        }
    }

    bool visit_typed_array(const jsoncons::span<const int8_t>& data,  
                        semantic_tag,
                        const ser_context& context, 
                        std::error_code& ec) override
    {
        if (options_.use_typed_arrays())
        {
            write_tag(0x48);
            std::vector<uint8_t> v(data.size()*sizeof(int8_t));
            std::memcpy(v.data(), data.data(), data.size()*sizeof(int8_t));
            write_byte_string(byte_string_view(v));
            return true;
        }
        else
        {
            bool more = this->begin_array(data.size(), semantic_tag::none,context, ec);
            for (auto p = data.begin(); more && p != data.end(); ++p)
            {
                more = this->int64_value(*p,semantic_tag::none,context, ec);
            }
            if (more)
            {
                more = this->end_array(context, ec);
            }
            return more;
        }
    }

    bool visit_typed_array(const jsoncons::span<const int16_t>& data,  
                        semantic_tag tag,
                        const ser_context& context, 
                        std::error_code& ec) override
    {
        if (options_.use_typed_arrays())
        {
            write_typed_array_tag(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>(), 
                                  int16_t(), 
                                  tag);
            std::vector<uint8_t> v(data.size()*sizeof(int16_t));
            std::memcpy(v.data(), data.data(), data.size()*sizeof(int16_t));
            write_byte_string(byte_string_view(v));
            return true;
        }
        else
        {
            bool more = this->begin_array(data.size(), semantic_tag::none,context, ec);
            for (auto p = data.begin(); more && p != data.end(); ++p)
            {
                more = this->int64_value(*p,semantic_tag::none,context, ec);
            }
            if (more)
            {
                more = this->end_array(context, ec);
            }
            return more;
        }
    }

    bool visit_typed_array(const jsoncons::span<const int32_t>& data,  
                        semantic_tag tag,
                        const ser_context& context, 
                        std::error_code& ec) override
    {
        if (options_.use_typed_arrays())
        {
            write_typed_array_tag(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>(), 
                                  int32_t(), 
                                  tag);
            std::vector<uint8_t> v(data.size()*sizeof(int32_t));
            std::memcpy(v.data(), data.data(), data.size()*sizeof(int32_t));
            write_byte_string(byte_string_view(v));
            return true;
        }
        else
        {
            bool more = this->begin_array(data.size(), semantic_tag::none,context, ec);
            for (auto p = data.begin(); more && p != data.end(); ++p)
            {
                more = this->int64_value(*p,semantic_tag::none,context, ec);
            }
            if (more)
            {
                more = this->end_array(context, ec);
            }
            return more;
        }
    }

    bool visit_typed_array(const jsoncons::span<const int64_t>& data,  
                        semantic_tag tag,
                        const ser_context& context, 
                        std::error_code& ec) override
    {
        if (options_.use_typed_arrays())
        {
            write_typed_array_tag(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>(), 
                                  int64_t(), 
                                  tag);
            std::vector<uint8_t> v(data.size()*sizeof(int64_t));
            std::memcpy(v.data(), data.data(), data.size()*sizeof(int64_t));
            write_byte_string(byte_string_view(v));
            return true;
        }
        else
        {
            bool more = this->begin_array(data.size(), semantic_tag::none,context, ec);
            for (auto p = data.begin(); more && p != data.end(); ++p)
            {
                more = this->int64_value(*p,semantic_tag::none,context, ec);
            }
            if (more)
            {
                more = this->end_array(context, ec);
            }
            return more;
        }
    }

    bool visit_typed_array(half_arg_t, const jsoncons::span<const uint16_t>& data,  
                        semantic_tag tag,
                        const ser_context& context, 
                        std::error_code& ec) override
    {
        if (options_.use_typed_arrays())
        {
            write_typed_array_tag(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>(), 
                                  half_arg, 
                                  tag);
            std::vector<uint8_t> v(data.size()*sizeof(uint16_t));
            std::memcpy(v.data(),data.data(),data.size()*sizeof(uint16_t));
            write_byte_string(byte_string_view(v));
            return true;
        }
        else
        {
            bool more = this->begin_array(data.size(), semantic_tag::none, context, ec);
            for (auto p = data.begin(); more && p != data.end(); ++p)
            {
                more = this->half_value(*p, tag, context, ec);
            }
            if (more)
            {
                more = this->end_array(context, ec);
            }
            return more;
        }
    }

    bool visit_typed_array(const jsoncons::span<const float>& data,  
                        semantic_tag tag,
                        const ser_context& context, 
                        std::error_code& ec) override
    {
        if (options_.use_typed_arrays())
        {
            write_typed_array_tag(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>(), 
                                  float(), 
                                  tag);
            std::vector<uint8_t> v(data.size()*sizeof(float));
            std::memcpy(v.data(), data.data(), data.size()*sizeof(float));
            write_byte_string(byte_string_view(v));
            return true;
        }
        else
        {
            bool more = this->begin_array(data.size(), semantic_tag::none,context, ec);
            for (const auto* p = data.begin(); more && p != data.end(); ++p)
            {
                more = this->double_value(*p,semantic_tag::none,context, ec);
            }
            if (more)
            {
                more = this->end_array(context, ec);
            }
            return more;
        }
    }

    bool visit_typed_array(const jsoncons::span<const double>& data,  
                        semantic_tag tag,
                        const ser_context& context, 
                        std::error_code& ec) override
    {
        if (options_.use_typed_arrays())
        {
            write_typed_array_tag(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>(), 
                                  double(), 
                                  tag);
            std::vector<uint8_t> v(data.size()*sizeof(double));
            std::memcpy(v.data(), data.data(), data.size()*sizeof(double));
            write_byte_string(byte_string_view(v));
            return true;
        }
        
        bool more = this->begin_array(data.size(), semantic_tag::none,context, ec);
        for (auto p = data.begin(); more && p != data.end(); ++p)
        {
            more = this->double_value(*p,semantic_tag::none,context, ec);
        }
        if (more)
        {
            more = this->end_array(context, ec);
        }
        return more;
    }
/*
    bool visit_typed_array(const jsoncons::span<const float128_type>&, 
                        semantic_tag,
                        const ser_context&, 
                        std::error_code&) override
    {
        return true;
    }
*/
    bool visit_begin_multi_dim(const jsoncons::span<const size_t>& shape,
                            semantic_tag tag,
                            const ser_context& context, 
                            std::error_code& ec) override
    {
        switch (tag)
        {
            case semantic_tag::multi_dim_column_major:
                write_tag(1040);
                break;
            default:
                write_tag(40);
                break;
        }
        bool more = visit_begin_array(2, semantic_tag::none, context, ec);
        if (more)
            more = visit_begin_array(shape.size(), semantic_tag::none, context, ec);
        for (auto it = shape.begin(); more && it != shape.end(); ++it)
        {
            more = visit_uint64(*it, semantic_tag::none, context, ec);
        }
        if (more)
        {
            more = visit_end_array(context, ec);
        }
        return more;
    }

    bool visit_end_multi_dim(const ser_context& context,
                          std::error_code& ec) override
    {
        bool more = visit_end_array(context, ec);
        return more;
    }

    void write_typed_array_tag(std::true_type, 
                               uint16_t,
                               semantic_tag)
    {
        write_tag(0x41); // big endian
    }
    void write_typed_array_tag(std::false_type,
                               uint16_t,
                               semantic_tag)
    {
        write_tag(0x45);
    }

    void write_typed_array_tag(std::true_type, 
                               uint32_t,
                               semantic_tag)
    {
        write_tag(0x42); // big endian
    }
    void write_typed_array_tag(std::false_type,
                               uint32_t,
                               semantic_tag)
    {
        write_tag(0x46);  // little endian
    }

    void write_typed_array_tag(std::true_type, 
                               uint64_t,
                               semantic_tag)
    {
        write_tag(0x43); // big endian
    }
    void write_typed_array_tag(std::false_type,
                               uint64_t,
                               semantic_tag)
    {
        write_tag(0x47);  // little endian
    }

    void write_typed_array_tag(std::true_type, 
                               int16_t,
                               semantic_tag)
    {
        write_tag(0x49); // big endian
    }
    void write_typed_array_tag(std::false_type,
                               int16_t,
                               semantic_tag)
    {
        write_tag(0x4d);  // little endian
    }

    void write_typed_array_tag(std::true_type, 
                               int32_t,
                               semantic_tag)
    {
        write_tag(0x4a); // big endian
    }
    void write_typed_array_tag(std::false_type,
                               int32_t,
                               semantic_tag)
    {
        write_tag(0x4e);  // little endian
    }

    void write_typed_array_tag(std::true_type, 
                               int64_t,
                               semantic_tag)
    {
        write_tag(0x4b); // big endian
    }
    void write_typed_array_tag(std::false_type,
                               int64_t,
                               semantic_tag)
    {
        write_tag(0x4f);  // little endian
    }

    void write_typed_array_tag(std::true_type, 
                               half_arg_t,
                               semantic_tag)
    {
        write_tag(0x50);
    }
    void write_typed_array_tag(std::false_type,
                               half_arg_t,
                               semantic_tag)
    {
        write_tag(0x54);
    }
                        
    void write_typed_array_tag(std::true_type, 
                               float,
                               semantic_tag)
    {
        write_tag(0x51); // big endian
    }
    void write_typed_array_tag(std::false_type,
                               float,
                               semantic_tag)
    {
        write_tag(0x55);  // little endian
    }

    void write_typed_array_tag(std::true_type, 
                               double,
                               semantic_tag)
    {
        write_tag(0x52); // big endian
    }
    void write_typed_array_tag(std::false_type,
                               double,
                               semantic_tag)
    {
        write_tag(0x56);  // little endian
    }

    void end_value()
    {
        if (!stack_.empty())
        {
            ++stack_.back().index_;
        }
    }
};

using cbor_stream_encoder = basic_cbor_encoder<jsoncons::binary_stream_sink>;
using cbor_bytes_encoder = basic_cbor_encoder<jsoncons::bytes_sink<std::vector<uint8_t>>>;

} // namespace cbor
} // namespace jsoncons

#endif
