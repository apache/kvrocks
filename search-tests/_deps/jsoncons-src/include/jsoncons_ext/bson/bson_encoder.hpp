// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_BSON_BSON_ENCODER_HPP
#define JSONCONS_EXT_BSON_BSON_ENCODER_HPP

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits> // std::numeric_limits
#include <memory>
#include <string>
#include <system_error>
#include <utility> // std::move
#include <vector>

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/utility/byte_string.hpp>
#include <jsoncons/json_visitor.hpp>
#include <jsoncons/ser_context.hpp>
#include <jsoncons/sink.hpp>
#include <jsoncons/tag_type.hpp>
#include <jsoncons/utility/binary.hpp>
#include <jsoncons/utility/unicode_traits.hpp>

#include <jsoncons_ext/bson/bson_decimal128.hpp>
#include <jsoncons_ext/bson/bson_error.hpp>
#include <jsoncons_ext/bson/bson_oid.hpp>
#include <jsoncons_ext/bson/bson_options.hpp>
#include <jsoncons_ext/bson/bson_type.hpp>

namespace jsoncons { namespace bson {

template <typename Sink=jsoncons::binary_stream_sink,typename Allocator=std::allocator<char>>
class basic_bson_encoder final : public basic_json_visitor<char>
{
    enum class decimal_parse_state { start, integer, exp1, exp2, fraction1 };
    static constexpr int64_t nanos_in_milli = 1000000;
    static constexpr int64_t nanos_in_second = 1000000000;
    static constexpr int64_t millis_in_second = 1000;
public:
    using allocator_type = Allocator;
    using char_type = char;
    using typename basic_json_visitor<char>::string_view_type;
    using sink_type = Sink;

private:
    struct stack_item
    {
        jsoncons::bson::bson_container_type type_;
        std::size_t offset_{0};
        std::size_t name_offset_{0};
        std::size_t index_{0};

        stack_item(jsoncons::bson::bson_container_type type, std::size_t offset) noexcept
           : type_(type), offset_(offset)
        {
        }

        std::size_t offset() const
        {
            return offset_;
        }

        std::size_t member_offset() const
        {
            return name_offset_;
        }

        void member_offset(std::size_t offset) 
        {
            name_offset_ = offset;
        }

        std::size_t next_index()
        {
            return index_++;
        }

        bool is_object() const
        {
            return type_ == jsoncons::bson::bson_container_type::document;
        }


    };

    sink_type sink_;
    const bson_encode_options options_;
    allocator_type alloc_;

    std::vector<stack_item> stack_;
    std::vector<uint8_t> buffer_;
    int nesting_depth_;
public:

    // Noncopyable and nonmoveable
    basic_bson_encoder(const basic_bson_encoder&) = delete;
    basic_bson_encoder(basic_bson_encoder&&) = delete;

    explicit basic_bson_encoder(Sink&& sink, 
                                const Allocator& alloc = Allocator())
       : basic_bson_encoder(std::forward<Sink>(sink),
                            bson_encode_options(),
                            alloc)
    {
    }

    explicit basic_bson_encoder(Sink&& sink, 
                                const bson_encode_options& options, 
                                const Allocator& alloc = Allocator())
       : sink_(std::forward<Sink>(sink)),
         options_(options),
         alloc_(alloc), 
         nesting_depth_(0)
    {
    }

    ~basic_bson_encoder() noexcept
    {
        sink_.flush();
    }

    basic_bson_encoder& operator=(const basic_bson_encoder&) = delete;
    basic_bson_encoder& operator=(basic_bson_encoder&&) = delete;

    void reset()
    {
        stack_.clear();
        buffer_.clear();
        nesting_depth_ = 0;
    }

    void reset(Sink&& sink)
    {
        sink_ = std::move(sink);
        reset();
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
            ec = bson_errc::max_nesting_depth_exceeded;
            return false;
        } 
        if (buffer_.size() > 0)
        {
            if (stack_.empty())
            {
                ec = bson_errc::expected_bson_document;
                return false;
            }
            before_value(jsoncons::bson::bson_type::document_type);
        }

        stack_.emplace_back(jsoncons::bson::bson_container_type::document, buffer_.size());
        buffer_.insert(buffer_.end(), sizeof(int32_t), 0);

        return true;
    }

    bool visit_end_object(const ser_context&, std::error_code&) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        --nesting_depth_;

        buffer_.push_back(0x00);

        std::size_t length = buffer_.size() - stack_.back().offset();
        binary::native_to_little(static_cast<uint32_t>(length), buffer_.begin()+stack_.back().offset());

        stack_.pop_back();
        if (stack_.empty())
        {
            for (auto c : buffer_)
            {
                sink_.push_back(c);
            }
        }
        return true;
    }

    bool visit_begin_array(semantic_tag, const ser_context&, std::error_code& ec) override
    {
        if (JSONCONS_UNLIKELY(++nesting_depth_ > options_.max_nesting_depth()))
        {
            ec = bson_errc::max_nesting_depth_exceeded;
            return false;
        } 
        if (buffer_.size() > 0)
        {
            if (stack_.empty())
            {
                ec = bson_errc::expected_bson_document;
                return false;
            }
            before_value(jsoncons::bson::bson_type::array_type);
        }
        stack_.emplace_back(jsoncons::bson::bson_container_type::array, buffer_.size());
        buffer_.insert(buffer_.end(), sizeof(int32_t), 0);
        return true;
    }

    bool visit_end_array(const ser_context&, std::error_code&) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        --nesting_depth_;

        buffer_.push_back(0x00);

        std::size_t length = buffer_.size() - stack_.back().offset();
        binary::native_to_little(static_cast<uint32_t>(length), buffer_.begin()+stack_.back().offset());

        stack_.pop_back();
        if (stack_.empty())
        {
            for (auto c : buffer_)
            {
                sink_.push_back(c);
            }
        }
        return true;
    }

    bool visit_key(const string_view_type& name, const ser_context&, std::error_code&) override
    {
        stack_.back().member_offset(buffer_.size());
        buffer_.push_back(0x00); // reserve space for code
        for (auto c : name)
        {
            buffer_.push_back(c);
        }
        buffer_.push_back(0x00);
        return true;
    }

    bool visit_null(semantic_tag tag, const ser_context&, std::error_code& ec) override
    {
        if (stack_.empty())
        {
            ec = bson_errc::expected_bson_document;
            return false;
        }
        switch (tag)
        {
            case semantic_tag::undefined:
                before_value(jsoncons::bson::bson_type::undefined_type);
                break;
            default:
                before_value(jsoncons::bson::bson_type::null_type);
                break;
        }
        return true;
    }

    bool visit_bool(bool val, semantic_tag, const ser_context&, std::error_code& ec) override
    {
        if (stack_.empty())
        {
            ec = bson_errc::expected_bson_document;
            return false;
        }
        before_value(jsoncons::bson::bson_type::bool_type);
        if (val)
        {
            buffer_.push_back(0x01);
        }
        else
        {
            buffer_.push_back(0x00);
        }

        return true;
    }

    bool visit_string(const string_view_type& sv, semantic_tag tag, const ser_context&, std::error_code& ec) override
    {
        if (stack_.empty())
        {
            ec = bson_errc::expected_bson_document;
            return false;
        }

        switch (tag)
        {
            case semantic_tag::float128:
            {
                before_value(jsoncons::bson::bson_type::decimal128_type);
                decimal128_t dec;
                auto rc = decimal128_from_chars(sv.data(), sv.data()+sv.size(), dec);
                if (rc.ec != std::errc())
                {
                    ec = bson_errc::invalid_decimal128_string;
                    return false;
                }
                binary::native_to_little(dec.low,std::back_inserter(buffer_));
                binary::native_to_little(dec.high,std::back_inserter(buffer_));
                break;
            }
            case semantic_tag::id:
            {
                before_value(jsoncons::bson::bson_type::object_id_type);
                oid_t oid(sv);
                for (auto b : oid)
                {
                    buffer_.push_back(b);
                }
                break;
            }
            case semantic_tag::regex:
            {
                before_value(jsoncons::bson::bson_type::regex_type);
                std::size_t first = sv.find_first_of('/');
                std::size_t last = sv.find_last_of('/');
                if (first == string_view::npos || last == string_view::npos || first == last)
                {
                    ec = bson_errc::invalid_regex_string;
                    return false;
                }
                string_view regex = sv.substr(first+1,last-1);
                for (auto c : regex)
                {
                    buffer_.push_back(c);
                }
                buffer_.push_back(0x00);
                string_view options = sv.substr(last+1);
                for (auto c : options)
                {
                    buffer_.push_back(c);
                }
                buffer_.push_back(0x00);
                break;
            }
            default:
                switch (tag)
                {
                    case semantic_tag::code:
                        before_value(jsoncons::bson::bson_type::javascript_type);
                        break;
                    default:
                        before_value(jsoncons::bson::bson_type::string_type);
                        break;
                }
                std::size_t offset = buffer_.size();
                buffer_.insert(buffer_.end(), sizeof(int32_t), 0);
                std::size_t string_offset = buffer_.size();
                auto sink = unicode_traits::validate(sv.data(), sv.size());
                if (sink.ec != unicode_traits::conv_errc())
                {
                    ec = bson_errc::invalid_utf8_text_string;
                    return false;
                }
                for (auto c : sv)
                {
                    buffer_.push_back(c);
                }
                buffer_.push_back(0x00);
                std::size_t length = buffer_.size() - string_offset;
                binary::native_to_little(static_cast<uint32_t>(length), buffer_.begin()+offset);
                break;
        }

        return true;
    }

    bool visit_byte_string(const byte_string_view& b, 
                           semantic_tag, 
                           const ser_context&,
                           std::error_code& ec) override
    {
        if (stack_.empty())
        {
            ec = bson_errc::expected_bson_document;
            return false;
        }
        before_value(jsoncons::bson::bson_type::binary_type);

        std::size_t offset = buffer_.size();
        buffer_.insert(buffer_.end(), sizeof(int32_t), 0);
        std::size_t string_offset = buffer_.size();

        buffer_.push_back(0x80); // default subtype

        for (auto c : b)
        {
            buffer_.push_back(c);
        }
        std::size_t length = buffer_.size() - string_offset - 1;
        binary::native_to_little(static_cast<uint32_t>(length), buffer_.begin()+offset);

        return true;
    }

    bool visit_byte_string(const byte_string_view& b, 
                           uint64_t ext_tag, 
                           const ser_context&,
                           std::error_code& ec) override
    {
        if (stack_.empty())
        {
            ec = bson_errc::expected_bson_document;
            return false;
        }
        before_value(jsoncons::bson::bson_type::binary_type);

        std::size_t offset = buffer_.size();
        buffer_.insert(buffer_.end(), sizeof(int32_t), 0);
        std::size_t string_offset = buffer_.size();

        buffer_.push_back(static_cast<uint8_t>(ext_tag)); // default subtype

        for (auto c : b)
        {
            buffer_.push_back(c);
        }
        std::size_t length = buffer_.size() - string_offset - 1;
        binary::native_to_little(static_cast<uint32_t>(length), buffer_.begin()+offset);

        return true;
    }

    bool visit_int64(int64_t val, 
                     semantic_tag tag, 
                     const ser_context&,
                     std::error_code& ec) override
    {
        static constexpr int64_t min_value_div_1000 = (std::numeric_limits<int64_t>::min)() / 1000;
        static constexpr int64_t max_value_div_1000 = (std::numeric_limits<int64_t>::max)() / 1000;
        if (stack_.empty())
        {
            ec = bson_errc::expected_bson_document;
            return false;
        }

        switch (tag)
        {
            case semantic_tag::epoch_second:
                if (val < min_value_div_1000)
                {
                    ec = bson_errc::datetime_too_small;
                    return false;
                }
                if (val > max_value_div_1000)
                {
                    ec = bson_errc::datetime_too_large;
                    return false;
                }
                before_value(jsoncons::bson::bson_type::datetime_type);
                binary::native_to_little(val*millis_in_second,std::back_inserter(buffer_));
                return true;
            case semantic_tag::epoch_milli:
                before_value(jsoncons::bson::bson_type::datetime_type);
                binary::native_to_little(val,std::back_inserter(buffer_));
                return true;
            case semantic_tag::epoch_nano:
                before_value(jsoncons::bson::bson_type::datetime_type);
                if (val != 0)
                {
                    val /= nanos_in_milli;
                }
                binary::native_to_little(static_cast<int64_t>(val),std::back_inserter(buffer_));
                return true;
            default:
            {
                if (val >= (std::numeric_limits<int32_t>::lowest)() && val <= (std::numeric_limits<int32_t>::max)())
                {
                    before_value(jsoncons::bson::bson_type::int32_type);
                    binary::native_to_little(static_cast<uint32_t>(val),std::back_inserter(buffer_));
                }
                else 
                {
                    before_value(jsoncons::bson::bson_type::int64_type);
                    binary::native_to_little(static_cast<int64_t>(val),std::back_inserter(buffer_));
                }
                return true;
            }
        }
    }

    bool visit_uint64(uint64_t val, 
                      semantic_tag tag, 
                      const ser_context&,
                      std::error_code& ec) override
    {
        static constexpr uint64_t max_value_div_1000 = (std::numeric_limits<uint64_t>::max)() / 1000;
        if (stack_.empty())
        {
            ec = bson_errc::expected_bson_document;
            return false;
        }

        switch (tag)
        {
            case semantic_tag::epoch_second:
                if (val > max_value_div_1000)
                {
                    ec = bson_errc::datetime_too_large;
                    return false;
                }
                before_value(jsoncons::bson::bson_type::datetime_type);
                binary::native_to_little(static_cast<int64_t>(val*millis_in_second),std::back_inserter(buffer_));
                return true;
            case semantic_tag::epoch_milli:
                before_value(jsoncons::bson::bson_type::datetime_type);
                binary::native_to_little(static_cast<int64_t>(val),std::back_inserter(buffer_));
                return true;
            case semantic_tag::epoch_nano:
                before_value(jsoncons::bson::bson_type::datetime_type);
                if (val != 0)
                {
                    val /= nanos_in_second;
                }
                binary::native_to_little(static_cast<int64_t>(val),std::back_inserter(buffer_));
                return true;
            default:
            {
                bool more;
                if (val <= static_cast<uint64_t>((std::numeric_limits<int32_t>::max)()))
                {
                    before_value(jsoncons::bson::bson_type::int32_type);
                    binary::native_to_little(static_cast<uint32_t>(val),std::back_inserter(buffer_));
                    more = true;
                }
                else if (val <= static_cast<uint64_t>((std::numeric_limits<int64_t>::max)()))
                {
                    before_value(jsoncons::bson::bson_type::int64_type);
                    binary::native_to_little(static_cast<uint64_t>(val),std::back_inserter(buffer_));
                    more = true;
                }
                else
                {
                    ec = bson_errc::number_too_large;
                    more = false;
                }
                return more;
            }
        }
    }

    bool visit_double(double val, 
                      semantic_tag,
                      const ser_context&,
                      std::error_code& ec) override
    {
        if (stack_.empty())
        {
            ec = bson_errc::expected_bson_document;
            return false;
        }
        before_value(jsoncons::bson::bson_type::double_type);
        binary::native_to_little(val,std::back_inserter(buffer_));
        return true;
    }

    void before_value(uint8_t code) 
    {
        JSONCONS_ASSERT(!stack_.empty());
        if (stack_.back().is_object())
        {
            buffer_[stack_.back().member_offset()] = code;
        }
        else
        {
            buffer_.push_back(code);
            std::string name = std::to_string(stack_.back().next_index());
            buffer_.insert(buffer_.end(), name.begin(), name.end());
            buffer_.push_back(0x00);
        }
    }
};

using bson_stream_encoder = basic_bson_encoder<jsoncons::binary_stream_sink>;
using bson_bytes_encoder = basic_bson_encoder<jsoncons::bytes_sink<std::vector<uint8_t>>>;

} // namespace bson
} // namespace jsoncons

#endif // JSONCONS_EXT_BSON_BSON_ENCODER_HPP
