// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_STAJ_EVENT_HPP
#define JSONCONS_STAJ_EVENT_HPP

#include <array> // std::array
#include <cstddef>
#include <cstdint>
#include <functional> // std::function
#include <ios>
#include <memory> // std::allocator
#include <system_error>
#include <type_traits> // std::enable_if

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/conv_error.hpp>
#include <jsoncons/detail/write_number.hpp>
#include <jsoncons/item_event_visitor.hpp>
#include <jsoncons/json_exception.hpp>
#include <jsoncons/json_parser.hpp>
#include <jsoncons/json_type_traits.hpp>
#include <jsoncons/json_visitor.hpp>
#include <jsoncons/ser_context.hpp>
#include <jsoncons/sink.hpp>
#include <jsoncons/tag_type.hpp>
#include <jsoncons/typed_array_view.hpp>
#include <jsoncons/utility/bigint.hpp>
#include <jsoncons/utility/extension_traits.hpp>

#include <jsoncons/value_converter.hpp>

namespace jsoncons {

enum class staj_event_type
{
    begin_array,
    end_array,
    begin_object,
    end_object,
    key,
    string_value,
    byte_string_value,
    null_value,
    bool_value,
    int64_value,
    uint64_value,
    half_value,
    double_value
};

template <typename CharT>
std::basic_ostream<CharT>& operator<<(std::basic_ostream<CharT>& os, staj_event_type tag)
{
    static constexpr const CharT* begin_array_name = JSONCONS_CSTRING_CONSTANT(CharT, "begin_array");
    static constexpr const CharT* end_array_name = JSONCONS_CSTRING_CONSTANT(CharT, "end_array");
    static constexpr const CharT* begin_object_name = JSONCONS_CSTRING_CONSTANT(CharT, "begin_object");
    static constexpr const CharT* end_object_name = JSONCONS_CSTRING_CONSTANT(CharT, "end_object");
    static constexpr const CharT* key_name = JSONCONS_CSTRING_CONSTANT(CharT, "key");
    static constexpr const CharT* string_value_name = JSONCONS_CSTRING_CONSTANT(CharT, "string_value");
    static constexpr const CharT* byte_string_value_name = JSONCONS_CSTRING_CONSTANT(CharT, "byte_string_value");
    static constexpr const CharT* null_value_name = JSONCONS_CSTRING_CONSTANT(CharT, "null_value");
    static constexpr const CharT* bool_value_name = JSONCONS_CSTRING_CONSTANT(CharT, "bool_value");
    static constexpr const CharT* uint64_value_name = JSONCONS_CSTRING_CONSTANT(CharT, "uint64_value");
    static constexpr const CharT* int64_value_name = JSONCONS_CSTRING_CONSTANT(CharT, "int64_value");
    static constexpr const CharT* half_value_name = JSONCONS_CSTRING_CONSTANT(CharT, "half_value");
    static constexpr const CharT* double_value_name = JSONCONS_CSTRING_CONSTANT(CharT, "double_value");

    switch (tag)
    {
        case staj_event_type::begin_array:
        {
            os << begin_array_name;
            break;
        }
        case staj_event_type::end_array:
        {
            os << end_array_name;
            break;
        }
        case staj_event_type::begin_object:
        {
            os << begin_object_name;
            break;
        }
        case staj_event_type::end_object:
        {
            os << end_object_name;
            break;
        }
        case staj_event_type::key:
        {
            os << key_name;
            break;
        }
        case staj_event_type::string_value:
        {
            os << string_value_name;
            break;
        }
        case staj_event_type::byte_string_value:
        {
            os << byte_string_value_name;
            break;
        }
        case staj_event_type::null_value:
        {
            os << null_value_name;
            break;
        }
        case staj_event_type::bool_value:
        {
            os << bool_value_name;
            break;
        }
        case staj_event_type::int64_value:
        {
            os << int64_value_name;
            break;
        }
        case staj_event_type::uint64_value:
        {
            os << uint64_value_name;
            break;
        }
        case staj_event_type::half_value:
        {
            os << half_value_name;
            break;
        }
        case staj_event_type::double_value:
        {
            os << double_value_name;
            break;
        }
    }
    return os;
}

template <typename CharT>
class basic_staj_event
{
    staj_event_type event_type_;
    semantic_tag tag_;
    uint64_t ext_tag_{0};
    union
    {
        bool bool_value_;
        int64_t int64_value_;
        uint64_t uint64_value_;
        uint16_t half_value_;
        double double_value_;
        const CharT* string_data_;
        const uint8_t* byte_string_data_;
    } value_;
    std::size_t length_{0};
public:
    using string_view_type = jsoncons::basic_string_view<CharT>;

    basic_staj_event(staj_event_type event_type, semantic_tag tag = semantic_tag::none)
        : event_type_(event_type), tag_(tag), value_()
    {
    }

    basic_staj_event(staj_event_type event_type, std::size_t length, semantic_tag tag = semantic_tag::none)
        : event_type_(event_type), tag_(tag), value_(), length_(length)
    {
    }

    basic_staj_event(null_type, semantic_tag tag)
        : event_type_(staj_event_type::null_value), tag_(tag), value_()
    {
    }

    basic_staj_event(bool value, semantic_tag tag)
        : event_type_(staj_event_type::bool_value), tag_(tag)
    {
        value_.bool_value_ = value;
    }

    basic_staj_event(int64_t value, semantic_tag tag)
        : event_type_(staj_event_type::int64_value), tag_(tag)
    {
        value_.int64_value_ = value;
    }

    basic_staj_event(uint64_t value, semantic_tag tag)
        : event_type_(staj_event_type::uint64_value), tag_(tag)
    {
        value_.uint64_value_ = value;
    }

    basic_staj_event(half_arg_t, uint16_t value, semantic_tag tag)
        : event_type_(staj_event_type::half_value), tag_(tag)
    {
        value_.half_value_ = value;
    }

    basic_staj_event(double value, semantic_tag tag)
        : event_type_(staj_event_type::double_value), tag_(tag)
    {
        value_.double_value_ = value;
    }

    basic_staj_event(const string_view_type& s,
        staj_event_type event_type,
        semantic_tag tag = semantic_tag::none)
        : event_type_(event_type), tag_(tag), length_(s.length())
    {
        value_.string_data_ = s.data();
    }

    basic_staj_event(const byte_string_view& s,
        staj_event_type event_type,
        semantic_tag tag = semantic_tag::none)
        : event_type_(event_type), tag_(tag), length_(s.size())
    {
        value_.byte_string_data_ = s.data();
    }

    basic_staj_event(const byte_string_view& s,
        staj_event_type event_type,
        uint64_t ext_tag)
        : event_type_(event_type), tag_(semantic_tag::ext), ext_tag_(ext_tag), length_(s.size())
    {
        value_.byte_string_data_ = s.data();
    }
    
    ~basic_staj_event() = default;

    std::size_t size() const
    {
        return length_;
    }

    template <typename T>
    T get() const
    {
        std::error_code ec;
        T val = get<T>(ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec));
        }
        return val;
    }

    template <typename T>
    T get(std::error_code& ec) const
    {
        return get_<T>(std::allocator<char>{}, ec);
    }

    template <typename T,typename Allocator,typename CharT_ = CharT>
    typename std::enable_if<extension_traits::is_string<T>::value && std::is_same<typename T::value_type, CharT_>::value, T>::type
    get_(Allocator,std::error_code& ec) const
    {
        switch (event_type_)
        {
            case staj_event_type::key:
            case staj_event_type::string_value:
            {
                value_converter<jsoncons::basic_string_view<CharT>,T> converter;
                return converter.convert(jsoncons::basic_string_view<CharT>(value_.string_data_, length_), tag(), ec);
            }
            case staj_event_type::byte_string_value:
            {
                value_converter<jsoncons::byte_string_view,T> converter;
                return converter.convert(byte_string_view(value_.byte_string_data_,length_),tag(),ec);
            }
            case staj_event_type::uint64_value:
            {
                value_converter<uint64_t,T> converter;
                return converter.convert(value_.uint64_value_, tag(), ec);
            }
            case staj_event_type::int64_value:
            {
                value_converter<int64_t,T> converter;
                return converter.convert(value_.int64_value_, tag(), ec);
            }
            case staj_event_type::half_value:
            {
                value_converter<half_arg_t,T> converter;
                return converter.convert(value_.half_value_, tag(), ec);
            }
            case staj_event_type::double_value:
            {
                value_converter<double,T> converter;
                return converter.convert(value_.double_value_, tag(), ec);
            }
            case staj_event_type::bool_value:
            {
                value_converter<bool,T> converter;
                return converter.convert(value_.bool_value_,tag(),ec);
            }
            case staj_event_type::null_value:
            {
                value_converter<null_type,T> converter;
                return converter.convert(tag(), ec);
            }
            default:
            {
                ec = conv_errc::not_string;
                return T{};
            }
        }
    }

    template <typename T,typename Allocator,typename CharT_ = CharT>
    typename std::enable_if<extension_traits::is_string_view<T>::value && std::is_same<typename T::value_type, CharT_>::value, T>::type
        get_(Allocator, std::error_code& ec) const
    {
        T s;
        switch (event_type_)
        {
        case staj_event_type::key:
        case staj_event_type::string_value:
            s = T(value_.string_data_, length_);
            break;
        default:
            ec = conv_errc::not_string_view;
            break;        
        }
        return s;
    }

    template <typename T,typename Allocator>
    typename std::enable_if<std::is_same<T, byte_string_view>::value, T>::type
        get_(Allocator, std::error_code& ec) const
    {
        T s;
        switch (event_type_)
        {
            case staj_event_type::byte_string_value:
                s = T(value_.byte_string_data_, length_);
                break;
            default:
                ec = conv_errc::not_byte_string_view;
                break;
        }
        return s;
    }

    template <typename T,typename Allocator>
    typename std::enable_if<extension_traits::is_array_like<T>::value &&
                            std::is_same<typename T::value_type,uint8_t>::value,T>::type
    get_(Allocator, std::error_code& ec) const
    {
        switch (event_type_)
        {
        case staj_event_type::byte_string_value:
            {
                value_converter<byte_string_view,T> converter;
                return converter.convert(byte_string_view(value_.byte_string_data_, length_), tag(), ec);
            }
        case staj_event_type::string_value:
            {
                value_converter<jsoncons::basic_string_view<CharT>,T> converter;
                return converter.convert(jsoncons::basic_string_view<CharT>(value_.string_data_, length_), tag(), ec);
            }
            default:
                ec = conv_errc::not_byte_string;
                return T{};
        }
    }

    template <typename IntegerType,typename Allocator>
    typename std::enable_if<extension_traits::is_integer<IntegerType>::value, IntegerType>::type
    get_(Allocator, std::error_code& ec) const
    {
        switch (event_type_)
        {
            case staj_event_type::string_value:
            {
                IntegerType val;
                auto result = jsoncons::detail::to_integer(value_.string_data_, length_, val);
                if (!result)
                {
                    ec = conv_errc::not_integer;
                    return IntegerType();
                }
                return val;
            }
            case staj_event_type::half_value:
                return static_cast<IntegerType>(value_.half_value_);
            case staj_event_type::double_value:
                return static_cast<IntegerType>(value_.double_value_);
            case staj_event_type::int64_value:
                return static_cast<IntegerType>(value_.int64_value_);
            case staj_event_type::uint64_value:
                return static_cast<IntegerType>(value_.uint64_value_);
            case staj_event_type::bool_value:
                return static_cast<IntegerType>(value_.bool_value_ ? 1 : 0);
            default:
                ec = conv_errc::not_integer;
                return IntegerType();
        }
    }

    template <typename T,typename Allocator>
    typename std::enable_if<std::is_floating_point<T>::value, T>::type
        get_(Allocator, std::error_code& ec) const
    {
        return static_cast<T>(as_double(ec));
    }

    template <typename T,typename Allocator>
    typename std::enable_if<extension_traits::is_bool<T>::value, T>::type
        get_(Allocator, std::error_code& ec) const
    {
        return as_bool(ec);
    }

    staj_event_type event_type() const noexcept { return event_type_; }

    semantic_tag tag() const noexcept { return tag_; }

    uint64_t ext_tag() const noexcept { return ext_tag_; }

private:

    double as_double(std::error_code& ec) const
    {
        switch (event_type_)
        {
            case staj_event_type::key:
            case staj_event_type::string_value:
            {
                jsoncons::detail::chars_to f;
                return f(value_.string_data_, length_);
            }
            case staj_event_type::double_value:
                return value_.double_value_;
            case staj_event_type::int64_value:
                return static_cast<double>(value_.int64_value_);
            case staj_event_type::uint64_value:
                return static_cast<double>(value_.uint64_value_);
            case staj_event_type::half_value:
            {
                double x = binary::decode_half(value_.half_value_);
                return static_cast<double>(x);
            }
            default:
                ec = conv_errc::not_double;
                return double();
        }
    }

    bool as_bool(std::error_code& ec) const
    {
        switch (event_type_)
        {
            case staj_event_type::bool_value:
                return value_.bool_value_;
            case staj_event_type::double_value:
                return value_.double_value_ != 0.0;
            case staj_event_type::int64_value:
                return value_.int64_value_ != 0;
            case staj_event_type::uint64_value:
                return value_.uint64_value_ != 0;
            default:
                ec = conv_errc::not_bool;
                return bool();
        }
    }
public:
    bool send_json_event(basic_json_visitor<CharT>& visitor,
        const ser_context& context,
        std::error_code& ec) const
    {
        switch (event_type())
        {
            case staj_event_type::begin_array:
                return visitor.begin_array(tag(), context);
            case staj_event_type::end_array:
                return visitor.end_array(context);
            case staj_event_type::begin_object:
                return visitor.begin_object(tag(), context, ec);
            case staj_event_type::end_object:
                return visitor.end_object(context, ec);
            case staj_event_type::key:
                return visitor.key(string_view_type(value_.string_data_,length_), context);
            case staj_event_type::string_value:
                return visitor.string_value(string_view_type(value_.string_data_,length_), tag(), context);
            case staj_event_type::byte_string_value:
                return visitor.byte_string_value(byte_string_view(value_.byte_string_data_,length_), tag(), context);
            case staj_event_type::null_value:
                return visitor.null_value(tag(), context);
            case staj_event_type::bool_value:
                return visitor.bool_value(value_.bool_value_, tag(), context);
            case staj_event_type::int64_value:
                return visitor.int64_value(value_.int64_value_, tag(), context);
            case staj_event_type::uint64_value:
                return visitor.uint64_value(value_.uint64_value_, tag(), context);
            case staj_event_type::half_value:
                return visitor.half_value(value_.half_value_, tag(), context);
            case staj_event_type::double_value:
                return visitor.double_value(value_.double_value_, tag(), context);
            default:
                return false;
        }
    }
    
    bool send_value_event(basic_item_event_visitor<CharT>& visitor,
        const ser_context& context,
        std::error_code& ec) const
    {
        switch (event_type())
        {
            case staj_event_type::key:
                return visitor.string_value(string_view_type(value_.string_data_,length_), tag(), context);
            case staj_event_type::begin_array:
                return visitor.begin_array(tag(), context);
            case staj_event_type::end_array:
                return visitor.end_array(context);
            case staj_event_type::begin_object:
                return visitor.begin_object(tag(), context, ec);
            case staj_event_type::end_object:
                return visitor.end_object(context, ec);
            case staj_event_type::string_value:
                return visitor.string_value(string_view_type(value_.string_data_,length_), tag(), context);
            case staj_event_type::byte_string_value:
                return visitor.byte_string_value(byte_string_view(value_.byte_string_data_,length_), tag(), context);
            case staj_event_type::null_value:
                return visitor.null_value(tag(), context);
            case staj_event_type::bool_value:
                return visitor.bool_value(value_.bool_value_, tag(), context);
            case staj_event_type::int64_value:
                return visitor.int64_value(value_.int64_value_, tag(), context);
            case staj_event_type::uint64_value:
                return visitor.uint64_value(value_.uint64_value_, tag(), context);
            case staj_event_type::half_value:
                return visitor.half_value(value_.half_value_, tag(), context);
            case staj_event_type::double_value:
                return visitor.double_value(value_.double_value_, tag(), context);
            default:
                return false;
        }
    }

};

} // namespace jsoncons

#endif // JSONCONS_STAJ_EVENT_HPP

