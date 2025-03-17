// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_BSON_DECODE_BSON_HPP
#define JSONCONS_EXT_BSON_DECODE_BSON_HPP

#include <istream> // std::basic_istream
#include <type_traits> // std::enable_if

#include <jsoncons/allocator_set.hpp>
#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/utility/extension_traits.hpp>
#include <jsoncons/basic_json.hpp>
#include <jsoncons/conv_error.hpp>
#include <jsoncons/json_decoder.hpp>
#include <jsoncons/decode_traits.hpp>
#include <jsoncons/json_visitor.hpp>
#include <jsoncons/source.hpp>

#include <jsoncons_ext/bson/bson_cursor.hpp>
#include <jsoncons_ext/bson/bson_options.hpp>
#include <jsoncons_ext/bson/bson_reader.hpp>

namespace jsoncons { 
namespace bson {

    template <typename T,typename Source>
    typename std::enable_if<extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_byte_sequence<Source>::value,T>::type 
    decode_bson(const Source& v, 
                const bson_decode_options& options = bson_decode_options())
    {
        jsoncons::json_decoder<T> decoder;
        auto adaptor = make_json_visitor_adaptor<json_visitor>(decoder);
        basic_bson_reader<jsoncons::bytes_source> reader(v, adaptor, options);
        reader.read();
        if (!decoder.is_valid())
        {
            JSONCONS_THROW(ser_error(conv_errc::conversion_failed, reader.line(), reader.column()));
        }
        return decoder.get_result();
    }

    template <typename T,typename Source>
    typename std::enable_if<!extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_byte_sequence<Source>::value,T>::type 
    decode_bson(const Source& v, 
                const bson_decode_options& options = bson_decode_options())
    {
        basic_bson_cursor<bytes_source> cursor(v, options);
        json_decoder<basic_json<char,sorted_policy>> decoder{};

        std::error_code ec;
        T val = decode_traits<T,char>::decode(cursor, decoder, ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec, cursor.context().line(), cursor.context().column()));
        }
        return val;
    }

    template <typename T>
    typename std::enable_if<extension_traits::is_basic_json<T>::value,T>::type 
    decode_bson(std::istream& is, 
                const bson_decode_options& options = bson_decode_options())
    {
        jsoncons::json_decoder<T> decoder;
        auto adaptor = make_json_visitor_adaptor<json_visitor>(decoder);
        bson_stream_reader reader(is, adaptor, options);
        reader.read();
        if (!decoder.is_valid())
        {
            JSONCONS_THROW(ser_error(conv_errc::conversion_failed, reader.line(), reader.column()));
        }
        return decoder.get_result();
    }

    template <typename T>
    typename std::enable_if<!extension_traits::is_basic_json<T>::value,T>::type 
    decode_bson(std::istream& is, 
                const bson_decode_options& options = bson_decode_options())
    {
        basic_bson_cursor<binary_stream_source> cursor(is, options);
        json_decoder<basic_json<char,sorted_policy>> decoder{};

        std::error_code ec;
        T val = decode_traits<T,char>::decode(cursor, decoder, ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec, cursor.context().line(), cursor.context().column()));
        }
        return val;
    }

    template <typename T,typename InputIt>
    typename std::enable_if<extension_traits::is_basic_json<T>::value,T>::type 
    decode_bson(InputIt first, InputIt last,
                const bson_decode_options& options = bson_decode_options())
    {
        jsoncons::json_decoder<T> decoder;
        auto adaptor = make_json_visitor_adaptor<json_visitor>(decoder);
        basic_bson_reader<binary_iterator_source<InputIt>> reader(binary_iterator_source<InputIt>(first, last), adaptor, options);
        reader.read();
        if (!decoder.is_valid())
        {
            JSONCONS_THROW(ser_error(conv_errc::conversion_failed, reader.line(), reader.column()));
        }
        return decoder.get_result();
    }

    template <typename T,typename InputIt>
    typename std::enable_if<!extension_traits::is_basic_json<T>::value,T>::type 
    decode_bson(InputIt first, InputIt last,
                const bson_decode_options& options = bson_decode_options())
    {
        basic_bson_cursor<binary_iterator_source<InputIt>> cursor(binary_iterator_source<InputIt>(first, last), options);
        json_decoder<basic_json<char,sorted_policy>> decoder{};

        std::error_code ec;
        T val = decode_traits<T,char>::decode(cursor, decoder, ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec, cursor.context().line(), cursor.context().column()));
        }
        return val;
    }

    // With leading allocator_set parameter

    template <typename T,typename Source,typename Allocator,typename TempAllocator >
    typename std::enable_if<extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_byte_sequence<Source>::value,T>::type 
    decode_bson(const allocator_set<Allocator,TempAllocator>& alloc_set,
                const Source& v, 
                const bson_decode_options& options = bson_decode_options())
    {
        json_decoder<T,TempAllocator> decoder(alloc_set.get_allocator(), alloc_set.get_temp_allocator());
        auto adaptor = make_json_visitor_adaptor<json_visitor>(decoder);
        basic_bson_reader<jsoncons::bytes_source,TempAllocator> reader(v, adaptor, options, alloc_set.get_temp_allocator());
        reader.read();
        if (!decoder.is_valid())
        {
            JSONCONS_THROW(ser_error(conv_errc::conversion_failed, reader.line(), reader.column()));
        }
        return decoder.get_result();
    }

    template <typename T,typename Source,typename Allocator,typename TempAllocator >
    typename std::enable_if<!extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_byte_sequence<Source>::value,T>::type 
    decode_bson(const allocator_set<Allocator,TempAllocator>& alloc_set,
                const Source& v, 
                const bson_decode_options& options = bson_decode_options())
    {
        basic_bson_cursor<bytes_source,TempAllocator> cursor(v, options, alloc_set.get_temp_allocator());
        json_decoder<basic_json<char,sorted_policy,TempAllocator>,TempAllocator> decoder(alloc_set.get_temp_allocator(), alloc_set.get_temp_allocator());

        std::error_code ec;
        T val = decode_traits<T,char>::decode(cursor, decoder, ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec, cursor.context().line(), cursor.context().column()));
        }
        return val;
    }

    template <typename T,typename Allocator,typename TempAllocator >
    typename std::enable_if<extension_traits::is_basic_json<T>::value,T>::type 
    decode_bson(const allocator_set<Allocator,TempAllocator>& alloc_set,
                std::istream& is, 
                const bson_decode_options& options = bson_decode_options())
    {
        json_decoder<T,TempAllocator> decoder(alloc_set.get_allocator(), alloc_set.get_temp_allocator());
        auto adaptor = make_json_visitor_adaptor<json_visitor>(decoder);
        basic_bson_reader<jsoncons::binary_stream_source,TempAllocator> reader(is, adaptor, options, alloc_set.get_temp_allocator());
        reader.read();
        if (!decoder.is_valid())
        {
            JSONCONS_THROW(ser_error(conv_errc::conversion_failed, reader.line(), reader.column()));
        }
        return decoder.get_result();
    }

    template <typename T,typename Allocator,typename TempAllocator >
    typename std::enable_if<!extension_traits::is_basic_json<T>::value,T>::type 
    decode_bson(const allocator_set<Allocator,TempAllocator>& alloc_set,
                std::istream& is, 
                const bson_decode_options& options = bson_decode_options())
    {
        basic_bson_cursor<binary_stream_source,TempAllocator> cursor(is, options, alloc_set.get_temp_allocator());
        json_decoder<basic_json<char,sorted_policy,TempAllocator>,TempAllocator> decoder(alloc_set.get_temp_allocator(), alloc_set.get_temp_allocator());

        std::error_code ec;
        T val = decode_traits<T,char>::decode(cursor, decoder, ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec, cursor.context().line(), cursor.context().column()));
        }
        return val;
    }
  
} // namespace bson
} // namespace jsoncons

#endif // JSONCONS_EXT_BSON_DECODE_BSON_HPP
