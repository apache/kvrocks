// Copyright 2017-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_CBOR_DECODE_CBOR_HPP
#define JSONCONS_EXT_CBOR_DECODE_CBOR_HPP

#include <istream> // std::basic_istream
#include <type_traits> // std::enable_if
#include <vector>

#include <jsoncons/allocator_set.hpp>
#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/conv_error.hpp>
#include <jsoncons/decode_traits.hpp>
#include <jsoncons/json_filter.hpp>
#include <jsoncons/basic_json.hpp>
#include <jsoncons/source.hpp>

#include <jsoncons_ext/cbor/cbor_cursor.hpp>
#include <jsoncons_ext/cbor/cbor_reader.hpp>

namespace jsoncons { 
namespace cbor {

    template <typename T,typename Source>
    typename std::enable_if<extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_byte_sequence<Source>::value,T>::type 
    decode_cbor(const Source& v, 
                const cbor_decode_options& options = cbor_decode_options())
    {
        jsoncons::json_decoder<T> decoder;
        auto adaptor = make_json_visitor_adaptor<json_visitor>(decoder);
        basic_cbor_reader<jsoncons::bytes_source> reader(v, adaptor, options);
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
    decode_cbor(const Source& v, 
                const cbor_decode_options& options = cbor_decode_options())
    {
        basic_cbor_cursor<bytes_source> cursor(v, options);
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
    decode_cbor(std::istream& is, 
                const cbor_decode_options& options = cbor_decode_options())
    {
        jsoncons::json_decoder<T> decoder;
        auto adaptor = make_json_visitor_adaptor<json_visitor>(decoder);
        cbor_stream_reader reader(is, adaptor, options);
        reader.read();
        if (!decoder.is_valid())
        {
            JSONCONS_THROW(ser_error(conv_errc::conversion_failed, reader.line(), reader.column()));
        }
        return decoder.get_result();
    }
     
    template <typename T>
    typename std::enable_if<!extension_traits::is_basic_json<T>::value,T>::type 
    decode_cbor(std::istream& is, 
                const cbor_decode_options& options = cbor_decode_options())
    {
        basic_cbor_cursor<binary_stream_source> cursor(is, options);
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
    decode_cbor(InputIt first, InputIt last,
                const cbor_decode_options& options = cbor_decode_options())
    {
        jsoncons::json_decoder<T> decoder;
        auto adaptor = make_json_visitor_adaptor<json_visitor>(decoder);
        basic_cbor_reader<binary_iterator_source<InputIt>> reader(binary_iterator_source<InputIt>(first, last), adaptor, options);
        reader.read();
        if (!decoder.is_valid())
        {
            JSONCONS_THROW(ser_error(conv_errc::conversion_failed, reader.line(), reader.column()));
        }
        return decoder.get_result();
    }

    template <typename T,typename InputIt>
    typename std::enable_if<!extension_traits::is_basic_json<T>::value,T>::type 
    decode_cbor(InputIt first, InputIt last,
                const cbor_decode_options& options = cbor_decode_options())
    {
        basic_cbor_cursor<binary_iterator_source<InputIt>> cursor(binary_iterator_source<InputIt>(first, last), options);
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
    decode_cbor(const allocator_set<Allocator,TempAllocator>& alloc_set,
                const Source& v, 
                const cbor_decode_options& options = cbor_decode_options())
    {
        json_decoder<T,TempAllocator> decoder(alloc_set.get_allocator(), alloc_set.get_temp_allocator());
        auto adaptor = make_json_visitor_adaptor<json_visitor>(decoder);
        basic_cbor_reader<jsoncons::bytes_source,TempAllocator> reader(v, adaptor, options, alloc_set.get_temp_allocator());
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
    decode_cbor(const allocator_set<Allocator,TempAllocator>& alloc_set,
                const Source& v, 
                const cbor_decode_options& options = cbor_decode_options())
    {
        basic_cbor_cursor<bytes_source,TempAllocator> cursor(v, options, alloc_set.get_temp_allocator());
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
    decode_cbor(const allocator_set<Allocator,TempAllocator>& alloc_set,
                std::istream& is, 
                const cbor_decode_options& options = cbor_decode_options())
    {
        json_decoder<T,TempAllocator> decoder(alloc_set.get_allocator(), alloc_set.get_temp_allocator());
        auto adaptor = make_json_visitor_adaptor<json_visitor>(decoder);
        basic_cbor_reader<jsoncons::binary_stream_source,TempAllocator> reader(is, adaptor, options, alloc_set.get_temp_allocator());
        reader.read();
        if (!decoder.is_valid())
        {
            JSONCONS_THROW(ser_error(conv_errc::conversion_failed, reader.line(), reader.column()));
        }
        return decoder.get_result();
    }

    template <typename T,typename Allocator,typename TempAllocator >
    typename std::enable_if<!extension_traits::is_basic_json<T>::value,T>::type 
    decode_cbor(const allocator_set<Allocator,TempAllocator>& alloc_set,
                std::istream& is, 
                const cbor_decode_options& options = cbor_decode_options())
    {
        basic_cbor_cursor<binary_stream_source,TempAllocator> cursor(is, options, alloc_set.get_temp_allocator());
        json_decoder<basic_json<char,sorted_policy,TempAllocator>,TempAllocator> decoder(alloc_set.get_temp_allocator(), alloc_set.get_temp_allocator());

        std::error_code ec;
        T val = decode_traits<T,char>::decode(cursor, decoder, ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec, cursor.context().line(), cursor.context().column()));
        }
        return val;
    }

} // namespace cbor
} // namespace jsoncons

#endif
