// Copyright 2017-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_CBOR_ENCODE_CBOR_HPP
#define JSONCONS_EXT_CBOR_ENCODE_CBOR_HPP

#include <ostream> // std::basic_ostream
#include <type_traits> // std::enable_if

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/encode_traits.hpp>
#include <jsoncons/json_filter.hpp>
#include <jsoncons/basic_json.hpp>

#include <jsoncons_ext/cbor/cbor_encoder.hpp>

namespace jsoncons { 
namespace cbor {

    // to bytes 

    template <typename T,typename ByteContainer>
    typename std::enable_if<extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_back_insertable_byte_container<ByteContainer>::value,void>::type 
    encode_cbor(const T& j, 
                ByteContainer& cont, 
                const cbor_encode_options& options = cbor_encode_options())
    {
        using char_type = typename T::char_type;
        basic_cbor_encoder<jsoncons::bytes_sink<ByteContainer>> encoder(cont, options);
        auto adaptor = make_json_visitor_adaptor<basic_json_visitor<char_type>>(encoder);
        j.dump(adaptor);
    }

    template <typename T,typename ByteContainer>
    typename std::enable_if<!extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_back_insertable_byte_container<ByteContainer>::value,void>::type 
    encode_cbor(const T& val, ByteContainer& cont, 
                const cbor_encode_options& options = cbor_encode_options())
    {
        basic_cbor_encoder<jsoncons::bytes_sink<ByteContainer>> encoder(cont, options);
        std::error_code ec;
        encode_traits<T,char>::encode(val, encoder, json(), ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec));
        }
    }

    // stream

    template <typename T>
    typename std::enable_if<extension_traits::is_basic_json<T>::value,void>::type 
    encode_cbor(const T& j, 
                std::ostream& os, 
                const cbor_encode_options& options = cbor_encode_options())
    {
        using char_type = typename T::char_type;
        cbor_stream_encoder encoder(os, options);
        auto adaptor = make_json_visitor_adaptor<basic_json_visitor<char_type>>(encoder);
        j.dump(adaptor);
    }

    template <typename T>
    typename std::enable_if<!extension_traits::is_basic_json<T>::value,void>::type 
    encode_cbor(const T& val, 
                std::ostream& os, 
                const cbor_encode_options& options = cbor_encode_options())
    {
        cbor_stream_encoder encoder(os, options);
        std::error_code ec;
        encode_traits<T,char>::encode(val, encoder, json(), ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec));
        }
    }

    // temp_allocator_arg

    // to bytes 

    template <typename T,typename ByteContainer,typename Allocator,typename TempAllocator >
    typename std::enable_if<extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_back_insertable_byte_container<ByteContainer>::value,void>::type 
    encode_cbor(const allocator_set<Allocator,TempAllocator>& alloc_set,
                const T& j, 
                ByteContainer& cont, 
                const cbor_encode_options& options = cbor_encode_options())
    {
        using char_type = typename T::char_type;
        basic_cbor_encoder<bytes_sink<ByteContainer>,TempAllocator> encoder(cont, options, alloc_set.get_temp_allocator());
        auto adaptor = make_json_visitor_adaptor<basic_json_visitor<char_type>>(encoder);
        j.dump(adaptor);
    }

    template <typename T,typename ByteContainer,typename Allocator,typename TempAllocator >
    typename std::enable_if<!extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_back_insertable_byte_container<ByteContainer>::value,void>::type 
    encode_cbor(const allocator_set<Allocator,TempAllocator>& alloc_set,
                const T& val, 
                ByteContainer& cont, 
                const cbor_encode_options& options = cbor_encode_options())
    {
        basic_cbor_encoder<jsoncons::bytes_sink<ByteContainer>,TempAllocator> encoder(cont, options, alloc_set.get_temp_allocator());
        std::error_code ec;
        encode_traits<T,char>::encode(val, encoder, json(), ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec));
        }
    }

    // stream

    template <typename T,typename Allocator,typename TempAllocator >
    typename std::enable_if<extension_traits::is_basic_json<T>::value,void>::type 
    encode_cbor(const allocator_set<Allocator,TempAllocator>& alloc_set,
                const T& j, 
                std::ostream& os, 
                const cbor_encode_options& options = cbor_encode_options())
    {
        using char_type = typename T::char_type;
        basic_cbor_encoder<binary_stream_sink,TempAllocator> encoder(os, options, alloc_set.get_temp_allocator());
        auto adaptor = make_json_visitor_adaptor<basic_json_visitor<char_type>>(encoder);
        j.dump(adaptor);
    }

    template <typename T,typename Allocator,typename TempAllocator >
    typename std::enable_if<!extension_traits::is_basic_json<T>::value,void>::type 
    encode_cbor(const allocator_set<Allocator,TempAllocator>& alloc_set,
                const T& val, 
                std::ostream& os, 
                const cbor_encode_options& options = cbor_encode_options())
    {
        basic_cbor_encoder<binary_stream_sink,TempAllocator> encoder(os, options, alloc_set.get_temp_allocator());
        std::error_code ec;
        encode_traits<T,char>::encode(val, encoder, json(), ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec));
        }
    }

} // namespace cbor
} // namespace jsoncons

#endif
