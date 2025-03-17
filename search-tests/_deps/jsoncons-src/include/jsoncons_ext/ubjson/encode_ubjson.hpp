// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_UBJSON_ENCODE_UBJSON_HPP
#define JSONCONS_EXT_UBJSON_ENCODE_UBJSON_HPP

#include <ostream> // std::basic_ostream
#include <type_traits> // std::enable_if

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/utility/extension_traits.hpp>
#include <jsoncons/basic_json.hpp>
#include <jsoncons/encode_traits.hpp>
#include <jsoncons/sink.hpp>

#include <jsoncons_ext/ubjson/ubjson_encoder.hpp>

namespace jsoncons { 
namespace ubjson {

    template <typename T,typename ByteContainer>
    typename std::enable_if<extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_back_insertable_byte_container<ByteContainer>::value,void>::type 
    encode_ubjson(const T& j, 
                  ByteContainer& cont, 
                  const ubjson_encode_options& options = ubjson_encode_options())
    {
        using char_type = typename T::char_type;
        basic_ubjson_encoder<jsoncons::bytes_sink<ByteContainer>> encoder(cont, options);
        auto adaptor = make_json_visitor_adaptor<basic_json_visitor<char_type>>(encoder);
        j.dump(adaptor);
    }

    template <typename T,typename ByteContainer>
    typename std::enable_if<!extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_back_insertable_byte_container<ByteContainer>::value,void>::type 
    encode_ubjson(const T& val, 
                  ByteContainer& cont, 
                  const ubjson_encode_options& options = ubjson_encode_options())
    {
        basic_ubjson_encoder<jsoncons::bytes_sink<ByteContainer>> encoder(cont, options);
        std::error_code ec;
        encode_traits<T,char>::encode(val, encoder, json(), ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec));
        }
    }

    template <typename T>
    typename std::enable_if<extension_traits::is_basic_json<T>::value,void>::type 
    encode_ubjson(const T& j, 
                  std::ostream& os, 
                  const ubjson_encode_options& options = ubjson_encode_options())
    {
        using char_type = typename T::char_type;
        ubjson_stream_encoder encoder(os, options);
        auto adaptor = make_json_visitor_adaptor<basic_json_visitor<char_type>>(encoder);
        j.dump(adaptor);
    }

    template <typename T>
    typename std::enable_if<!extension_traits::is_basic_json<T>::value,void>::type 
    encode_ubjson(const T& val, 
                  std::ostream& os, 
                  const ubjson_encode_options& options = ubjson_encode_options())
    {
        ubjson_stream_encoder encoder(os, options);
        std::error_code ec;
        encode_traits<T,char>::encode(val, encoder, json(), ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec));
        }
    }

    // with temp_allocator_arg_t

    template <typename T,typename ByteContainer,typename Allocator,typename TempAllocator >
    typename std::enable_if<extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_back_insertable_byte_container<ByteContainer>::value,void>::type 
    encode_ubjson(const allocator_set<Allocator,TempAllocator>& alloc_set,const T& j, 
                  ByteContainer& cont, 
                  const ubjson_encode_options& options = ubjson_encode_options())
    {
        using char_type = typename T::char_type;
        basic_ubjson_encoder<jsoncons::bytes_sink<ByteContainer>,TempAllocator> encoder(cont, options, alloc_set.get_temp_allocator());
        auto adaptor = make_json_visitor_adaptor<basic_json_visitor<char_type>>(encoder);
        j.dump(adaptor);
    }

    template <typename T,typename ByteContainer,typename Allocator,typename TempAllocator >
    typename std::enable_if<!extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_back_insertable_byte_container<ByteContainer>::value,void>::type 
    encode_ubjson(const allocator_set<Allocator,TempAllocator>& alloc_set,const T& val, 
                  ByteContainer& cont, 
                  const ubjson_encode_options& options = ubjson_encode_options())
    {
        basic_ubjson_encoder<jsoncons::bytes_sink<ByteContainer>,TempAllocator> encoder(cont, options, alloc_set.get_temp_allocator());
        std::error_code ec;
        encode_traits<T,char>::encode(val, encoder, json(), ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec));
        }
    }

    template <typename T,typename Allocator,typename TempAllocator >
    typename std::enable_if<extension_traits::is_basic_json<T>::value,void>::type 
    encode_ubjson(const allocator_set<Allocator,TempAllocator>& alloc_set,
                  const T& j, 
                  std::ostream& os, 
                  const ubjson_encode_options& options = ubjson_encode_options())
    {
        using char_type = typename T::char_type;
        basic_ubjson_encoder<jsoncons::binary_stream_sink,TempAllocator> encoder(os, options, alloc_set.get_temp_allocator());
        auto adaptor = make_json_visitor_adaptor<basic_json_visitor<char_type>>(encoder);
        j.dump(adaptor);
    }

    template <typename T,typename Allocator,typename TempAllocator >
    typename std::enable_if<!extension_traits::is_basic_json<T>::value,void>::type 
    encode_ubjson(const allocator_set<Allocator,TempAllocator>& alloc_set,
                  const T& val, 
                  std::ostream& os, 
                  const ubjson_encode_options& options = ubjson_encode_options())
    {
        basic_ubjson_encoder<jsoncons::binary_stream_sink,TempAllocator> encoder(os, options, alloc_set.get_temp_allocator());
        std::error_code ec;
        encode_traits<T,char>::encode(val, encoder, json(), ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec));
        }
    }

} // namespace ubjson
} // namespace jsoncons

#endif // JSONCONS_EXT_UBJSON_ENCODE_UBJSON_HPP
