// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_MSGPACK_ENCODE_MSGPACK_HPP
#define JSONCONS_EXT_MSGPACK_ENCODE_MSGPACK_HPP

#include <ostream> // std::basic_ostream
#include <system_error> 
#include <type_traits> 

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/utility/extension_traits.hpp>
#include <jsoncons/allocator_set.hpp>
#include <jsoncons/basic_json.hpp>
#include <jsoncons/encode_traits.hpp>
#include <jsoncons/json_exception.hpp>
#include <jsoncons/json_visitor.hpp>
#include <jsoncons/sink.hpp>

#include <jsoncons_ext/msgpack/msgpack_encoder.hpp>
#include <jsoncons_ext/msgpack/msgpack_options.hpp>

namespace jsoncons { 
namespace msgpack {

    template <typename T,typename ByteContainer>
    typename std::enable_if<extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_back_insertable_byte_container<ByteContainer>::value,void>::type 
    encode_msgpack(const T& j, 
                   ByteContainer& cont, 
                   const msgpack_encode_options& options = msgpack_encode_options())
    {
        using char_type = typename T::char_type;
        basic_msgpack_encoder<jsoncons::bytes_sink<ByteContainer>> encoder(cont, options);
        auto adaptor = make_json_visitor_adaptor<basic_json_visitor<char_type>>(encoder);
        j.dump(adaptor);
    }

    template <typename T,typename ByteContainer>
    typename std::enable_if<!extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_back_insertable_byte_container<ByteContainer>::value,void>::type 
    encode_msgpack(const T& val, 
                   ByteContainer& cont, 
                   const msgpack_encode_options& options = msgpack_encode_options())
    {
        basic_msgpack_encoder<jsoncons::bytes_sink<ByteContainer>> encoder(cont, options);
        std::error_code ec;
        encode_traits<T,char>::encode(val, encoder, json(), ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec));
        }
    }

    template <typename T>
    typename std::enable_if<extension_traits::is_basic_json<T>::value,void>::type 
    encode_msgpack(const T& j, 
                   std::ostream& os, 
                   const msgpack_encode_options& options = msgpack_encode_options())
    {
        using char_type = typename T::char_type;
        msgpack_stream_encoder encoder(os, options);
        auto adaptor = make_json_visitor_adaptor<basic_json_visitor<char_type>>(encoder);
        j.dump(adaptor);
    }

    template <typename T>
    typename std::enable_if<!extension_traits::is_basic_json<T>::value,void>::type 
    encode_msgpack(const T& val, 
                   std::ostream& os, 
                   const msgpack_encode_options& options = msgpack_encode_options())
    {
        msgpack_stream_encoder encoder(os, options);
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
    encode_msgpack(const allocator_set<Allocator,TempAllocator>& alloc_set, const T& j, 
                   ByteContainer& cont, 
                   const msgpack_encode_options& options = msgpack_encode_options())
    {
        using char_type = typename T::char_type;
        basic_msgpack_encoder<jsoncons::bytes_sink<ByteContainer>,TempAllocator> encoder(cont, options, alloc_set.get_temp_allocator());
        auto adaptor = make_json_visitor_adaptor<basic_json_visitor<char_type>>(encoder);
        j.dump(adaptor);
    }

    template <typename T,typename ByteContainer,typename Allocator,typename TempAllocator >
    typename std::enable_if<!extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_back_insertable_byte_container<ByteContainer>::value,void>::type 
    encode_msgpack(const allocator_set<Allocator,TempAllocator>& alloc_set, 
                   const T& val, ByteContainer& cont, 
                   const msgpack_encode_options& options = msgpack_encode_options())
    {
        basic_msgpack_encoder<jsoncons::bytes_sink<ByteContainer>,TempAllocator> encoder(cont, options, alloc_set.get_temp_allocator());
        std::error_code ec;
        encode_traits<T,char>::encode(val, encoder, json(), ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec));
        }
    }

    template <typename T,typename Allocator,typename TempAllocator >
    typename std::enable_if<extension_traits::is_basic_json<T>::value,void>::type 
    encode_msgpack(const allocator_set<Allocator,TempAllocator>& alloc_set, 
                   const T& j, 
                   std::ostream& os, 
                   const msgpack_encode_options& options = msgpack_encode_options())
    {
        using char_type = typename T::char_type;
        basic_msgpack_encoder<jsoncons::binary_stream_sink,TempAllocator> encoder(os, options, alloc_set.get_temp_allocator());
        auto adaptor = make_json_visitor_adaptor<basic_json_visitor<char_type>>(encoder);
        j.dump(adaptor);
    }

    template <typename T,typename Allocator,typename TempAllocator >
    typename std::enable_if<!extension_traits::is_basic_json<T>::value,void>::type 
    encode_msgpack(const allocator_set<Allocator,TempAllocator>& alloc_set, 
                   const T& val, 
                   std::ostream& os, 
                   const msgpack_encode_options& options = msgpack_encode_options())
    {
        basic_msgpack_encoder<jsoncons::binary_stream_sink,TempAllocator> encoder(os, options, alloc_set.get_temp_allocator());
        std::error_code ec;
        encode_traits<T,char>::encode(val, encoder, json(), ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec));
        }
    }

} // namespace msgpack
} // namespace jsoncons

#endif // JSONCONS_EXT_MSGPACK_ENCODE_MSGPACK_HPP
