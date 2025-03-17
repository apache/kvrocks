/// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_CSV_DECODE_CSV_HPP
#define JSONCONS_EXT_CSV_DECODE_CSV_HPP

#include <type_traits>

#include <jsoncons/allocator_set.hpp>
#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/basic_json.hpp>
#include <jsoncons/decode_traits.hpp>
#include <jsoncons/json_exception.hpp>
#include <jsoncons/source.hpp>

#include <jsoncons_ext/csv/csv_cursor.hpp>
#include <jsoncons_ext/csv/csv_encoder.hpp>
#include <jsoncons_ext/csv/csv_options.hpp>
#include <jsoncons_ext/csv/csv_reader.hpp>

namespace jsoncons { 
namespace csv {

    template <typename T,typename Source>
    typename std::enable_if<extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_sequence_of<Source,typename T::char_type>::value,T>::type 
    decode_csv(const Source& s, const basic_csv_decode_options<typename Source::value_type>& options = basic_csv_decode_options<typename Source::value_type>())
    {
        using char_type = typename Source::value_type;

        json_decoder<T> decoder;

        basic_csv_reader<char_type,jsoncons::string_source<char_type>> reader(s,decoder,options);
        reader.read();
        if (!decoder.is_valid())
        {
            JSONCONS_THROW(ser_error(conv_errc::conversion_failed, reader.line(), reader.column()));
        }
        return decoder.get_result();
    }

    template <typename T,typename Source>
    typename std::enable_if<!extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_char_sequence<Source>::value,T>::type 
    decode_csv(const Source& s, const basic_csv_decode_options<typename Source::value_type>& options = basic_csv_decode_options<typename Source::value_type>())
    {
        using char_type = typename Source::value_type;

        basic_csv_cursor<char_type> cursor(s, options);
        jsoncons::json_decoder<basic_json<char_type>> decoder;

        std::error_code ec;
        T val = decode_traits<T,char_type>::decode(cursor, decoder, ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec, cursor.context().line(), cursor.context().column()));
        }
        return val;
    }

    template <typename T,typename CharT>
    typename std::enable_if<extension_traits::is_basic_json<T>::value,T>::type 
    decode_csv(std::basic_istream<CharT>& is, const basic_csv_decode_options<CharT>& options = basic_csv_decode_options<CharT>())
    {
        using char_type = CharT;

        json_decoder<T> decoder;

        basic_csv_reader<char_type,jsoncons::stream_source<char_type>> reader(is,decoder,options);
        reader.read();
        if (!decoder.is_valid())
        {
            JSONCONS_THROW(ser_error(conv_errc::conversion_failed, reader.line(), reader.column()));
        }
        return decoder.get_result();
    }

    template <typename T,typename CharT>
    typename std::enable_if<!extension_traits::is_basic_json<T>::value,T>::type 
    decode_csv(std::basic_istream<CharT>& is, const basic_csv_decode_options<CharT>& options = basic_csv_decode_options<CharT>())
    {
        basic_csv_cursor<CharT> cursor(is, options);
        jsoncons::json_decoder<basic_json<CharT>> decoder;

        std::error_code ec;
        T val = decode_traits<T,CharT>::decode(cursor, decoder, ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec, cursor.context().line(), cursor.context().column()));
        }
        return val;
    }

    template <typename T,typename InputIt>
    typename std::enable_if<extension_traits::is_basic_json<T>::value,T>::type
    decode_csv(InputIt first, InputIt last,
                const basic_csv_decode_options<typename std::iterator_traits<InputIt>::value_type>& options = 
                    basic_csv_decode_options<typename std::iterator_traits<InputIt>::value_type>())
    {
        using char_type = typename std::iterator_traits<InputIt>::value_type;

        jsoncons::json_decoder<T> decoder;
        basic_csv_reader<char_type, iterator_source<InputIt>> reader(iterator_source<InputIt>(first,last), decoder, options);
        reader.read();
        if (!decoder.is_valid())
        {
            JSONCONS_THROW(ser_error(conv_errc::conversion_failed, reader.line(), reader.column()));
        }
        return decoder.get_result();
    }

    template <typename T,typename InputIt>
    typename std::enable_if<!extension_traits::is_basic_json<T>::value,T>::type
    decode_csv(InputIt first, InputIt last,
               const basic_csv_decode_options<typename std::iterator_traits<InputIt>::value_type>& options = 
                    basic_csv_decode_options<typename std::iterator_traits<InputIt>::value_type>())
    {
        using char_type = typename std::iterator_traits<InputIt>::value_type;

        basic_csv_cursor<char_type,iterator_source<InputIt>> cursor(iterator_source<InputIt>(first, last), options);
        jsoncons::json_decoder<basic_json<char_type>> decoder;
        std::error_code ec;
        T val = decode_traits<T,char_type>::decode(cursor, decoder, ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec, cursor.context().line(), cursor.context().column()));
        }
        return val;
    }

    // With leading allocator_set parameter

    template <typename T,typename Source,typename Allocator,typename TempAllocator >
    typename std::enable_if<extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_sequence_of<Source,typename T::char_type>::value,T>::type 
    decode_csv(const allocator_set<Allocator,TempAllocator>& alloc_set,
               const Source& s, 
               const basic_csv_decode_options<typename Source::value_type>& options = basic_csv_decode_options<typename Source::value_type>())
    {
        using char_type = typename Source::value_type;

        json_decoder<T,TempAllocator> decoder(alloc_set.get_allocator(), alloc_set.get_temp_allocator());

        basic_csv_reader<char_type,jsoncons::string_source<char_type>,TempAllocator> reader(s,decoder,options,alloc_set.get_temp_allocator());
        reader.read();
        if (!decoder.is_valid())
        {
            JSONCONS_THROW(ser_error(conv_errc::conversion_failed, reader.line(), reader.column()));
        }
        return decoder.get_result();
    }

    template <typename T,typename Source,typename Allocator,typename TempAllocator >
    typename std::enable_if<!extension_traits::is_basic_json<T>::value &&
                            extension_traits::is_char_sequence<Source>::value,T>::type 
    decode_csv(const allocator_set<Allocator,TempAllocator>& alloc_set,
               const Source& s, 
               const basic_csv_decode_options<typename Source::value_type>& options = basic_csv_decode_options<typename Source::value_type>())
    {
        using char_type = typename Source::value_type;

        basic_csv_cursor<char_type,stream_source<char_type>,TempAllocator> cursor(s, options, alloc_set.get_temp_allocator());
        json_decoder<basic_json<char_type,sorted_policy,TempAllocator>,TempAllocator> decoder(alloc_set.get_temp_allocator(), alloc_set.get_temp_allocator());

        std::error_code ec;
        T val = decode_traits<T,char_type>::decode(cursor, decoder, ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec, cursor.context().line(), cursor.context().column()));
        }
        return val;
    }

    template <typename T,typename CharT,typename Allocator,typename TempAllocator >
    typename std::enable_if<extension_traits::is_basic_json<T>::value,T>::type 
    decode_csv(const allocator_set<Allocator,TempAllocator>& alloc_set,
               std::basic_istream<CharT>& is, 
               const basic_csv_decode_options<CharT>& options = basic_csv_decode_options<CharT>())
    {
        using char_type = CharT;

        json_decoder<T,TempAllocator> decoder(alloc_set.get_allocator(), alloc_set.get_temp_allocator());

        basic_csv_reader<char_type,jsoncons::string_source<char_type>,TempAllocator> reader(is,decoder,options,alloc_set.get_temp_allocator());
        reader.read();
        if (!decoder.is_valid())
        {
            JSONCONS_THROW(ser_error(conv_errc::conversion_failed, reader.line(), reader.column()));
        }
        return decoder.get_result();
    }

    template <typename T,typename CharT,typename Allocator,typename TempAllocator >
    typename std::enable_if<!extension_traits::is_basic_json<T>::value,T>::type 
    decode_csv(const allocator_set<Allocator,TempAllocator>& alloc_set,
               std::basic_istream<CharT>& is, 
               const basic_csv_decode_options<CharT>& options = basic_csv_decode_options<CharT>())
    {
        basic_csv_cursor<CharT,stream_source<CharT>,TempAllocator> cursor(is, options, alloc_set.get_temp_allocator());
        json_decoder<basic_json<CharT,sorted_policy,TempAllocator>,TempAllocator> decoder(alloc_set.get_temp_allocator(), alloc_set.get_temp_allocator());

        std::error_code ec;
        T val = decode_traits<T,CharT>::decode(cursor, decoder, ec);
        if (ec)
        {
            JSONCONS_THROW(ser_error(ec, cursor.context().line(), cursor.context().column()));
        }
        return val;
    }

} // namespace csv 
} // namespace jsoncons

#endif // JSONCONS_EXT_CSV_DECODE_CSV_HPP
