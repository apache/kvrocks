// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSON_TYPE_TRAITS_HPP
#define JSONCONS_JSON_TYPE_TRAITS_HPP

#include <algorithm> // std::swap
#include <array>
#include <bitset> // std::bitset
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iterator> // std::iterator_traits, std::input_iterator_tag
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits> // std::enable_if
#include <utility>
#include <valarray>
#include <vector>

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/utility/byte_string.hpp>
#include <jsoncons/conv_error.hpp>
#include <jsoncons/json_type.hpp>
#include <jsoncons/json_visitor.hpp>
#include <jsoncons/tag_type.hpp>
#include <jsoncons/utility/bigint.hpp>
#include <jsoncons/utility/extension_traits.hpp>
#include <jsoncons/value_converter.hpp>

#if defined(JSONCONS_HAS_STD_VARIANT)
  #include <variant>
#endif

namespace jsoncons {

    template <typename T>
    struct is_json_type_traits_declared : public std::false_type
    {};

    // json_type_traits

    template <typename T>
    struct unimplemented : std::false_type
    {};

    template <typename Json,typename T,typename Enable=void>
    struct json_type_traits
    {
        using allocator_type = typename Json::allocator_type;

        static constexpr bool is_compatible = false;

        static constexpr bool is(const Json&) noexcept
        {
            return false;
        }

        static T as(const Json&)
        {
            static_assert(unimplemented<T>::value, "as not implemented");
        }

        static Json to_json(const T&)
        {
            static_assert(unimplemented<T>::value, "to_json not implemented");
        }

        static Json to_json(const T&, const allocator_type&)
        {
            static_assert(unimplemented<T>::value, "to_json not implemented");
        }
    };

namespace detail {

template <typename Json,typename T>
using
traits_can_convert_t = decltype(json_type_traits<Json,T>::can_convert(Json()));

template <typename Json,typename T>
using
has_can_convert = extension_traits::is_detected<traits_can_convert_t, Json, T>;

    template <typename T>
    struct invoke_can_convert
    {
        template <typename Json>
        static 
        typename std::enable_if<has_can_convert<Json,T>::value,bool>::type
        can_convert(const Json& j) noexcept
        {
            return json_type_traits<Json,T>::can_convert(j);
        }
        template <typename Json>
        static 
        typename std::enable_if<!has_can_convert<Json,T>::value,bool>::type
        can_convert(const Json& j) noexcept
        {
            return json_type_traits<Json,T>::is(j);
        }
    };

    // is_json_type_traits_unspecialized
    template <typename Json,typename T,typename Enable = void>
    struct is_json_type_traits_unspecialized : std::false_type {};

    // is_json_type_traits_unspecialized
    template <typename Json,typename T>
    struct is_json_type_traits_unspecialized<Json,T,
        typename std::enable_if<!std::integral_constant<bool, json_type_traits<Json, T>::is_compatible>::value>::type
    > : std::true_type {};

    // is_compatible_array_type
    template <typename Json,typename T,typename Enable=void>
    struct is_compatible_array_type : std::false_type {};

    template <typename Json,typename T>
    struct is_compatible_array_type<Json,T, 
        typename std::enable_if<!std::is_same<T,typename Json::array>::value &&
        extension_traits::is_array_like<T>::value && 
        !is_json_type_traits_unspecialized<Json,typename std::iterator_traits<typename T::iterator>::value_type>::value
    >::type> : std::true_type {};

} // namespace detail

    // is_json_type_traits_specialized
    template <typename Json,typename T,typename Enable=void>
    struct is_json_type_traits_specialized : std::false_type {};

    template <typename Json,typename T>
    struct is_json_type_traits_specialized<Json,T, 
        typename std::enable_if<!jsoncons::detail::is_json_type_traits_unspecialized<Json,T>::value
    >::type> : std::true_type {};

    template <typename Json>
    struct json_type_traits<Json, const typename std::decay<typename Json::char_type>::type*>
    {
        using char_type = typename Json::char_type;
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_string();
        }
        static const char_type* as(const Json& j)
        {
            return j.as_cstring();
        }
        template <typename ... Args>
        static Json to_json(const char_type* s, Args&&... args)
        {
            return Json(s, semantic_tag::none, std::forward<Args>(args)...);
        }
    };

    template <typename Json>
    struct json_type_traits<Json,typename std::decay<typename Json::char_type>::type*>
    {
        using char_type = typename Json::char_type;
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_string();
        }
        template <typename ... Args>
        static Json to_json(const char_type* s, Args&&... args)
        {
            return Json(s, semantic_tag::none, std::forward<Args>(args)...);
        }
    };

    // integer

    template <typename Json,typename T>
    struct json_type_traits<Json, T,
        typename std::enable_if<(extension_traits::is_signed_integer<T>::value && sizeof(T) <= sizeof(int64_t)) || (extension_traits::is_unsigned_integer<T>::value && sizeof(T) <= sizeof(uint64_t)) 
    >::type>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.template is_integer<T>();
        }
        static T as(const Json& j)
        {
            return j.template as_integer<T>();
        }

        static Json to_json(T val)
        {
            return Json(val, semantic_tag::none);
        }

        static Json to_json(T val, const allocator_type&)
        {
            return Json(val, semantic_tag::none);
        }
    };

    template <typename Json,typename T>
    struct json_type_traits<Json, T,
        typename std::enable_if<(extension_traits::is_signed_integer<T>::value && sizeof(T) > sizeof(int64_t)) || (extension_traits::is_unsigned_integer<T>::value && sizeof(T) > sizeof(uint64_t)) 
    >::type>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.template is_integer<T>();
        }
        static T as(const Json& j)
        {
            return j.template as_integer<T>();
        }

        static Json to_json(T val, const allocator_type& alloc = allocator_type())
        {
            return Json(val, semantic_tag::none, alloc);
        }
    };

    template <typename Json,typename T>
    struct json_type_traits<Json, T,
                            typename std::enable_if<std::is_floating_point<T>::value
    >::type>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_double();
        }
        static T as(const Json& j)
        {
            return static_cast<T>(j.as_double());
        }
        static Json to_json(T val)
        {
            return Json(val, semantic_tag::none);
        }
        static Json to_json(T val, const allocator_type&)
        {
            return Json(val, semantic_tag::none);
        }
    };

    template <typename Json>
    struct json_type_traits<Json,typename Json::object>
    {
        using json_object = typename Json::object;
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_object();
        }
        static Json to_json(const json_object& o)
        {
            return Json(o,semantic_tag::none);
        }
        static Json to_json(const json_object& o, const allocator_type&)
        {
            return Json(o,semantic_tag::none);
        }
    };

    template <typename Json>
    struct json_type_traits<Json,typename Json::array>
    {
        using json_array = typename Json::array;
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_array();
        }
        static Json to_json(const json_array& a)
        {
            return Json(a, semantic_tag::none);
        }
        static Json to_json(const json_array& a, const allocator_type&)
        {
            return Json(a, semantic_tag::none);
        }
    };

    template <typename Json>
    struct json_type_traits<Json, Json>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json&) noexcept
        {
            return true;
        }
        static Json as(Json j)
        {
            return j;
        }
        static Json to_json(const Json& val)
        {
            return val;
        }
        static Json to_json(const Json& val, const allocator_type&)
        {
            return val;
        }
    };

    template <typename Json>
    struct json_type_traits<Json, jsoncons::null_type>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_null();
        }
        static typename jsoncons::null_type as(const Json& j)
        {
            if (!j.is_null())
            {
                JSONCONS_THROW(conv_error(conv_errc::not_jsoncons_null_type));
            }
            return jsoncons::null_type();
        }
        static Json to_json(jsoncons::null_type)
        {
            return Json(jsoncons::null_type{}, semantic_tag::none);
        }
        static Json to_json(jsoncons::null_type, const allocator_type&)
        {
            return Json(jsoncons::null_type{}, semantic_tag::none);
        }
    };

    template <typename Json>
    struct json_type_traits<Json, bool>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_bool();
        }
        static bool as(const Json& j)
        {
            return j.as_bool();
        }
        static Json to_json(bool val)
        {
            return Json(val, semantic_tag::none);
        }
        static Json to_json(bool val, const allocator_type&)
        {
            return Json(val, semantic_tag::none);
        }
    };

    template <typename Json,typename T>
    struct json_type_traits<Json, T,typename std::enable_if<std::is_same<T, 
        std::conditional<!std::is_same<bool,std::vector<bool>::const_reference>::value,
                         std::vector<bool>::const_reference,
                         void>::type>::value>::type>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_bool();
        }
        static bool as(const Json& j)
        {
            return j.as_bool();
        }
        static Json to_json(bool val)
        {
            return Json(val, semantic_tag::none);
        }
        static Json to_json(bool val, const allocator_type&)
        {
            return Json(val, semantic_tag::none);
        }
    };

    template <typename Json>
    struct json_type_traits<Json, std::vector<bool>::reference>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_bool();
        }
        static bool as(const Json& j)
        {
            return j.as_bool();
        }
        static Json to_json(bool val)
        {
            return Json(val, semantic_tag::none);
        }
        static Json to_json(bool val, const allocator_type&)
        {
            return Json(val, semantic_tag::none);
        }
    };

    template <typename Json,typename T>
    struct json_type_traits<Json, T, 
                            typename std::enable_if<!is_json_type_traits_declared<T>::value && 
                                                    extension_traits::is_string<T>::value &&
                                                    std::is_same<typename Json::char_type,typename T::value_type>::value>::type>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_string();
        }

        static T as(const Json& j)
        {
            return T(j.as_string());
        }

        static Json to_json(const T& val)
        {
            return Json(val, semantic_tag::none);
        }

        static Json to_json(const T& val, const allocator_type& alloc)
        {
            return Json(val, semantic_tag::none, alloc);
        }
    };

    template <typename Json,typename T>
    struct json_type_traits<Json, T, 
                            typename std::enable_if<!is_json_type_traits_declared<T>::value && 
                                                    extension_traits::is_string<T>::value &&
                                                    !std::is_same<typename Json::char_type,typename T::value_type>::value>::type>
    {
        using char_type = typename Json::char_type;
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_string();
        }

        static T as(const Json& j)
        {
            auto s = j.as_string();
            T val;
            unicode_traits::convert(s.data(), s.size(), val);
            return val;
        }

        static Json to_json(const T& val)
        {
            std::basic_string<char_type> s;
            unicode_traits::convert(val.data(), val.size(), s);

            return Json(s, semantic_tag::none);
        }

        static Json to_json(const T& val, const allocator_type& alloc)
        {
            std::basic_string<char_type> s;
            unicode_traits::convert(val.data(), val.size(), s);
            return Json(s, semantic_tag::none, alloc);
        }
    };

    template <typename Json,typename T>
    struct json_type_traits<Json, T, 
                            typename std::enable_if<!is_json_type_traits_declared<T>::value && 
                                                    extension_traits::is_string_view<T>::value &&
                                                    std::is_same<typename Json::char_type,typename T::value_type>::value>::type>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_string_view();
        }

        static T as(const Json& j)
        {
            return T(j.as_string_view().data(),j.as_string_view().size());
        }

        static Json to_json(const T& val)
        {
            return Json(val, semantic_tag::none);
        }

        static Json to_json(const T& val, const allocator_type& alloc)
        {
            return Json(val, semantic_tag::none, alloc);
        }
    };

    // array back insertable

    template <typename Json,typename T>
    struct json_type_traits<Json, T, 
                            typename std::enable_if<!is_json_type_traits_declared<T>::value && 
                                                    jsoncons::detail::is_compatible_array_type<Json,T>::value &&
                                                    extension_traits::is_back_insertable<T>::value 
                                                    >::type>
    {
        typedef typename std::iterator_traits<typename T::iterator>::value_type value_type;
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            bool result = j.is_array();
            if (result)
            {
                for (auto e : j.array_range())
                {
                    if (!e.template is<value_type>())
                    {
                        result = false;
                        break;
                    }
                }
            }
            return result;
        }

        // array back insertable non-byte container

        template <typename Container = T>
        static typename std::enable_if<!extension_traits::is_byte<typename Container::value_type>::value,Container>::type
        as(const Json& j)
        {
            if (j.is_array())
            {
                T result;
                visit_reserve_(typename std::integral_constant<bool, extension_traits::has_reserve<T>::value>::type(),result,j.size());
                for (const auto& item : j.array_range())
                {
                    result.push_back(item.template as<value_type>());
                }

                return result;
            }
            else 
            {
                JSONCONS_THROW(conv_error(conv_errc::not_vector));
            }
        }

        // array back insertable byte container

        template <typename Container = T>
        static typename std::enable_if<extension_traits::is_byte<typename Container::value_type>::value,Container>::type
        as(const Json& j)
        {
            std::error_code ec;
            if (j.is_array())
            {
                T result;
                visit_reserve_(typename std::integral_constant<bool, extension_traits::has_reserve<T>::value>::type(),result,j.size());
                for (const auto& item : j.array_range())
                {
                    result.push_back(item.template as<value_type>());
                }

                return result;
            }
            else if (j.is_byte_string_view())
            {
                value_converter<byte_string_view,T> converter;
                auto v = converter.convert(j.as_byte_string_view(),j.tag(), ec);
                if (ec)
                {
                    JSONCONS_THROW(conv_error(ec));
                }
                return v;
            }
            else if (j.is_string())
            {
                value_converter<basic_string_view<char>,T> converter;
                auto v = converter.convert(j.as_string_view(),j.tag(), ec);
                if (ec)
                {
                    JSONCONS_THROW(conv_error(ec));
                }
                return v;
            }
            else
            {
                JSONCONS_THROW(conv_error(conv_errc::not_vector));
            }
        }

        template <typename Container = T>
        static typename std::enable_if<!extension_traits::is_std_byte<typename Container::value_type>::value,Json>::type
        to_json(const T& val)
        {
            Json j(json_array_arg);
            auto first = std::begin(val);
            auto last = std::end(val);
            std::size_t size = std::distance(first,last);
            j.reserve(size);
            for (auto it = first; it != last; ++it)
            {
                j.push_back(*it);
            }
            return j;
        }

        template <typename Container = T>
        static typename std::enable_if<!extension_traits::is_std_byte<typename Container::value_type>::value,Json>::type
        to_json(const T& val, const allocator_type& alloc)
        {
            Json j(json_array_arg, alloc);
            auto first = std::begin(val);
            auto last = std::end(val);
            std::size_t size = std::distance(first, last);
            j.reserve(size);
            for (auto it = first; it != last; ++it)
            {
                j.push_back(*it);
            }
            return j;
        }

        template <typename Container = T>
        static typename std::enable_if<extension_traits::is_std_byte<typename Container::value_type>::value,Json>::type
        to_json(const T& val)
        {
            Json j(byte_string_arg, val);
            return j;
        }

        template <typename Container = T>
        static typename std::enable_if<extension_traits::is_std_byte<typename Container::value_type>::value,Json>::type
        to_json(const T& val, const allocator_type& alloc)
        {
            Json j(byte_string_arg, val, semantic_tag::none, alloc);
            return j;
        }

        static void visit_reserve_(std::true_type, T& v, std::size_t size)
        {
            v.reserve(size);
        }

        static void visit_reserve_(std::false_type, T&, std::size_t)
        {
        }
    };

    // array, not back insertable but insertable

    template <typename Json,typename T>
    struct json_type_traits<Json, T, 
                            typename std::enable_if<!is_json_type_traits_declared<T>::value && 
                                                    jsoncons::detail::is_compatible_array_type<Json,T>::value &&
                                                    !extension_traits::is_back_insertable<T>::value &&
                                                    extension_traits::is_insertable<T>::value>::type>
    {
        typedef typename std::iterator_traits<typename T::iterator>::value_type value_type;
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            bool result = j.is_array();
            if (result)
            {
                for (auto e : j.array_range())
                {
                    if (!e.template is<value_type>())
                    {
                        result = false;
                        break;
                    }
                }
            }
            return result;
        }

        static T as(const Json& j)
        {
            if (j.is_array())
            {
                T result;
                for (const auto& item : j.array_range())
                {
                    result.insert(item.template as<value_type>());
                }

                return result;
            }
            else 
            {
                JSONCONS_THROW(conv_error(conv_errc::not_vector));
            }
        }

        static Json to_json(const T& val)
        {
            Json j(json_array_arg);
            auto first = std::begin(val);
            auto last = std::end(val);
            std::size_t size = std::distance(first,last);
            j.reserve(size);
            for (auto it = first; it != last; ++it)
            {
                j.push_back(*it);
            }
            return j;
        }

        static Json to_json(const T& val, const allocator_type& alloc)
        {
            Json j(json_array_arg, alloc);
            auto first = std::begin(val);
            auto last = std::end(val);
            std::size_t size = std::distance(first, last);
            j.reserve(size);
            for (auto it = first; it != last; ++it)
            {
                j.push_back(*it);
            }
            return j;
        }
    };

    // array not back insertable or insertable, but front insertable

    template <typename Json,typename T>
    struct json_type_traits<Json, T, 
                            typename std::enable_if<!is_json_type_traits_declared<T>::value && 
                                                    jsoncons::detail::is_compatible_array_type<Json,T>::value &&
                                                    !extension_traits::is_back_insertable<T>::value &&
                                                    !extension_traits::is_insertable<T>::value &&
                                                    extension_traits::is_front_insertable<T>::value>::type>
    {
        typedef typename std::iterator_traits<typename T::iterator>::value_type value_type;
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            bool result = j.is_array();
            if (result)
            {
                for (auto e : j.array_range())
                {
                    if (!e.template is<value_type>())
                    {
                        result = false;
                        break;
                    }
                }
            }
            return result;
        }

        static T as(const Json& j)
        {
            if (j.is_array())
            {
                T result;

                auto it = j.array_range().rbegin();
                auto end = j.array_range().rend();
                for (; it != end; ++it)
                {
                    result.push_front((*it).template as<value_type>());
                }

                return result;
            }
            else 
            {
                JSONCONS_THROW(conv_error(conv_errc::not_vector));
            }
        }

        static Json to_json(const T& val)
        {
            Json j(json_array_arg);
            auto first = std::begin(val);
            auto last = std::end(val);
            std::size_t size = std::distance(first,last);
            j.reserve(size);
            for (auto it = first; it != last; ++it)
            {
                j.push_back(*it);
            }
            return j;
        }

        static Json to_json(const T& val, const allocator_type& alloc)
        {
            Json j(json_array_arg, alloc);
            auto first = std::begin(val);
            auto last = std::end(val);
            std::size_t size = std::distance(first, last);
            j.reserve(size);
            for (auto it = first; it != last; ++it)
            {
                j.push_back(*it);
            }
            return j;
        }
    };

    // std::array

    template <typename Json,typename E, std::size_t N>
    struct json_type_traits<Json, std::array<E, N>>
    {
        using allocator_type = typename Json::allocator_type;

        using value_type = E;

        static bool is(const Json& j) noexcept
        {
            bool result = j.is_array() && j.size() == N;
            if (result)
            {
                for (auto e : j.array_range())
                {
                    if (!e.template is<value_type>())
                    {
                        result = false;
                        break;
                    }
                }
            }
            return result;
        }

        static std::array<E, N> as(const Json& j)
        {
            std::array<E, N> buff;
            if (j.size() != N)
            {
                JSONCONS_THROW(conv_error(conv_errc::not_array));
            }
            for (std::size_t i = 0; i < N; i++)
            {
                buff[i] = j[i].template as<E>();
            }
            return buff;
        }

        static Json to_json(const std::array<E, N>& val)
        {
            Json j(json_array_arg);
            j.reserve(N);
            for (auto it = val.begin(); it != val.end(); ++it)
            {
                j.push_back(*it);
            }
            return j;
        }

        static Json to_json(const std::array<E, N>& val, 
                            const allocator_type& alloc)
        {
            Json j(json_array_arg, alloc);
            j.reserve(N);
            for (auto it = val.begin(); it != val.end(); ++it)
            {
                j.push_back(*it);
            }
            return j;
        }
    };

    // map like
    template <typename Json,typename T>
    struct json_type_traits<Json, T, 
                            typename std::enable_if<!is_json_type_traits_declared<T>::value && 
                                                    extension_traits::is_map_like<T>::value &&
                                                    extension_traits::is_constructible_from_const_pointer_and_size<typename T::key_type>::value &&
                                                    is_json_type_traits_specialized<Json,typename T::mapped_type>::value>::type
    >
    {
        using mapped_type = typename T::mapped_type;
        using value_type = typename T::value_type;
        using key_type = typename T::key_type;
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            bool result = j.is_object();
            for (auto member : j.object_range())
            {
                if (!member.value().template is<mapped_type>())
                {
                    result = false;
                }
            }
            return result;
        }

        static T as(const Json& j)
        {
            if (!j.is_object())
            {
                JSONCONS_THROW(conv_error(conv_errc::not_map));
            }
            T result;
            for (const auto& item : j.object_range())
            {
                result.emplace(key_type(item.key().data(),item.key().size()), item.value().template as<mapped_type>());
            }

            return result;
        }

        static Json to_json(const T& val)
        {
            Json j(json_object_arg, val.begin(), val.end());
            return j;
        }

        static Json to_json(const T& val, const allocator_type& alloc)
        {
            Json j(json_object_arg, val.begin(), val.end(), alloc);
            return j;
        }
    };

    template <typename Json,typename T>
    struct json_type_traits<Json, T, 
                            typename std::enable_if<!is_json_type_traits_declared<T>::value && 
                                                    extension_traits::is_map_like<T>::value &&
                                                    !extension_traits::is_constructible_from_const_pointer_and_size<typename T::key_type>::value &&
                                                    is_json_type_traits_specialized<Json,typename T::key_type>::value &&
                                                    is_json_type_traits_specialized<Json,typename T::mapped_type>::value>::type
    >
    {
        using mapped_type = typename T::mapped_type;
        using value_type = typename T::value_type;
        using key_type = typename T::key_type;
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& val) noexcept 
        {
            if (!val.is_object())
                return false;
            for (const auto& item : val.object_range())
            {
                Json j(item.key());
                if (!j.template is<key_type>())
                {
                    return false;
                }
                if (!item.value().template is<mapped_type>())
                {
                    return false;
                }
            }
            return true;
        }

        static T as(const Json& val) 
        {
            T result;
            for (const auto& item : val.object_range())
            {
                Json j(item.key());
                auto key = json_type_traits<Json,key_type>::as(j);
                result.emplace(std::move(key), item.value().template as<mapped_type>());
            }

            return result;
        }

        static Json to_json(const T& val) 
        {
            Json j(json_object_arg);
            j.reserve(val.size());
            for (const auto& item : val)
            {
                auto temp = json_type_traits<Json,key_type>::to_json(item.first);
                if (temp.is_string_view())
                {
                    j.try_emplace(typename Json::key_type(temp.as_string_view()), item.second);
                }
                else
                {
                    typename Json::key_type key;
                    temp.dump(key);
                    j.try_emplace(std::move(key), item.second);
                }
            }
            return j;
        }

        static Json to_json(const T& val, const allocator_type& alloc) 
        {
            Json j(json_object_arg, semantic_tag::none, alloc);
            j.reserve(val.size());
            for (const auto& item : val)
            {
                auto temp = json_type_traits<Json, key_type>::to_json(item.first, alloc);
                if (temp.is_string_view())
                {
                    j.try_emplace(typename Json::key_type(temp.as_string_view(), alloc), item.second);
                }
                else
                {
                    typename Json::key_type key(alloc);
                    temp.dump(key);
                    j.try_emplace(std::move(key), item.second, alloc);
                }
            }
            return j;
        }
    };

    namespace tuple_detail
    {
        template<size_t Pos, std::size_t Size,typename Json,typename Tuple>
        struct json_tuple_helper
        {
            using element_type = typename std::tuple_element<Size-Pos, Tuple>::type;
            using next = json_tuple_helper<Pos-1, Size, Json, Tuple>;
            
            static bool is(const Json& j) noexcept
            {
                if (j[Size-Pos].template is<element_type>())
                {
                    return next::is(j);
                }
                else
                {
                    return false;
                }
            }

            static void as(Tuple& tuple, const Json& j)
            {
                std::get<Size-Pos>(tuple) = j[Size-Pos].template as<element_type>();
                next::as(tuple, j);
            }

            static void to_json(const Tuple& tuple, Json& j)
            {
                j.push_back(json_type_traits<Json, element_type>::to_json(std::get<Size-Pos>(tuple)));
                next::to_json(tuple, j);
            }
        };

        template<size_t Size,typename Json,typename Tuple>
        struct json_tuple_helper<0, Size, Json, Tuple>
        {
            static bool is(const Json&) noexcept
            {
                return true;
            }

            static void as(Tuple&, const Json&)
            {
            }

            static void to_json(const Tuple&, Json&)
            {
            }
        };
    } // namespace tuple_detail

    template <typename Json,typename... E>
    struct json_type_traits<Json, std::tuple<E...>>
    {
    private:
        using helper = tuple_detail::json_tuple_helper<sizeof...(E), sizeof...(E), Json, std::tuple<E...>>;

    public:
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return helper::is(j);
        }
        
        static std::tuple<E...> as(const Json& j)
        {
            std::tuple<E...> buff;
            helper::as(buff, j);
            return buff;
        }
         
        static Json to_json(const std::tuple<E...>& val)
        {
            Json j(json_array_arg);
            j.reserve(sizeof...(E));
            helper::to_json(val, j);
            return j;
        }

        static Json to_json(const std::tuple<E...>& val,
                            const allocator_type& alloc)
        {
            Json j(json_array_arg, alloc);
            j.reserve(sizeof...(E));
            helper::to_json(val, j);
            return j;
        }
    };

    template <typename Json,typename T1,typename T2>
    struct json_type_traits<Json, std::pair<T1,T2>>
    {
    public:
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_array() && j.size() == 2;
        }
        
        static std::pair<T1,T2> as(const Json& j)
        {
            return std::make_pair<T1,T2>(j[0].template as<T1>(),j[1].template as<T2>());
        }
        
        static Json to_json(const std::pair<T1,T2>& val)
        {
            Json j(json_array_arg);
            j.reserve(2);
            j.push_back(val.first);
            j.push_back(val.second);
            return j;
        }

        static Json to_json(const std::pair<T1, T2>& val, const allocator_type& alloc)
        {
            Json j(json_array_arg, alloc);
            j.reserve(2);
            j.push_back(val.first);
            j.push_back(val.second);
            return j;
        }
    };

    template <typename Json,typename T>
    struct json_type_traits<Json, T,
                            typename std::enable_if<extension_traits::is_basic_byte_string<T>::value>::type>
    {
    public:
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_byte_string();
        }
        
        static T as(const Json& j)
        { 
            return j.template as_byte_string<typename T::allocator_type>();
        }
        
        static Json to_json(const T& val, 
                            const allocator_type& alloc = allocator_type())
        {
            return Json(byte_string_arg, val, semantic_tag::none, alloc);
        }
    };

    template <typename Json,typename ValueType>
    struct json_type_traits<Json, std::shared_ptr<ValueType>,
                            typename std::enable_if<!is_json_type_traits_declared<std::shared_ptr<ValueType>>::value &&
                                                    !std::is_polymorphic<ValueType>::value
    >::type>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_null() || j.template is<ValueType>();
        }

        static std::shared_ptr<ValueType> as(const Json& j) 
        {
            return j.is_null() ? std::shared_ptr<ValueType>(nullptr) : std::make_shared<ValueType>(j.template as<ValueType>());
        }

        static Json to_json(const std::shared_ptr<ValueType>& ptr, 
            const allocator_type& alloc = allocator_type())
        {
            if (ptr.get() != nullptr) 
            {
                Json j(*ptr, alloc);
                return j;
            }
            else 
            {
                return Json::null();
            }
        }
    };

    template <typename Json,typename ValueType>
    struct json_type_traits<Json, std::unique_ptr<ValueType>,
                            typename std::enable_if<!is_json_type_traits_declared<std::unique_ptr<ValueType>>::value &&
                                                    !std::is_polymorphic<ValueType>::value
    >::type>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_null() || j.template is<ValueType>();
        }

        static std::unique_ptr<ValueType> as(const Json& j) 
        {
            return j.is_null() ? std::unique_ptr<ValueType>(nullptr) : jsoncons::make_unique<ValueType>(j.template as<ValueType>());
        }

        static Json to_json(const std::unique_ptr<ValueType>& ptr,
            const allocator_type& alloc = allocator_type())
        {
            if (ptr.get() != nullptr) 
            {
                Json j(*ptr, alloc);
                return j;
            }
            else 
            {
                return Json::null();
            }
        }
    };

    template <typename Json,typename T>
    struct json_type_traits<Json, jsoncons::optional<T>,
                            typename std::enable_if<!is_json_type_traits_declared<jsoncons::optional<T>>::value>::type>
    {
        using allocator_type = typename Json::allocator_type;
    public:
        static bool is(const Json& j) noexcept
        {
            return j.is_null() || j.template is<T>();
        }
        
        static jsoncons::optional<T> as(const Json& j)
        { 
            return j.is_null() ? jsoncons::optional<T>() : jsoncons::optional<T>(j.template as<T>());
        }
        
        static Json to_json(const jsoncons::optional<T>& val, 
            const allocator_type& alloc = allocator_type())
        {
            return val.has_value() ? Json(*val, alloc) : Json::null();
        }
    };

    template <typename Json>
    struct json_type_traits<Json, byte_string_view>
    {
        using allocator_type = typename Json::allocator_type;

    public:
        static bool is(const Json& j) noexcept
        {
            return j.is_byte_string_view();
        }
        
        static byte_string_view as(const Json& j)
        {
            return j.as_byte_string_view();
        }
        
        static Json to_json(const byte_string_view& val, const allocator_type& alloc = allocator_type())
        {
            return Json(byte_string_arg, val, semantic_tag::none, alloc);
        }
    };

    // basic_bigint

    template <typename Json,typename Allocator>
    struct json_type_traits<Json, basic_bigint<Allocator>>
    {
    public:
        using allocator_type = typename Json::allocator_type;
        using char_type = typename Json::char_type;

        static bool is(const Json& j) noexcept
        {
            switch (j.type())
            {
                case json_type::string_value:
                    return jsoncons::detail::is_base10(j.as_string_view().data(), j.as_string_view().length());
                case json_type::int64_value:
                case json_type::uint64_value:
                    return true;
                default:
                    return false;
            }
        }
        
        static basic_bigint<Allocator> as(const Json& j)
        {
            switch (j.type())
            {
                case json_type::string_value:
                    if (!jsoncons::detail::is_base10(j.as_string_view().data(), j.as_string_view().length()))
                    {
                        JSONCONS_THROW(conv_error(conv_errc::not_bigint));
                    }
                    return basic_bigint<Allocator>::from_string(j.as_string_view().data(), j.as_string_view().length());
                case json_type::half_value:
                case json_type::double_value:
                    return basic_bigint<Allocator>(j.template as<int64_t>());
                case json_type::int64_value:
                    return basic_bigint<Allocator>(j.template as<int64_t>());
                case json_type::uint64_value:
                    return basic_bigint<Allocator>(j.template as<uint64_t>());
                default:
                    JSONCONS_THROW(conv_error(conv_errc::not_bigint));
            }
        }
        
        static Json to_json(const basic_bigint<Allocator>& val)
        {
            std::basic_string<char_type> s;
            val.write_string(s);
            return Json(s,semantic_tag::bigint);
        }

        static Json to_json(const basic_bigint<Allocator>& val, const allocator_type&)
        {
            std::basic_string<char_type> s;
            val.write_string(s);
            return Json(s,semantic_tag::bigint);
        }
    };

    // std::valarray

    template <typename Json,typename T>
    struct json_type_traits<Json, std::valarray<T>>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            bool result = j.is_array();
            if (result)
            {
                for (auto e : j.array_range())
                {
                    if (!e.template is<T>())
                    {
                        result = false;
                        break;
                    }
                }
            }
            return result;
        }
        
        static std::valarray<T> as(const Json& j)
        {
            if (j.is_array())
            {
                std::valarray<T> v(j.size());
                for (std::size_t i = 0; i < j.size(); ++i)
                {
                    v[i] = j[i].template as<T>();
                }
                return v;
            }
            else
            {
                JSONCONS_THROW(conv_error(conv_errc::not_array));
            }
        }
        
        static Json to_json(const std::valarray<T>& val)
        {
            Json j(json_array_arg);
            auto first = std::begin(val);
            auto last = std::end(val);
            std::size_t size = std::distance(first,last);
            j.reserve(size);
            for (auto it = first; it != last; ++it)
            {
                j.push_back(*it);
            }
            return j;
        } 

        static Json to_json(const std::valarray<T>& val, const allocator_type& alloc)
        {
            Json j(json_array_arg, alloc);
            auto first = std::begin(val);
            auto last = std::end(val);
            std::size_t size = std::distance(first,last);
            j.reserve(size);
            for (auto it = first; it != last; ++it)
            {
                j.push_back(*it);
            }
            return j;
        }
    };

#if defined(JSONCONS_HAS_STD_VARIANT)

namespace variant_detail
{
    template<int N,typename Json,typename Variant,typename ... Args>
    typename std::enable_if<N == std::variant_size_v<Variant>, bool>::type
    is_variant(const Json& /*j*/)
    {
        return false;
    }

    template<std::size_t N,typename Json,typename Variant,typename T,typename ... U>
    typename std::enable_if<N < std::variant_size_v<Variant>, bool>::type
    is_variant(const Json& j)
    {
      if (j.template is<T>())
      {
          return true;
      }
      else
      {
          return is_variant<N+1, Json, Variant, U...>(j);
      }
    }

    template<int N,typename Json,typename Variant,typename ... Args>
    typename std::enable_if<N == std::variant_size_v<Variant>, Variant>::type
    as_variant(const Json& /*j*/)
    {
        JSONCONS_THROW(conv_error(conv_errc::not_variant));
    }

    template<std::size_t N,typename Json,typename Variant,typename T,typename ... U>
    typename std::enable_if<N < std::variant_size_v<Variant>, Variant>::type
    as_variant(const Json& j)
    {
      if (j.template is<T>())
      {
        Variant var(j.template as<T>());
        return var;
      }
      else
      {
          return as_variant<N+1, Json, Variant, U...>(j);
      }
    }

    template <typename Json>
    struct variant_to_json_visitor
    {
        Json& j_;

        variant_to_json_visitor(Json& j) : j_(j) {}

        template <typename T>
        void operator()(const T& value) const
        {
            j_ = value;
        }
    };

} // namespace variant_detail

    template <typename Json,typename... VariantTypes>
    struct json_type_traits<Json, std::variant<VariantTypes...>>
    {
    public:
        using variant_type = typename std::variant<VariantTypes...>;
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return variant_detail::is_variant<0,Json,variant_type, VariantTypes...>(j); 
        }

        static std::variant<VariantTypes...> as(const Json& j)
        {
            return variant_detail::as_variant<0,Json,variant_type, VariantTypes...>(j); 
        }

        static Json to_json(const std::variant<VariantTypes...>& var)
        {
            Json j(json_array_arg);
            variant_detail::variant_to_json_visitor<Json> visitor(j);
            std::visit(visitor, var);
            return j;
        }

        static Json to_json(const std::variant<VariantTypes...>& var,
                            const allocator_type& alloc)
        {
            Json j(json_array_arg, alloc);
            variant_detail::variant_to_json_visitor<Json> visitor(j);
            std::visit(visitor, var);
            return j;
        }
    };
#endif

    // std::chrono::duration
    template <typename Json,typename Rep,typename Period>
    struct json_type_traits<Json,std::chrono::duration<Rep,Period>>
    {
        using duration_type = std::chrono::duration<Rep,Period>;

        using allocator_type = typename Json::allocator_type;

        static constexpr int64_t nanos_in_milli = 1000000;
        static constexpr int64_t nanos_in_second = 1000000000;
        static constexpr int64_t millis_in_second = 1000;

        static bool is(const Json& j) noexcept
        {
            return (j.tag() == semantic_tag::epoch_second || j.tag() == semantic_tag::epoch_milli || j.tag() == semantic_tag::epoch_nano);
        }

        static duration_type as(const Json& j)
        {
            return from_json_(j);
        }

        static Json to_json(const duration_type& val, const allocator_type& = allocator_type())
        {
            return to_json_(val);
        }

        template <typename PeriodT=Period>
        static 
        typename std::enable_if<std::is_same<PeriodT,std::ratio<1>>::value, duration_type>::type
        from_json_(const Json& j)
        {
            if (j.is_int64() || j.is_uint64() || j.is_double())
            {
                auto count = j.template as<Rep>();
                switch (j.tag())
                {
                    case semantic_tag::epoch_second:
                        return duration_type(count);
                    case semantic_tag::epoch_milli:
                        return duration_type(count == 0 ? 0 : count/millis_in_second);
                    case semantic_tag::epoch_nano:
                        return duration_type(count == 0 ? 0 : count/nanos_in_second);
                    default:
                        return duration_type(count);
                }
            }
            else if (j.is_string())
            {
                switch (j.tag())
                {
                    case semantic_tag::epoch_second:
                    {
                        auto count = j.template as<Rep>();
                        return duration_type(count);
                    }
                    case semantic_tag::epoch_milli:
                    {
                        auto sv = j.as_string_view();
                        bigint n = bigint::from_string(sv.data(), sv.length());
                        if (n != 0)
                        {
                            n = n / millis_in_second;
                        }
                        return duration_type(static_cast<Rep>(n));
                    }
                    case semantic_tag::epoch_nano:
                    {
                        auto sv = j.as_string_view();
                        bigint n = bigint::from_string(sv.data(), sv.length());
                        if (n != 0)
                        {
                            n = n / nanos_in_second;
                        }
                        return duration_type(static_cast<Rep>(n));
                    }
                    default:
                    {
                        auto count = j.template as<Rep>();
                        return duration_type(count);
                    }
                }
            }
            else
            {
                return duration_type();
            }
        }

        template <typename PeriodT=Period>
        static 
        typename std::enable_if<std::is_same<PeriodT,std::milli>::value, duration_type>::type
        from_json_(const Json& j)
        {
            if (j.is_int64() || j.is_uint64())
            {
                auto count = j.template as<Rep>();
                switch (j.tag())
                {
                    case semantic_tag::epoch_second:
                        return duration_type(count*millis_in_second);
                    case semantic_tag::epoch_milli:
                        return duration_type(count);
                    case semantic_tag::epoch_nano:
                        return duration_type(count == 0 ? 0 : count/nanos_in_milli);
                    default:
                        return duration_type(count);
                }
            }
            else if (j.is_double())
            {
                auto count = j.template as<double>();
                switch (j.tag())
                {
                    case semantic_tag::epoch_second:
                        return duration_type(static_cast<Rep>(count * millis_in_second));
                    case semantic_tag::epoch_milli:
                        return duration_type(static_cast<Rep>(count));
                    case semantic_tag::epoch_nano:
                        return duration_type(count == 0 ? 0 : static_cast<Rep>(count / nanos_in_milli));
                    default:
                        return duration_type(static_cast<Rep>(count));
                }
            }
            else if (j.is_string())
            {
                switch (j.tag())
                {
                    case semantic_tag::epoch_second:
                    {
                        auto count = j.template as<Rep>();
                        return duration_type(count*millis_in_second);
                    }
                    case semantic_tag::epoch_milli:
                    {
                        auto sv = j.as_string_view();
                        Rep n{0};
                        auto result = jsoncons::detail::decimal_to_integer(sv.data(), sv.size(), n);
                        if (!result)
                        {
                            return duration_type();
                        }
                        return duration_type(n);
                    }
                    case semantic_tag::epoch_nano:
                    {
                        auto sv = j.as_string_view();
                        bigint n = bigint::from_string(sv.data(), sv.length());
                        if (n != 0)
                        {
                            n = n / nanos_in_milli;
                        }
                        return duration_type(static_cast<Rep>(n));
                    }
                    default:
                    {
                        auto count = j.template as<Rep>();
                        return duration_type(count);
                    }
                }
            }
            else
            {
                return duration_type();
            }
        }

        template <typename PeriodT=Period>
        static 
        typename std::enable_if<std::is_same<PeriodT,std::nano>::value, duration_type>::type
        from_json_(const Json& j)
        {
            if (j.is_int64() || j.is_uint64() || j.is_double())
            {
                auto count = j.template as<Rep>();
                switch (j.tag())
                {
                    case semantic_tag::epoch_second:
                        return duration_type(count*nanos_in_second);
                    case semantic_tag::epoch_milli:
                        return duration_type(count*nanos_in_milli);
                    case semantic_tag::epoch_nano:
                        return duration_type(count);
                    default:
                        return duration_type(count);
                }
            }
            else if (j.is_double())
            {
                auto count = j.template as<double>();
                switch (j.tag())
                {
                    case semantic_tag::epoch_second:
                        return duration_type(static_cast<Rep>(count * nanos_in_second));
                    case semantic_tag::epoch_milli:
                        return duration_type(static_cast<Rep>(count * nanos_in_milli));
                    case semantic_tag::epoch_nano:
                        return duration_type(static_cast<Rep>(count));
                    default:
                        return duration_type(static_cast<Rep>(count));
                }
            }
            else if (j.is_string())
            {
                auto count = j.template as<Rep>();
                switch (j.tag())
                {
                    case semantic_tag::epoch_second:
                        return duration_type(count*nanos_in_second);
                    case semantic_tag::epoch_milli:
                        return duration_type(count*nanos_in_milli);
                    case semantic_tag::epoch_nano:
                        return duration_type(count);
                    default:
                        return duration_type(count);
                }
            }
            else
            {
                return duration_type();
            }
        }

        template <typename PeriodT=Period>
        static 
        typename std::enable_if<std::is_same<PeriodT,std::ratio<1>>::value,Json>::type
        to_json_(const duration_type& val)
        {
            return Json(val.count(), semantic_tag::epoch_second);
        }

        template <typename PeriodT=Period>
        static 
        typename std::enable_if<std::is_same<PeriodT,std::milli>::value,Json>::type
        to_json_(const duration_type& val)
        {
            return Json(val.count(), semantic_tag::epoch_milli);
        }

        template <typename PeriodT=Period>
        static 
        typename std::enable_if<std::is_same<PeriodT,std::nano>::value,Json>::type
        to_json_(const duration_type& val)
        {
            return Json(val.count(), semantic_tag::epoch_nano);
        }
    };

    // std::nullptr_t
    template <typename Json>
    struct json_type_traits<Json,std::nullptr_t>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_null();
        }

        static std::nullptr_t as(const Json& j)
        {
            if (!j.is_null())
            {
                JSONCONS_THROW(conv_error(conv_errc::not_nullptr));
            }
            return nullptr;
        }

        static Json to_json(const std::nullptr_t&, const allocator_type& = allocator_type())
        {
            return Json::null();
        }
    };

    // std::bitset

    struct null_back_insertable_byte_container
    {
        using value_type = uint8_t;

        void push_back(value_type)
        {
        }
    };

    template <typename Json, std::size_t N>
    struct json_type_traits<Json, std::bitset<N>>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            if (j.is_byte_string())
            {
                return true;
            }
            else if (j.is_string())
            {
                jsoncons::string_view sv = j.as_string_view();
                null_back_insertable_byte_container cont;
                auto result = decode_base16(sv.begin(), sv.end(), cont);
                return result.ec == conv_errc::success ? true : false;
            }
            return false;
        }

        static std::bitset<N> as(const Json& j)
        {
            if (j.template is<uint64_t>())
            {
                auto bits = j.template as<uint64_t>();
                std::bitset<N> bs = static_cast<unsigned long long>(bits);
                return bs;
            }
            else if (j.is_byte_string() || j.is_string())
            {
                std::bitset<N> bs;
                std::vector<uint8_t> bits;
                if (j.is_byte_string())
                {
                    bits = j.template as<std::vector<uint8_t>>();
                }
                else
                {
                    jsoncons::string_view sv = j.as_string_view();
                    auto result = decode_base16(sv.begin(), sv.end(), bits);
                    if (result.ec != conv_errc::success)
                    {
                        JSONCONS_THROW(conv_error(conv_errc::not_bitset));
                    }
                }
                std::uint8_t byte = 0;
                std::uint8_t mask  = 0;

                std::size_t pos = 0;
                for (std::size_t i = 0; i < N; ++i)
                {
                    if (mask == 0)
                    {
                        if (pos >= bits.size())
                        {
                            JSONCONS_THROW(conv_error(conv_errc::not_bitset));
                        }
                        byte = bits.at(pos++);
                        mask = 0x80;
                    }

                    if (byte & mask)
                    {
                        bs[i] = 1;
                    }

                    mask = static_cast<std::uint8_t>(mask >> 1);
                }
                return bs;
            }
            else
            {
                JSONCONS_THROW(conv_error(conv_errc::not_bitset));
            }
        }

        static Json to_json(const std::bitset<N>& val, 
                            const allocator_type& alloc = allocator_type())
        {
            std::vector<uint8_t> bits;

            uint8_t byte = 0;
            uint8_t mask = 0x80;

            for (std::size_t i = 0; i < N; ++i)
            {
                if (val[i])
                {
                    byte |= mask;
                }

                mask = static_cast<uint8_t>(mask >> 1);

                if (mask == 0)
                {
                    bits.push_back(byte);
                    byte = 0;
                    mask = 0x80;
                }
            }

            // Encode remainder
            if (mask != 0x80)
            {
                bits.push_back(byte);
            }

            Json j(byte_string_arg, bits, semantic_tag::base16, alloc);
            return j;
        }
    };

} // namespace jsoncons

#endif // JSONCONS_JSON_TYPE_TRAITS_HPP
