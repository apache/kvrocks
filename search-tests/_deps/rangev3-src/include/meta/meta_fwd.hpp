/// \file meta_fwd.hpp Forward declarations
//
// Meta library
//
//  Copyright Eric Niebler 2014-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/meta
//

#ifndef META_FWD_HPP
#define META_FWD_HPP

#include <type_traits>
#include <utility>

#ifdef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-variable-declarations"
#endif

#define META_CXX_STD_14 201402L
#define META_CXX_STD_17 201703L

#if defined(_MSVC_LANG) && _MSVC_LANG > __cplusplus // Older clangs define _MSVC_LANG < __cplusplus
#define META_CXX_VER _MSVC_LANG
#else
#define META_CXX_VER __cplusplus
#endif

#if defined(__apple_build_version__) || defined(__clang__)
#if defined(__apple_build_version__) || (defined(__clang__) && __clang_major__ < 6)
#define META_WORKAROUND_LLVM_28385 // https://llvm.org/bugs/show_bug.cgi?id=28385
#endif

#elif defined(_MSC_VER)
#define META_HAS_MAKE_INTEGER_SEQ 1
#if _MSC_VER < 1920
#define META_WORKAROUND_MSVC_702792 // Bogus C4018 comparing constant expressions with dependent type
#define META_WORKAROUND_MSVC_703656 // ICE with pack expansion inside decltype in alias template
#endif

#if _MSC_VER < 1921
#define META_WORKAROUND_MSVC_756112 // fold expression + alias templates in template argument
#endif

#elif defined(__GNUC__)
#define META_WORKAROUND_GCC_86356 // https://gcc.gnu.org/bugzilla/show_bug.cgi?id=86356
#if __GNUC__ < 8
#define META_WORKAROUND_GCC_UNKNOWN1 // Older GCCs don't like fold + debug + -march=native
#endif
#if __GNUC__ == 5 && __GNUC_MINOR__ == 1
#define META_WORKAROUND_GCC_66405 // https://gcc.gnu.org/bugzilla/show_bug.cgi?id=66405
#endif
#if __GNUC__ < 5
#define META_WORKAROUND_CWG_1558 // https://wg21.link/cwg1558
#endif
#endif

#ifndef META_CXX_VARIABLE_TEMPLATES
#ifdef __cpp_variable_templates
#define META_CXX_VARIABLE_TEMPLATES __cpp_variable_templates
#else
#define META_CXX_VARIABLE_TEMPLATES (META_CXX_VER >= META_CXX_STD_14)
#endif
#endif

#ifndef META_CXX_INLINE_VARIABLES
#ifdef __cpp_inline_variables
#define META_CXX_INLINE_VARIABLES __cpp_inline_variables
#else
#define META_CXX_INLINE_VARIABLES (META_CXX_VER >= META_CXX_STD_17)
#endif
#endif

#ifndef META_INLINE_VAR
#if META_CXX_INLINE_VARIABLES
#define META_INLINE_VAR inline
#else
#define META_INLINE_VAR
#endif
#endif

#ifndef META_CXX_INTEGER_SEQUENCE
#ifdef __cpp_lib_integer_sequence
#define META_CXX_INTEGER_SEQUENCE __cpp_lib_integer_sequence
#else
#define META_CXX_INTEGER_SEQUENCE (META_CXX_VER >= META_CXX_STD_14)
#endif
#endif

#ifndef META_HAS_MAKE_INTEGER_SEQ
#ifdef __has_builtin
#if __has_builtin(__make_integer_seq)
#define META_HAS_MAKE_INTEGER_SEQ 1
#endif
#endif
#endif
#ifndef META_HAS_MAKE_INTEGER_SEQ
#define META_HAS_MAKE_INTEGER_SEQ 0
#endif

#ifndef META_HAS_TYPE_PACK_ELEMENT
#ifdef __has_builtin
#if __has_builtin(__type_pack_element)
#define META_HAS_TYPE_PACK_ELEMENT 1
#endif
#endif
#endif
#ifndef META_HAS_TYPE_PACK_ELEMENT
#define META_HAS_TYPE_PACK_ELEMENT 0
#endif

#if !defined(META_DEPRECATED) && !defined(META_DISABLE_DEPRECATED_WARNINGS)
#if defined(__cpp_attribute_deprecated) || META_CXX_VER >= META_CXX_STD_14
#define META_DEPRECATED(...) [[deprecated(__VA_ARGS__)]]
#elif defined(__clang__) || defined(__GNUC__)
#define META_DEPRECATED(...) __attribute__((deprecated(__VA_ARGS__)))
#endif
#endif
#ifndef META_DEPRECATED
#define META_DEPRECATED(...)
#endif

#ifndef META_CXX_FOLD_EXPRESSIONS
#ifdef __cpp_fold_expressions
#define META_CXX_FOLD_EXPRESSIONS __cpp_fold_expressions
#else
#define META_CXX_FOLD_EXPRESSIONS (META_CXX_VER >= META_CXX_STD_17)
#endif
#endif

#if META_CXX_FOLD_EXPRESSIONS
#if !META_CXX_VARIABLE_TEMPLATES
#error Fold expressions, but no variable templates?
#endif
#endif

#if (defined(__cpp_concepts) && __cpp_concepts > 0) || defined(META_DOXYGEN_INVOKED)
#if !META_CXX_VARIABLE_TEMPLATES
#error Concepts, but no variable templates?
#endif
#if __cpp_concepts <= 201507L && !defined(META_DOXYGEN_INVOKED)
#define META_CONCEPT concept bool
// TS concepts subsumption barrier for atomic expressions
#define META_CONCEPT_BARRIER(...) ::meta::detail::barrier<__VA_ARGS__>
#define META_TYPE_CONSTRAINT(...) typename
#else
#define META_CONCEPT concept
#define META_CONCEPT_BARRIER(...) __VA_ARGS__
#define META_TYPE_CONSTRAINT(...) __VA_ARGS__
#endif
#else
#define META_TYPE_CONSTRAINT(...) typename
#endif

#if (defined(__cpp_lib_type_trait_variable_templates) && \
    __cpp_lib_type_trait_variable_templates > 0)
#define META_CXX_TRAIT_VARIABLE_TEMPLATES 1
#else
#define META_CXX_TRAIT_VARIABLE_TEMPLATES 0
#endif

#if defined(__clang__)
#define META_IS_SAME(...) __is_same(__VA_ARGS__)
#elif defined(__GNUC__) && __GNUC__ >= 6
#define META_IS_SAME(...) __is_same_as(__VA_ARGS__)
#elif META_CXX_TRAIT_VARIABLE_TEMPLATES
#define META_IS_SAME(...) std::is_same_v<__VA_ARGS__>
#else
#define META_IS_SAME(...) std::is_same<__VA_ARGS__>::value
#endif

#if defined(__GNUC__) || defined(_MSC_VER)
#define META_IS_BASE_OF(...) __is_base_of(__VA_ARGS__)
#elif META_CXX_TRAIT_VARIABLE_TEMPLATES
#define META_IS_BASE_OF(...) std::is_base_of_v<__VA_ARGS__>
#else
#define META_IS_BASE_OF(...) std::is_base_of<__VA_ARGS__>::value
#endif

#if defined(__clang__) || defined(_MSC_VER) || \
    (defined(__GNUC__) && __GNUC__ >= 8)
#define META_IS_CONSTRUCTIBLE(...) __is_constructible(__VA_ARGS__)
#elif META_CXX_TRAIT_VARIABLE_TEMPLATES
#define META_IS_CONSTRUCTIBLE(...) std::is_constructible_v<__VA_ARGS__>
#else
#define META_IS_CONSTRUCTIBLE(...) std::is_constructible<__VA_ARGS__>::value
#endif

/// \cond
// Non-portable forward declarations of standard containers
#ifdef _LIBCPP_VERSION
#define META_BEGIN_NAMESPACE_STD _LIBCPP_BEGIN_NAMESPACE_STD
#define META_END_NAMESPACE_STD _LIBCPP_END_NAMESPACE_STD
#elif defined(_MSVC_STL_VERSION)
#define META_BEGIN_NAMESPACE_STD _STD_BEGIN
#define META_END_NAMESPACE_STD _STD_END
#else
#define META_BEGIN_NAMESPACE_STD namespace std {
#define META_END_NAMESPACE_STD }
#endif

#if defined(__GLIBCXX__)
#define META_BEGIN_NAMESPACE_VERSION _GLIBCXX_BEGIN_NAMESPACE_VERSION
#define META_END_NAMESPACE_VERSION _GLIBCXX_END_NAMESPACE_VERSION
#define META_BEGIN_NAMESPACE_CONTAINER _GLIBCXX_BEGIN_NAMESPACE_CONTAINER
#define META_END_NAMESPACE_CONTAINER _GLIBCXX_END_NAMESPACE_CONTAINER
#else
#define META_BEGIN_NAMESPACE_VERSION
#define META_END_NAMESPACE_VERSION
#define META_BEGIN_NAMESPACE_CONTAINER
#define META_END_NAMESPACE_CONTAINER
#endif

#if defined(_LIBCPP_VERSION) && _LIBCPP_VERSION >= 4000
#define META_TEMPLATE_VIS _LIBCPP_TEMPLATE_VIS
#elif defined(_LIBCPP_VERSION)
#define META_TEMPLATE_VIS _LIBCPP_TYPE_VIS_ONLY
#else
#define META_TEMPLATE_VIS
#endif
/// \endcond

namespace meta
{
#if META_CXX_INTEGER_SEQUENCE
    using std::integer_sequence;
#else
    template <typename T, T...>
    struct integer_sequence;
#endif

    template <typename... Ts>
    struct list;

    template <typename T>
    struct id;

    template <template <typename...> class>
    struct quote;

    template <typename T, template <T...> class F>
    struct quote_i;

    template <template <typename...> class C, typename... Ts>
    struct defer;

    template <typename T, template <T...> class C, T... Is>
    struct defer_i;

#if META_CXX_VARIABLE_TEMPLATES || defined(META_DOXYGEN_INVOKED)
    /// is_v
    /// Test whether a type \p T is an instantiation of class
    /// template \p C.
    /// \ingroup trait
    template <typename, template <typename...> class>
    META_INLINE_VAR constexpr bool is_v = false;
    template <typename... Ts, template <typename...> class C>
    META_INLINE_VAR constexpr bool is_v<C<Ts...>, C> = true;
#endif

#ifdef META_CONCEPT
    namespace detail
    {
        template <bool B>
        META_INLINE_VAR constexpr bool barrier = B;

        template <class T, T> struct require_constant; // not defined
    }

    template <typename...>
    META_CONCEPT is_true = META_CONCEPT_BARRIER(true);

    template <typename T, typename U>
    META_CONCEPT same_as =
        META_CONCEPT_BARRIER(META_IS_SAME(T, U));

    template <template <typename...> class C, typename... Ts>
    META_CONCEPT valid = requires
    {
        typename C<Ts...>;
    };

    template <typename T, template <T...> class C, T... Is>
    META_CONCEPT valid_i = requires
    {
        typename C<Is...>;
    };

    template <typename T>
    META_CONCEPT trait = requires
    {
        typename T::type;
    };

    template <typename T>
    META_CONCEPT invocable = requires
    {
        typename quote<T::template invoke>;
    };

    template <typename T>
    META_CONCEPT list_like = is_v<T, list>;

    // clang-format off
    template <typename T>
    META_CONCEPT integral = requires
    {
        typename T::type;
        typename T::value_type;
        typename T::type::value_type;
    }
    && same_as<typename T::value_type, typename T::type::value_type>
#if META_CXX_TRAIT_VARIABLE_TEMPLATES
    && std::is_integral_v<typename T::value_type>
#else
    && std::is_integral<typename T::value_type>::value
#endif

    && requires
    {
        // { T::value } -> same_as<const typename T::value_type&>;
        T::value;
        requires same_as<decltype(T::value), const typename T::value_type>;
        typename detail::require_constant<decltype(T::value), T::value>;

        // { T::type::value } -> same_as<const typename T::value_type&>;
        T::type::value;
        requires same_as<decltype(T::type::value), const typename T::value_type>;
        typename detail::require_constant<decltype(T::type::value), T::type::value>;
        requires T::value == T::type::value;

        // { T{}() } -> same_as<typename T::value_type>;
        T{}();
        requires same_as<decltype(T{}()), typename T::value_type>;
        typename detail::require_constant<decltype(T{}()), T{}()>;
        requires T{}() == T::value;

        // { T{} } -> typename T::value_type;
    };
    // clang-format on
#endif // META_CONCEPT

    namespace extension
    {
        template <META_TYPE_CONSTRAINT(invocable) F, typename L>
        struct apply;
    }
} // namespace meta

#ifdef __clang__
#pragma GCC diagnostic pop
#endif

#endif
