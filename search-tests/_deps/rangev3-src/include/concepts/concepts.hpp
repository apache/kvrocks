/// \file
//  CPP, the Concepts PreProcessor library
//
//  Copyright Eric Niebler 2018-present
//  Copyright (c) 2018-present, Facebook, Inc.
//  Copyright (c) 2020-present, Google LLC.
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef CPP_CONCEPTS_HPP
#define CPP_CONCEPTS_HPP

// clang-format off

#include <initializer_list>
#include <utility>
#include <type_traits>
#include <concepts/swap.hpp>
#include <concepts/type_traits.hpp>

// Disable buggy clang compatibility warning about "requires" and "concept" being
// C++20 keywords.
// https://bugs.llvm.org/show_bug.cgi?id=43708
#if defined(__clang__) && __cplusplus <= 201703L
#define CPP_PP_IGNORE_CXX2A_COMPAT_BEGIN                                                \
    CPP_DIAGNOSTIC_PUSH                                                                 \
    CPP_DIAGNOSTIC_IGNORE_CPP2A_COMPAT

#define CPP_PP_IGNORE_CXX2A_COMPAT_END                                                  \
    CPP_DIAGNOSTIC_POP

#else
#define CPP_PP_IGNORE_CXX2A_COMPAT_BEGIN
#define CPP_PP_IGNORE_CXX2A_COMPAT_END
#endif

#if defined(_MSC_VER) && !defined(__clang__)
#define CPP_WORKAROUND_MSVC_779763 // FATAL_UNREACHABLE calling constexpr function via template parameter
#define CPP_WORKAROUND_MSVC_784772 // Failure to invoke *explicit* bool conversion in a constant expression
#endif

#if !defined(CPP_CXX_CONCEPTS)
#ifdef CPP_DOXYGEN_INVOKED
#define CPP_CXX_CONCEPTS 201800L
#elif defined(__cpp_concepts) && __cpp_concepts > 0
// gcc-6 concepts are too buggy to use
#if !defined(__GNUC__) || defined(__clang__) || __GNUC__ >= 7
#define CPP_CXX_CONCEPTS __cpp_concepts
#else
#define CPP_CXX_CONCEPTS 0L
#endif
#else
#define CPP_CXX_CONCEPTS 0L
#endif
#endif

#define CPP_PP_CAT_(X, ...)  X ## __VA_ARGS__
#define CPP_PP_CAT(X, ...)   CPP_PP_CAT_(X, __VA_ARGS__)

#define CPP_PP_EVAL_(X, ARGS) X ARGS
#define CPP_PP_EVAL(X, ...) CPP_PP_EVAL_(X, (__VA_ARGS__))

#define CPP_PP_EVAL2_(X, ARGS) X ARGS
#define CPP_PP_EVAL2(X, ...) CPP_PP_EVAL2_(X, (__VA_ARGS__))

#define CPP_PP_EXPAND(...) __VA_ARGS__
#define CPP_PP_EAT(...)

#define CPP_PP_FIRST(LIST) CPP_PP_FIRST_ LIST
#define CPP_PP_FIRST_(...) __VA_ARGS__ CPP_PP_EAT

#define CPP_PP_SECOND(LIST) CPP_PP_SECOND_ LIST
#define CPP_PP_SECOND_(...) CPP_PP_EXPAND

#define CPP_PP_CHECK(...) CPP_PP_EXPAND(CPP_PP_CHECK_N(__VA_ARGS__, 0,))
#define CPP_PP_CHECK_N(x, n, ...) n
#define CPP_PP_PROBE(x) x, 1,
#define CPP_PP_PROBE_N(x, n) x, n,

#define CPP_PP_IS_PAREN(x) CPP_PP_CHECK(CPP_PP_IS_PAREN_PROBE x)
#define CPP_PP_IS_PAREN_PROBE(...) CPP_PP_PROBE(~)

// CPP_CXX_VA_OPT
#ifndef CPP_CXX_VA_OPT
#if __cplusplus > 201703L
#define CPP_CXX_VA_OPT_(...) CPP_PP_CHECK(__VA_OPT__(,) 1)
#define CPP_CXX_VA_OPT CPP_CXX_VA_OPT_(~)
#else
#define CPP_CXX_VA_OPT 0
#endif
#endif // CPP_CXX_VA_OPT

// The final CPP_PP_EXPAND here is to avoid
// https://stackoverflow.com/questions/5134523/msvc-doesnt-expand-va-args-correctly
#define CPP_PP_COUNT(...)                                                       \
    CPP_PP_EXPAND(CPP_PP_COUNT_(__VA_ARGS__,                                    \
        50, 49, 48, 47, 46, 45, 44, 43, 42, 41,                                 \
        40, 39, 38, 37, 36, 35, 34, 33, 32, 31,                                 \
        30, 29, 28, 27, 26, 25, 24, 23, 22, 21,                                 \
        20, 19, 18, 17, 16, 15, 14, 13, 12, 11,                                 \
        10, 9, 8, 7, 6, 5, 4, 3, 2, 1,))

#define CPP_PP_COUNT_(                                                          \
    _01, _02, _03, _04, _05, _06, _07, _08, _09, _10,                           \
    _11, _12, _13, _14, _15, _16, _17, _18, _19, _20,                           \
    _21, _22, _23, _24, _25, _26, _27, _28, _29, _30,                           \
    _31, _32, _33, _34, _35, _36, _37, _38, _39, _40,                           \
    _41, _42, _43, _44, _45, _46, _47, _48, _49, _50, N, ...)                   \
    N

#define CPP_PP_IIF(BIT) CPP_PP_CAT_(CPP_PP_IIF_, BIT)
#define CPP_PP_IIF_0(TRUE, ...) __VA_ARGS__
#define CPP_PP_IIF_1(TRUE, ...) TRUE

#define CPP_PP_LPAREN (
#define CPP_PP_RPAREN )

#define CPP_PP_NOT(BIT) CPP_PP_CAT_(CPP_PP_NOT_, BIT)
#define CPP_PP_NOT_0 1
#define CPP_PP_NOT_1 0

#define CPP_PP_EMPTY()
#define CPP_PP_COMMA() ,
#define CPP_PP_LBRACE() {
#define CPP_PP_RBRACE() }
#define CPP_PP_COMMA_IIF(X)                                                     \
    CPP_PP_IIF(X)(CPP_PP_EMPTY, CPP_PP_COMMA)()

#define CPP_PP_FOR_EACH(M, ...)                                                 \
    CPP_PP_FOR_EACH_N(CPP_PP_COUNT(__VA_ARGS__), M, __VA_ARGS__)
#define CPP_PP_FOR_EACH_N(N, M, ...)                                            \
    CPP_PP_CAT(CPP_PP_FOR_EACH_, N)(M, __VA_ARGS__)
#define CPP_PP_FOR_EACH_1(M, _1)                                                \
    M(_1)
#define CPP_PP_FOR_EACH_2(M, _1, _2)                                            \
    M(_1), M(_2)
#define CPP_PP_FOR_EACH_3(M, _1, _2, _3)                                        \
    M(_1), M(_2), M(_3)
#define CPP_PP_FOR_EACH_4(M, _1, _2, _3, _4)                                    \
    M(_1), M(_2), M(_3), M(_4)
#define CPP_PP_FOR_EACH_5(M, _1, _2, _3, _4, _5)                                \
    M(_1), M(_2), M(_3), M(_4), M(_5)
#define CPP_PP_FOR_EACH_6(M, _1, _2, _3, _4, _5, _6)                            \
    M(_1), M(_2), M(_3), M(_4), M(_5), M(_6)
#define CPP_PP_FOR_EACH_7(M, _1, _2, _3, _4, _5, _6, _7)                        \
    M(_1), M(_2), M(_3), M(_4), M(_5), M(_6), M(_7)
#define CPP_PP_FOR_EACH_8(M, _1, _2, _3, _4, _5, _6, _7, _8)                    \
    M(_1), M(_2), M(_3), M(_4), M(_5), M(_6), M(_7), M(_8)

#define CPP_PP_PROBE_EMPTY_PROBE_CPP_PP_PROBE_EMPTY                             \
    CPP_PP_PROBE(~)

#define CPP_PP_PROBE_EMPTY()
#define CPP_PP_IS_NOT_EMPTY(...)                                                \
    CPP_PP_EVAL(                                                                \
        CPP_PP_CHECK,                                                           \
        CPP_PP_CAT(                                                             \
            CPP_PP_PROBE_EMPTY_PROBE_,                                          \
            CPP_PP_PROBE_EMPTY __VA_ARGS__ ()))

#if defined(_MSC_VER) && !defined(__clang__) && (__cplusplus <= 201703L)
#define CPP_BOOL(...) ::meta::bool_<__VA_ARGS__>::value
#define CPP_TRUE_FN                                                             \
    !::concepts::detail::instance_<                                             \
        decltype(CPP_true_fn(::concepts::detail::xNil{}))>

#define CPP_NOT(...) (!CPP_BOOL(__VA_ARGS__))
#else
#define CPP_BOOL(...) __VA_ARGS__
#define CPP_TRUE_FN CPP_true_fn(::concepts::detail::xNil{})
#define CPP_NOT(...) (!(__VA_ARGS__))
#endif

#define CPP_assert(...)                                                         \
    static_assert(static_cast<bool>(__VA_ARGS__),                               \
        "Concept assertion failed : " #__VA_ARGS__)

#define CPP_assert_msg static_assert

#if CPP_CXX_CONCEPTS || defined(CPP_DOXYGEN_INVOKED)
#define CPP_concept META_CONCEPT
#define CPP_and &&

#else
#define CPP_concept CPP_INLINE_VAR constexpr bool
#define CPP_and CPP_and_sfinae

#endif

////////////////////////////////////////////////////////////////////////////////
// CPP_template
// Usage:
//   CPP_template(typename A, typename B)
//     (requires Concept1<A> CPP_and Concept2<B>)
//   void foo(A a, B b)
//   {}
#if CPP_CXX_CONCEPTS
#if defined(CPP_DOXYGEN_INVOKED)
#define CPP_template(...) template<__VA_ARGS__> CPP_TEMPLATE_EXPAND_
#define CPP_TEMPLATE_EXPAND_(X,Y) X Y
#else
#define CPP_template(...) template<__VA_ARGS__ CPP_TEMPLATE_AUX_
#endif
#define CPP_template_def CPP_template
#define CPP_member
#define CPP_ctor(TYPE) TYPE CPP_CTOR_IMPL_1_
#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ < 10
#define CPP_auto_member template<typename...>
#else
#define CPP_auto_member
#endif

/// INTERNAL ONLY
#define CPP_CTOR_IMPL_1_(...) (__VA_ARGS__) CPP_PP_EXPAND

/// INTERNAL ONLY
#define CPP_TEMPLATE_AUX_(...)                                                  \
    > CPP_PP_CAT(                                                               \
        CPP_TEMPLATE_AUX_,                                                      \
        CPP_TEMPLATE_AUX_WHICH_(__VA_ARGS__,))(__VA_ARGS__)

/// INTERNAL ONLY
#define CPP_TEMPLATE_AUX_WHICH_(FIRST, ...)                                     \
    CPP_PP_EVAL(                                                                \
        CPP_PP_CHECK,                                                           \
        CPP_PP_CAT(CPP_TEMPLATE_PROBE_CONCEPT_, FIRST))

/// INTERNAL ONLY
#define CPP_TEMPLATE_PROBE_CONCEPT_concept                                      \
    CPP_PP_PROBE(~)

// A template with a requires clause
/// INTERNAL ONLY
#define CPP_TEMPLATE_AUX_0(...) __VA_ARGS__

// A concept definition
/// INTERNAL ONLY
#define CPP_TEMPLATE_AUX_1(DECL, ...)                                           \
    CPP_concept CPP_CONCEPT_NAME_(DECL) = __VA_ARGS__

#if defined(CPP_DOXYGEN_INVOKED)
#define CPP_concept_ref(NAME, ...)                                              \
    NAME<__VA_ARGS__>
#else
#define CPP_concept_ref(NAME, ...)                                              \
    CPP_PP_CAT(NAME, concept_)<__VA_ARGS__>
#endif

#else // ^^^^ with concepts / without concepts vvvv

#define CPP_template CPP_template_sfinae
#define CPP_template_def CPP_template_def_sfinae
#define CPP_member CPP_member_sfinae
#define CPP_auto_member CPP_member_sfinae
#define CPP_ctor CPP_ctor_sfinae
#define CPP_concept_ref(NAME, ...)                                              \
    (1u == sizeof(CPP_PP_CAT(NAME, concept_)(                                   \
        (::concepts::detail::tag<__VA_ARGS__>*)nullptr)))

/// INTERNAL ONLY
#define CPP_TEMPLATE_AUX_ CPP_TEMPLATE_SFINAE_AUX_

#endif

#define CPP_template_sfinae(...)                                                \
    CPP_PP_IGNORE_CXX2A_COMPAT_BEGIN                                            \
    template<__VA_ARGS__ CPP_TEMPLATE_SFINAE_AUX_

/// INTERNAL ONLY
#define CPP_TEMPLATE_SFINAE_PROBE_CONCEPT_concept                               \
    CPP_PP_PROBE(~)

/// INTERNAL ONLY
#define CPP_TEMPLATE_SFINAE_AUX_WHICH_(FIRST, ...)                              \
    CPP_PP_EVAL(                                                                \
        CPP_PP_CHECK,                                                           \
        CPP_PP_CAT(CPP_TEMPLATE_SFINAE_PROBE_CONCEPT_, FIRST))

/// INTERNAL ONLY
#define CPP_TEMPLATE_SFINAE_AUX_(...)                                           \
    CPP_PP_CAT(                                                                 \
        CPP_TEMPLATE_SFINAE_AUX_,                                               \
        CPP_TEMPLATE_SFINAE_AUX_WHICH_(__VA_ARGS__,))(__VA_ARGS__)

// A template with a requires clause
/// INTERNAL ONLY
#define CPP_TEMPLATE_SFINAE_AUX_0(...) ,                                        \
    bool CPP_true = true,                                                       \
    std::enable_if_t<                                                           \
        CPP_PP_CAT(CPP_TEMPLATE_SFINAE_AUX_3_, __VA_ARGS__) &&                  \
        CPP_BOOL(CPP_true),                                                     \
        int> = 0>                                                               \
    CPP_PP_IGNORE_CXX2A_COMPAT_END

// A concept definition
/// INTERNAL ONLY
#define CPP_TEMPLATE_SFINAE_AUX_1(DECL, ...) ,                                  \
        bool CPP_true = true,                                                   \
        std::enable_if_t<__VA_ARGS__ && CPP_BOOL(CPP_true), int> = 0>           \
    auto CPP_CONCEPT_NAME_(DECL)(                                               \
        ::concepts::detail::tag<CPP_CONCEPT_PARAMS_(DECL)>*)                    \
        -> char(&)[1];                                                          \
    auto CPP_CONCEPT_NAME_(DECL)(...) -> char(&)[2]                             \
    CPP_PP_IGNORE_CXX2A_COMPAT_END

/// INTERNAL ONLY
#define CPP_CONCEPT_NAME_(DECL)                                                 \
    CPP_PP_EVAL(                                                                \
        CPP_PP_CAT,                                                             \
        CPP_PP_EVAL(CPP_PP_FIRST, CPP_EAT_CONCEPT_(DECL)), concept_)

/// INTERNAL ONLY
#define CPP_CONCEPT_PARAMS_(DECL)                                               \
    CPP_PP_EVAL(CPP_PP_SECOND, CPP_EAT_CONCEPT_(DECL))

/// INTERNAL ONLY
#define CPP_EAT_CONCEPT_(DECL)                                                  \
    CPP_PP_CAT(CPP_EAT_CONCEPT_, DECL)

/// INTERNAL ONLY
#define CPP_EAT_CONCEPT_concept

/// INTERNAL ONLY
#define CPP_and_sfinae                                                          \
    && CPP_BOOL(CPP_true), int> = 0, std::enable_if_t<

/// INTERNAL ONLY
#define CPP_template_def_sfinae(...)                                            \
    template<__VA_ARGS__ CPP_TEMPLATE_DEF_SFINAE_AUX_

/// INTERNAL ONLY
#define CPP_TEMPLATE_DEF_SFINAE_AUX_(...) ,                                     \
    bool CPP_true,                                                              \
    std::enable_if_t<                                                           \
        CPP_PP_CAT(CPP_TEMPLATE_SFINAE_AUX_3_, __VA_ARGS__) &&                  \
        CPP_BOOL(CPP_true),                                                     \
        int>>

/// INTERNAL ONLY
#define CPP_and_sfinae_def                                                      \
    && CPP_BOOL(CPP_true), int>, std::enable_if_t<

/// INTERNAL ONLY
#define CPP_TEMPLATE_SFINAE_AUX_3_requires

/// INTERNAL ONLY
#define CPP_member_sfinae                                                       \
    CPP_broken_friend_member

/// INTERNAL ONLY
#define CPP_ctor_sfinae(TYPE)                                                   \
    CPP_PP_IGNORE_CXX2A_COMPAT_BEGIN                                            \
    TYPE CPP_CTOR_SFINAE_IMPL_1_

/// INTERNAL ONLY
#define CPP_CTOR_SFINAE_IMPL_1_(...)                                            \
    (__VA_ARGS__                                                                \
        CPP_PP_COMMA_IIF(                                                       \
            CPP_PP_NOT(CPP_PP_IS_NOT_EMPTY(__VA_ARGS__)))                       \
    CPP_CTOR_SFINAE_REQUIRES

/// INTERNAL ONLY
#define CPP_CTOR_SFINAE_PROBE_NOEXCEPT_noexcept                                 \
    CPP_PP_PROBE(~)

/// INTERNAL ONLY
#define CPP_CTOR_SFINAE_MAKE_PROBE(FIRST,...)                                   \
    CPP_PP_CAT(CPP_CTOR_SFINAE_PROBE_NOEXCEPT_, FIRST)

/// INTERNAL ONLY
#define CPP_CTOR_SFINAE_REQUIRES(...)                                           \
    CPP_PP_CAT(                                                                 \
        CPP_CTOR_SFINAE_REQUIRES_,                                              \
        CPP_PP_EVAL(                                                            \
            CPP_PP_CHECK,                                                       \
            CPP_CTOR_SFINAE_MAKE_PROBE(__VA_ARGS__,)))(__VA_ARGS__)

// No noexcept-clause:
/// INTERNAL ONLY
#define CPP_CTOR_SFINAE_REQUIRES_0(...)                                         \
    std::enable_if_t<                                                           \
        CPP_PP_CAT(CPP_TEMPLATE_SFINAE_AUX_3_, __VA_ARGS__) && CPP_TRUE_FN,     \
        ::concepts::detail::Nil                                                 \
    > = {})                                                                     \
    CPP_PP_IGNORE_CXX2A_COMPAT_END

// Yes noexcept-clause:
/// INTERNAL ONLY
#define CPP_CTOR_SFINAE_REQUIRES_1(...)                                         \
    std::enable_if_t<                                                           \
        CPP_PP_EVAL(CPP_PP_CAT,                                                 \
            CPP_TEMPLATE_SFINAE_AUX_3_,                                         \
            CPP_PP_CAT(CPP_CTOR_SFINAE_EAT_NOEXCEPT_, __VA_ARGS__)) && CPP_TRUE_FN,\
        ::concepts::detail::Nil                                                 \
    > = {})                                                                     \
    CPP_PP_EXPAND(CPP_PP_CAT(CPP_CTOR_SFINAE_SHOW_NOEXCEPT_, __VA_ARGS__)))

/// INTERNAL ONLY
#define CPP_CTOR_SFINAE_EAT_NOEXCEPT_noexcept(...)

/// INTERNAL ONLY
#define CPP_CTOR_SFINAE_SHOW_NOEXCEPT_noexcept(...)                             \
    noexcept(__VA_ARGS__)                                                       \
    CPP_PP_IGNORE_CXX2A_COMPAT_END                                              \
    CPP_PP_EAT CPP_PP_LPAREN

#ifdef CPP_DOXYGEN_INVOKED
/// INTERNAL ONLY
#define CPP_broken_friend_ret(...)                                              \
    __VA_ARGS__ CPP_PP_EXPAND

#else // ^^^ CPP_DOXYGEN_INVOKED / not CPP_DOXYGEN_INVOKED vvv
#define CPP_broken_friend_ret(...)                                              \
    ::concepts::return_t<                                                       \
        __VA_ARGS__,                                                            \
        std::enable_if_t<CPP_BROKEN_FRIEND_RETURN_TYPE_AUX_

/// INTERNAL ONLY
#define CPP_BROKEN_FRIEND_RETURN_TYPE_AUX_(...)                                 \
    CPP_BROKEN_FRIEND_RETURN_TYPE_AUX_3_(CPP_PP_CAT(                            \
        CPP_TEMPLATE_AUX_2_, __VA_ARGS__))

/// INTERNAL ONLY
#define CPP_TEMPLATE_AUX_2_requires

/// INTERNAL ONLY
#define CPP_BROKEN_FRIEND_RETURN_TYPE_AUX_3_(...)                               \
    (__VA_ARGS__ && CPP_TRUE_FN)>>

#ifdef CPP_WORKAROUND_MSVC_779763
/// INTERNAL ONLY
#define CPP_broken_friend_member                                                \
    template<::concepts::detail::CPP_true_t const &CPP_true_fn =                \
        ::concepts::detail::CPP_true_fn_>

#else // ^^^ workaround / no workaround vvv
/// INTERNAL ONLY
#define CPP_broken_friend_member                                                \
    template<bool (&CPP_true_fn)(::concepts::detail::xNil) =                    \
        ::concepts::detail::CPP_true_fn>

#endif // CPP_WORKAROUND_MSVC_779763
#endif

#if CPP_CXX_CONCEPTS
#if defined(CPP_DOXYGEN_INVOKED)
#define CPP_requires(NAME, REQS)                                                \
    concept NAME =                                                              \
        CPP_PP_CAT(CPP_REQUIRES_, REQS)
#define CPP_requires_ref(NAME, ...)                                             \
    NAME<__VA_ARGS__>
#else
#define CPP_requires(NAME, REQS)                                                \
    CPP_concept CPP_PP_CAT(NAME, requires_) =                                   \
        CPP_PP_CAT(CPP_REQUIRES_, REQS)
#define CPP_requires_ref(NAME, ...)                                             \
    CPP_PP_CAT(NAME, requires_)<__VA_ARGS__>
#endif

/// INTERNAL ONLY
#define CPP_REQUIRES_requires(...)                                              \
    requires(__VA_ARGS__) CPP_REQUIRES_AUX_

/// INTERNAL ONLY
#define CPP_REQUIRES_AUX_(...)                                                  \
    { __VA_ARGS__; }

#else
#define CPP_requires(NAME, REQS)                                                \
    auto CPP_PP_CAT(NAME, requires_test_)                                       \
    CPP_REQUIRES_AUX_(NAME, CPP_REQUIRES_ ## REQS)

#define CPP_requires_ref(NAME, ...)                                             \
    (1u == sizeof(CPP_PP_CAT(NAME, requires_)(                                  \
        (::concepts::detail::tag<__VA_ARGS__>*)nullptr)))

/// INTERNAL ONLY
#define CPP_REQUIRES_requires(...)                                              \
    (__VA_ARGS__) -> decltype CPP_REQUIRES_RETURN_

/// INTERNAL ONLY
#define CPP_REQUIRES_RETURN_(...) (__VA_ARGS__, void()) {}

/// INTERNAL ONLY
#define CPP_REQUIRES_AUX_(NAME, ...)                                            \
    __VA_ARGS__                                                                 \
    template<typename... As>                                                    \
    auto CPP_PP_CAT(NAME, requires_)(                                           \
        ::concepts::detail::tag<As...> *,                                       \
        decltype(&CPP_PP_CAT(NAME, requires_test_)<As...>) = nullptr)           \
        -> char(&)[1];                                                          \
    auto CPP_PP_CAT(NAME, requires_)(...) -> char(&)[2]

#endif

#if CPP_CXX_CONCEPTS
#define CPP_ret(...)                                                            \
    __VA_ARGS__ CPP_PP_EXPAND
#else
#define CPP_ret                                                                 \
    CPP_broken_friend_ret
#endif

////////////////////////////////////////////////////////////////////////////////
// CPP_fun
#if CPP_CXX_CONCEPTS

/// INTERNAL ONLY
#define CPP_FUN_IMPL_1_(...)                                                    \
    (__VA_ARGS__)                                                               \
    CPP_PP_EXPAND

#define CPP_fun(X) X CPP_FUN_IMPL_1_
#else
/// INTERNAL ONLY
#define CPP_FUN_IMPL_1_(...)                                                    \
    (__VA_ARGS__                                                                \
        CPP_PP_COMMA_IIF(                                                       \
            CPP_PP_NOT(CPP_PP_IS_NOT_EMPTY(__VA_ARGS__)))                       \
    CPP_FUN_IMPL_REQUIRES

/// INTERNAL ONLY
#define CPP_FUN_IMPL_REQUIRES(...)                                              \
    CPP_PP_EVAL2_(                                                              \
        CPP_FUN_IMPL_SELECT_CONST_,                                             \
        (__VA_ARGS__,)                                                          \
    )(__VA_ARGS__)

/// INTERNAL ONLY
#define CPP_FUN_IMPL_SELECT_CONST_(MAYBE_CONST, ...)                            \
    CPP_PP_CAT(CPP_FUN_IMPL_SELECT_CONST_,                                      \
        CPP_PP_EVAL(CPP_PP_CHECK, CPP_PP_CAT(                                   \
            CPP_PP_PROBE_CONST_PROBE_, MAYBE_CONST)))

/// INTERNAL ONLY
#define CPP_PP_PROBE_CONST_PROBE_const CPP_PP_PROBE(~)

/// INTERNAL ONLY
#define CPP_FUN_IMPL_SELECT_CONST_1(...)                                        \
    CPP_PP_EVAL(                                                                \
        CPP_FUN_IMPL_SELECT_CONST_NOEXCEPT_,                                    \
        CPP_PP_CAT(CPP_FUN_IMPL_EAT_CONST_, __VA_ARGS__),)(                     \
        CPP_PP_CAT(CPP_FUN_IMPL_EAT_CONST_, __VA_ARGS__))

/// INTERNAL ONLY
#define CPP_FUN_IMPL_SELECT_CONST_NOEXCEPT_(MAYBE_NOEXCEPT, ...)                \
    CPP_PP_CAT(CPP_FUN_IMPL_SELECT_CONST_NOEXCEPT_,                             \
        CPP_PP_EVAL2(CPP_PP_CHECK, CPP_PP_CAT(                                  \
            CPP_PP_PROBE_NOEXCEPT_PROBE_, MAYBE_NOEXCEPT)))

/// INTERNAL ONLY
#define CPP_PP_PROBE_NOEXCEPT_PROBE_noexcept CPP_PP_PROBE(~)

/// INTERNAL ONLY
#define CPP_FUN_IMPL_SELECT_CONST_NOEXCEPT_0(...)                               \
    std::enable_if_t<                                                           \
        CPP_PP_EVAL(                                                            \
            CPP_PP_CAT,                                                         \
            CPP_FUN_IMPL_EAT_REQUIRES_,                                         \
            __VA_ARGS__) && CPP_TRUE_FN,                                        \
        ::concepts::detail::Nil                                                 \
    > = {}) const                                                               \
    CPP_PP_IGNORE_CXX2A_COMPAT_END

/// INTERNAL ONLY
#define CPP_FUN_IMPL_SELECT_CONST_NOEXCEPT_1(...)                               \
    std::enable_if_t<                                                           \
        CPP_PP_EVAL(                                                            \
            CPP_PP_CAT,                                                         \
            CPP_FUN_IMPL_EAT_REQUIRES_,                                         \
            CPP_PP_CAT(CPP_FUN_IMPL_EAT_NOEXCEPT_, __VA_ARGS__)) && CPP_TRUE_FN,\
        ::concepts::detail::Nil                                                 \
    > = {}) const                                                               \
    CPP_PP_EXPAND(CPP_PP_CAT(CPP_FUN_IMPL_SHOW_NOEXCEPT_, __VA_ARGS__)))

/// INTERNAL ONLY
#define CPP_FUN_IMPL_EAT_NOEXCEPT_noexcept(...)

/// INTERNAL ONLY
#define CPP_FUN_IMPL_SHOW_NOEXCEPT_noexcept(...)                                \
    noexcept(__VA_ARGS__) CPP_PP_IGNORE_CXX2A_COMPAT_END                        \
    CPP_PP_EAT CPP_PP_LPAREN

/// INTERNAL ONLY
#define CPP_FUN_IMPL_SELECT_CONST_0(...)                                        \
    CPP_PP_EVAL_(                                                               \
        CPP_FUN_IMPL_SELECT_NONCONST_NOEXCEPT_,                                 \
        (__VA_ARGS__,)                                                          \
    )(__VA_ARGS__)

/// INTERNAL ONLY
#define CPP_FUN_IMPL_SELECT_NONCONST_NOEXCEPT_(MAYBE_NOEXCEPT, ...)             \
    CPP_PP_CAT(CPP_FUN_IMPL_SELECT_NONCONST_NOEXCEPT_,                          \
          CPP_PP_EVAL2(CPP_PP_CHECK, CPP_PP_CAT(                                \
            CPP_PP_PROBE_NOEXCEPT_PROBE_, MAYBE_NOEXCEPT)))

/// INTERNAL ONLY
#define CPP_FUN_IMPL_SELECT_NONCONST_NOEXCEPT_0(...)                            \
    std::enable_if_t<                                                           \
        CPP_PP_CAT(CPP_FUN_IMPL_EAT_REQUIRES_, __VA_ARGS__) && CPP_TRUE_FN,     \
        ::concepts::detail::Nil                                                 \
    > = {})                                                                     \
    CPP_PP_IGNORE_CXX2A_COMPAT_END

/// INTERNAL ONLY
#define CPP_FUN_IMPL_SELECT_NONCONST_NOEXCEPT_1(...)                            \
    std::enable_if_t<                                                           \
        CPP_PP_EVAL(                                                            \
            CPP_PP_CAT,                                                         \
            CPP_FUN_IMPL_EAT_REQUIRES_,                                         \
            CPP_PP_CAT(CPP_FUN_IMPL_EAT_NOEXCEPT_, __VA_ARGS__)                 \
        ) && CPP_TRUE_FN,                                                       \
        ::concepts::detail::Nil                                                 \
    > = {})                                                                     \
    CPP_PP_EXPAND(CPP_PP_CAT(CPP_FUN_IMPL_SHOW_NOEXCEPT_, __VA_ARGS__)))

/// INTERNAL ONLY
#define CPP_FUN_IMPL_EAT_CONST_const

/// INTERNAL ONLY
#define CPP_FUN_IMPL_EAT_REQUIRES_requires

////////////////////////////////////////////////////////////////////////////////
// CPP_fun
// Usage:
//   template <typename A, typename B>
//   void CPP_fun(foo)(A a, B b)([const]opt [noexcept(true)]opt
//       requires Concept1<A> && Concept2<B>)
//   {}
//
// Note: This macro cannot be used when the last function argument is a
//       parameter pack.
#define CPP_fun(X) CPP_PP_IGNORE_CXX2A_COMPAT_BEGIN X CPP_FUN_IMPL_1_
#endif

////////////////////////////////////////////////////////////////////////////////
// CPP_auto_fun
// Usage:
//   template <typename A, typename B>
//   auto CPP_auto_fun(foo)(A a, B b)([const]opt [noexcept(cond)]opt)opt
//   (
//       return a + b
//   )
#define CPP_auto_fun(X) X CPP_AUTO_FUN_IMPL_

/// INTERNAL ONLY
#define CPP_AUTO_FUN_IMPL_(...) (__VA_ARGS__) CPP_AUTO_FUN_RETURNS_

/// INTERNAL ONLY
#define CPP_AUTO_FUN_RETURNS_(...)                                              \
    CPP_PP_EVAL2_(                                                              \
        CPP_AUTO_FUN_SELECT_RETURNS_,                                           \
        (__VA_ARGS__,)                                                          \
    )(__VA_ARGS__)

/// INTERNAL ONLY
#define CPP_AUTO_FUN_SELECT_RETURNS_(MAYBE_CONST, ...)                          \
    CPP_PP_CAT(CPP_AUTO_FUN_RETURNS_CONST_,                                     \
        CPP_PP_EVAL(CPP_PP_CHECK, CPP_PP_CAT(                                   \
            CPP_PP_PROBE_CONST_MUTABLE_PROBE_, MAYBE_CONST)))

/// INTERNAL ONLY
#define CPP_PP_PROBE_CONST_MUTABLE_PROBE_const CPP_PP_PROBE_N(~, 1)

/// INTERNAL ONLY
#define CPP_PP_PROBE_CONST_MUTABLE_PROBE_mutable CPP_PP_PROBE_N(~, 2)

/// INTERNAL ONLY
#define CPP_PP_EAT_MUTABLE_mutable

/// INTERNAL ONLY
#define CPP_AUTO_FUN_RETURNS_CONST_2(...)                                       \
    CPP_PP_CAT(CPP_PP_EAT_MUTABLE_, __VA_ARGS__) CPP_AUTO_FUN_RETURNS_CONST_0

/// INTERNAL ONLY
#define CPP_AUTO_FUN_RETURNS_CONST_1(...)                                       \
    __VA_ARGS__ CPP_AUTO_FUN_RETURNS_CONST_0

/// INTERNAL ONLY
#define CPP_AUTO_FUN_RETURNS_CONST_0(...)                                       \
    CPP_PP_EVAL(CPP_AUTO_FUN_DECLTYPE_NOEXCEPT_,                                \
        CPP_PP_CAT(CPP_AUTO_FUN_RETURNS_, __VA_ARGS__))

/// INTERNAL ONLY
#define CPP_AUTO_FUN_RETURNS_return

#ifdef __cpp_guaranteed_copy_elision
/// INTERNAL ONLY
#define CPP_AUTO_FUN_DECLTYPE_NOEXCEPT_(...)                                    \
    noexcept(noexcept(__VA_ARGS__)) -> decltype(__VA_ARGS__)                    \
    { return (__VA_ARGS__); }

#else
/// INTERNAL ONLY
#define CPP_AUTO_FUN_DECLTYPE_NOEXCEPT_(...)                                    \
    noexcept(noexcept(decltype(__VA_ARGS__)(__VA_ARGS__))) ->                   \
    decltype(__VA_ARGS__)                                                       \
    { return (__VA_ARGS__); }

#endif

#if defined(CPP_DOXYGEN_INVOKED)
#define concept(NAME) concept NAME CPP_CONCEPT_EQUALS_
#define CPP_CONCEPT_EQUALS_(...) =
#endif

namespace concepts
{
    template<bool B>
    using bool_ = std::integral_constant<bool, B>;

#if defined(__cpp_fold_expressions) && __cpp_fold_expressions >= 201603
    template<bool...Bs>
    CPP_INLINE_VAR constexpr bool and_v = (Bs &&...);

    template<bool...Bs>
    CPP_INLINE_VAR constexpr bool or_v = (Bs ||...);
#else
    namespace detail
    {
        template<bool...>
        struct bools;
    } // namespace detail

    template<bool...Bs>
    CPP_INLINE_VAR constexpr bool and_v =
        META_IS_SAME(detail::bools<Bs..., true>, detail::bools<true, Bs...>);

    template<bool...Bs>
    CPP_INLINE_VAR constexpr bool or_v =
        !META_IS_SAME(detail::bools<Bs..., false>, detail::bools<false, Bs...>);
#endif

    template<typename>
    struct return_t_
    {
        template<typename T>
        using invoke = T;
    };

    template<typename T, typename EnableIf>
    using return_t = meta::invoke<return_t_<EnableIf>, T>;

    /// \cond
    namespace detail
    {
        struct ignore
        {
            template<class... Args>
            constexpr ignore(Args&&...) noexcept {}
        };

        template<class>
        constexpr bool true_()
        {
            return true;
        }

        template<typename...>
        struct tag;

        template<typename T>
        CPP_INLINE_VAR constexpr T instance_ = T{};

        template<typename>
        constexpr bool requires_()
        {
            return true;
        }

        struct Nil
        {};

#ifdef CPP_WORKAROUND_MSVC_779763
        enum class xNil {};

        struct CPP_true_t
        {
            constexpr bool operator()(Nil) const noexcept
            {
                return true;
            }
            constexpr bool operator()(xNil) const noexcept
            {
                return true;
            }
        };

        CPP_INLINE_VAR constexpr CPP_true_t CPP_true_fn_ {};

        constexpr bool CPP_true_fn(xNil)
        {
            return true;
        }
#else
        using xNil = Nil;
#endif

        constexpr bool CPP_true_fn(Nil)
        {
            return true;
        }
    } // namespace detail
    /// \endcond

#if defined(__clang__) || defined(_MSC_VER)
    template<bool B>
    std::enable_if_t<B> requires_()
    {}
#else
    template<bool B>
    CPP_INLINE_VAR constexpr std::enable_if_t<B, int> requires_ = 0;
#endif

    inline namespace defs
    {
        ////////////////////////////////////////////////////////////////////////
        // Utility concepts
        ////////////////////////////////////////////////////////////////////////

        /// \concept is_true
        /// \brief The \c is_true concept
        template<bool B>
        CPP_concept is_true = B;

        /// \concept type
        /// \brief The \c type concept
        template<typename... Args>
        CPP_concept type = true;

        /// \concept satisfies
        /// \brief The \c satisfies concept
        template<class T, template<typename...> class Trait, typename... Args>
        CPP_concept satisfies =
            static_cast<bool>(Trait<T, Args...>::type::value);

        ////////////////////////////////////////////////////////////////////////
        // Core language concepts
        ////////////////////////////////////////////////////////////////////////

        /// \concept same_as
        /// \brief The \c same_as concept
        template<typename A, typename B>
        CPP_concept same_as =
            META_IS_SAME(A, B) && META_IS_SAME(B, A);

        /// \cond
        /// \concept not_same_as_
        /// \brief The \c not_same_as_ concept
        template<typename A, typename B>
        CPP_concept not_same_as_ =
            (!same_as<remove_cvref_t<A>, remove_cvref_t<B>>);
        /// \endcond

        // Workaround bug in the Standard Library:
        // From cannot be an incomplete class type despite that
        // is_convertible<X, Y> should be equivalent to is_convertible<X&&, Y>
        // in such a case.
        /// \concept implicitly_convertible_to
        /// \brief The \c implicitly_convertible_to concept
        template<typename From, typename To>
        CPP_concept implicitly_convertible_to =
            std::is_convertible<std::add_rvalue_reference_t<From>, To>::value;

        /// \concept explicitly_convertible_to_
        /// \brief The \c explicitly_convertible_to_ concept
        template<typename From, typename To>
        CPP_requires(explicitly_convertible_to_,
            requires(From(*from)()) //
            (
                static_cast<To>(from())
            ));
        /// \concept explicitly_convertible_to
        /// \brief The \c explicitly_convertible_to concept
        template<typename From, typename To>
        CPP_concept explicitly_convertible_to =
            CPP_requires_ref(concepts::explicitly_convertible_to_, From, To);

        /// \concept convertible_to
        /// \brief The \c convertible_to concept
        template<typename From, typename To>
        CPP_concept convertible_to =
            implicitly_convertible_to<From, To> &&
            explicitly_convertible_to<From, To>;

        /// \concept derived_from_
        /// \brief The \c derived_from_ concept
        CPP_template(typename T, typename U)(
        concept (derived_from_)(T, U),
            convertible_to<T const volatile *, U const volatile *>
        );
        /// \concept derived_from
        /// \brief The \c derived_from concept
        template<typename T, typename U>
        CPP_concept derived_from =
            META_IS_BASE_OF(U, T) &&
            CPP_concept_ref(concepts::derived_from_, T, U);

        /// \concept common_reference_with_
        /// \brief The \c common_reference_with_ concept
        CPP_template(typename T, typename U)(
        concept (common_reference_with_)(T, U),
            same_as<common_reference_t<T, U>, common_reference_t<U, T>> CPP_and
            convertible_to<T, common_reference_t<T, U>> CPP_and
            convertible_to<U, common_reference_t<T, U>>
        );
        /// \concept common_reference_with
        /// \brief The \c common_reference_with concept
        template<typename T, typename U>
        CPP_concept common_reference_with =
            CPP_concept_ref(concepts::common_reference_with_, T, U);

        /// \concept common_with_
        /// \brief The \c common_with_ concept
        CPP_template(typename T, typename U)(
        concept (common_with_)(T, U),
            same_as<common_type_t<T, U>, common_type_t<U, T>> CPP_and
            convertible_to<T, common_type_t<T, U>> CPP_and
            convertible_to<U, common_type_t<T, U>> CPP_and
            common_reference_with<
                std::add_lvalue_reference_t<T const>,
                std::add_lvalue_reference_t<U const>> CPP_and
            common_reference_with<
                std::add_lvalue_reference_t<common_type_t<T, U>>,
                common_reference_t<
                    std::add_lvalue_reference_t<T const>,
                    std::add_lvalue_reference_t<U const>>>
        );
        /// \concept common_with
        /// \brief The \c common_with concept
        template<typename T, typename U>
        CPP_concept common_with =
            CPP_concept_ref(concepts::common_with_, T, U);

        /// \concept integral
        /// \brief The \c integral concept
        template<typename T>
        CPP_concept integral =
            std::is_integral<T>::value;

        /// \concept signed_integral
        /// \brief The \c signed_integral concept
        template<typename T>
        CPP_concept signed_integral =
            integral<T> &&
            std::is_signed<T>::value;

        /// \concept unsigned_integral
        /// \brief The \c unsigned_integral concept
        template<typename T>
        CPP_concept unsigned_integral =
            integral<T> &&
            !signed_integral<T>;

        /// \concept assignable_from_
        /// \brief The \c assignable_from_ concept
        template<typename T, typename U>
        CPP_requires(assignable_from_,
            requires(T t, U && u) //
            (
                t = (U &&) u,
                requires_<same_as<T, decltype(t = (U &&) u)>>
            ));
        /// \concept assignable_from
        /// \brief The \c assignable_from concept
        template<typename T, typename U>
        CPP_concept assignable_from =
            std::is_lvalue_reference<T>::value &&
            common_reference_with<detail::as_cref_t<T>, detail::as_cref_t<U>> &&
            CPP_requires_ref(defs::assignable_from_, T, U);

        /// \concept swappable_
        /// \brief The \c swappable_ concept
        template<typename T>
        CPP_requires(swappable_,
            requires(T & t, T & u) //
            (
                concepts::swap(t, u)
            ));
        /// \concept swappable
        /// \brief The \c swappable concept
        template<typename T>
        CPP_concept swappable =
            CPP_requires_ref(defs::swappable_, T);

        /// \concept swappable_with_
        /// \brief The \c swappable_with_ concept
        template<typename T, typename U>
        CPP_requires(swappable_with_,
            requires(T && t, U && u) //
            (
                concepts::swap((T &&) t, (T &&) t),
                concepts::swap((U &&) u, (U &&) u),
                concepts::swap((U &&) u, (T &&) t),
                concepts::swap((T &&) t, (U &&) u)
            ));
        /// \concept swappable_with
        /// \brief The \c swappable_with concept
        template<typename T, typename U>
        CPP_concept swappable_with =
            common_reference_with<detail::as_cref_t<T>, detail::as_cref_t<U>> &&
            CPP_requires_ref(defs::swappable_with_, T, U);

    }  // inline namespace defs

    namespace detail
    {
        /// \concept boolean_testable_impl_
        /// \brief The \c boolean_testable_impl_ concept
        template<typename T>
        CPP_concept boolean_testable_impl_ = convertible_to<T, bool>;

        /// \concept boolean_testable_frag_
        /// \brief The \c boolean_testable_frag_ concept
        template<typename T>
        CPP_requires(boolean_testable_frag_,
            requires(T && t) //
            (
                !(T&&) t,
                concepts::requires_<boolean_testable_impl_<decltype(!(T&&) t)>>
            ));

        /// \concept boolean_testable_
        /// \brief The \c boolean_testable_ concept
        template<typename T>
        CPP_concept boolean_testable_ =
            CPP_requires_ref(boolean_testable_frag_, T) &&
            boolean_testable_impl_<T>;

        CPP_DIAGNOSTIC_PUSH
        CPP_DIAGNOSTIC_IGNORE_FLOAT_EQUAL

        /// \concept weakly_equality_comparable_with_frag_
        /// \brief The \c weakly_equality_comparable_with_frag_ concept
        template<typename T, typename U>
        CPP_requires(weakly_equality_comparable_with_frag_,
            requires(detail::as_cref_t<T> t, detail::as_cref_t<U> u) //
            (
                concepts::requires_<boolean_testable_<decltype(t == u)>>,
                concepts::requires_<boolean_testable_<decltype(t != u)>>,
                concepts::requires_<boolean_testable_<decltype(u == t)>>,
                concepts::requires_<boolean_testable_<decltype(u != t)>>
            ));
        /// \concept weakly_equality_comparable_with_
        /// \brief The \c weakly_equality_comparable_with_ concept
        template<typename T, typename U>
        CPP_concept weakly_equality_comparable_with_ =
            CPP_requires_ref(weakly_equality_comparable_with_frag_, T, U);

        /// \concept partially_ordered_with_frag_
        /// \brief The \c partially_ordered_with_frag_ concept
        template<typename T, typename U>
        CPP_requires(partially_ordered_with_frag_,
            requires(detail::as_cref_t<T>& t, detail::as_cref_t<U>& u) //
            (
                concepts::requires_<boolean_testable_<decltype(t < u)>>,
                concepts::requires_<boolean_testable_<decltype(t > u)>>,
                concepts::requires_<boolean_testable_<decltype(t <= u)>>,
                concepts::requires_<boolean_testable_<decltype(t >= u)>>,
                concepts::requires_<boolean_testable_<decltype(u < t)>>,
                concepts::requires_<boolean_testable_<decltype(u > t)>>,
                concepts::requires_<boolean_testable_<decltype(u <= t)>>,
                concepts::requires_<boolean_testable_<decltype(u >= t)>>
            ));
        /// \concept partially_ordered_with_
        /// \brief The \c partially_ordered_with_ concept
        template<typename T, typename U>
        CPP_concept partially_ordered_with_ =
            CPP_requires_ref(partially_ordered_with_frag_, T, U);

        CPP_DIAGNOSTIC_POP
    } // namespace detail

    inline namespace defs
    {
        ////////////////////////////////////////////////////////////////////////
        // Comparison concepts
        ////////////////////////////////////////////////////////////////////////

        /// \concept equality_comparable
        /// \brief The \c equality_comparable concept
        template<typename T>
        CPP_concept equality_comparable =
            detail::weakly_equality_comparable_with_<T, T>;

        /// \concept equality_comparable_with_
        /// \brief The \c equality_comparable_with_ concept
        CPP_template(typename T, typename U)(
        concept (equality_comparable_with_)(T, U),
            equality_comparable<
                common_reference_t<detail::as_cref_t<T>, detail::as_cref_t<U>>>
        );
        /// \concept equality_comparable_with
        /// \brief The \c equality_comparable_with concept
        template<typename T, typename U>
        CPP_concept equality_comparable_with =
            equality_comparable<T> &&
            equality_comparable<U> &&
            detail::weakly_equality_comparable_with_<T, U> &&
            common_reference_with<detail::as_cref_t<T>, detail::as_cref_t<U>> &&
            CPP_concept_ref(concepts::equality_comparable_with_, T, U);

        /// \concept totally_ordered
        /// \brief The \c totally_ordered concept
        template<typename T>
        CPP_concept totally_ordered =
            equality_comparable<T> &&
            detail::partially_ordered_with_<T, T>;

        /// \concept totally_ordered_with_
        /// \brief The \c totally_ordered_with_ concept
        CPP_template(typename T, typename U)(
        concept (totally_ordered_with_)(T, U),
            totally_ordered<
                common_reference_t<
                    detail::as_cref_t<T>,
                    detail::as_cref_t<U>>> CPP_and
            detail::partially_ordered_with_<T, U>);

        /// \concept totally_ordered_with
        /// \brief The \c totally_ordered_with concept
        template<typename T, typename U>
        CPP_concept totally_ordered_with =
            totally_ordered<T> &&
            totally_ordered<U> &&
            equality_comparable_with<T, U> &&
            CPP_concept_ref(concepts::totally_ordered_with_, T, U);

        ////////////////////////////////////////////////////////////////////////
        // Object concepts
        ////////////////////////////////////////////////////////////////////////

        /// \concept destructible
        /// \brief The \c destructible concept
        template<typename T>
        CPP_concept destructible =
            std::is_nothrow_destructible<T>::value;

        /// \concept constructible_from
        /// \brief The \c constructible_from concept
        template<typename T, typename... Args>
        CPP_concept constructible_from =
            destructible<T> &&
            META_IS_CONSTRUCTIBLE(T, Args...);

        /// \concept default_constructible
        /// \brief The \c default_constructible concept
        template<typename T>
        CPP_concept default_constructible =
            constructible_from<T>;

        /// \concept move_constructible
        /// \brief The \c move_constructible concept
        template<typename T>
        CPP_concept move_constructible =
            constructible_from<T, T> &&
            convertible_to<T, T>;

        /// \concept copy_constructible_
        /// \brief The \c copy_constructible_ concept
        CPP_template(typename T)(
        concept (copy_constructible_)(T),
            constructible_from<T, T &> &&
            constructible_from<T, T const &> &&
            constructible_from<T, T const> &&
            convertible_to<T &, T> &&
            convertible_to<T const &, T> &&
            convertible_to<T const, T>);
        /// \concept copy_constructible
        /// \brief The \c copy_constructible concept
        template<typename T>
        CPP_concept copy_constructible =
            move_constructible<T> &&
            CPP_concept_ref(concepts::copy_constructible_, T);

        /// \concept move_assignable_
        /// \brief The \c move_assignable_ concept
        CPP_template(typename T)(
        concept (move_assignable_)(T),
            assignable_from<T &, T>
        );
        /// \concept movable
        /// \brief The \c movable concept
        template<typename T>
        CPP_concept movable =
            std::is_object<T>::value &&
            move_constructible<T> &&
            CPP_concept_ref(concepts::move_assignable_, T) &&
            swappable<T>;

        /// \concept copy_assignable_
        /// \brief The \c copy_assignable_ concept
        CPP_template(typename T)(
        concept (copy_assignable_)(T),
            assignable_from<T &, T const &>
        );
        /// \concept copyable
        /// \brief The \c copyable concept
        template<typename T>
        CPP_concept copyable =
            copy_constructible<T> &&
            movable<T> &&
            CPP_concept_ref(concepts::copy_assignable_, T);

        /// \concept semiregular
        /// \brief The \c semiregular concept
        template<typename T>
        CPP_concept semiregular =
            copyable<T> &&
            default_constructible<T>;
            // Axiom: copies are independent. See Fundamentals of Generic
            // Programming http://www.stepanovpapers.com/DeSt98.pdf

        /// \concept regular
        /// \brief The \c regular concept
        template<typename T>
        CPP_concept regular =
            semiregular<T> &&
            equality_comparable<T>;

    } // inline namespace defs
} // namespace concepts

#endif // RANGES_V3_UTILITY_CONCEPTS_HPP
