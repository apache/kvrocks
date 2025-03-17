/// \file meta.hpp Tiny meta-programming library.
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

#ifndef META_HPP
#define META_HPP

#include <cstddef>
#include <initializer_list>
#include <meta/meta_fwd.hpp>
#include <type_traits>
#include <utility>

#ifdef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunknown-pragmas"
#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic ignored "-Wdocumentation-deprecated-sync"
#pragma GCC diagnostic ignored "-Wmissing-variable-declarations"
#endif

/// \defgroup meta Meta
///
/// A tiny metaprogramming library

/// \defgroup trait Trait
/// Trait invocation/composition.
/// \ingroup meta

/// \defgroup invocation Invocation
/// Trait invocation
/// \ingroup trait

/// \defgroup composition Composition
/// Trait composition
/// \ingroup trait

/// \defgroup logical Logical
/// Logical operations
/// \ingroup meta

/// \defgroup algorithm Algorithms
/// Algorithms.
/// \ingroup meta

/// \defgroup query Query/Search
/// Query and search algorithms
/// \ingroup algorithm

/// \defgroup transformation Transformation
/// Transformation algorithms
/// \ingroup algorithm

/// \defgroup runtime Runtime
/// Runtime algorithms
/// \ingroup algorithm

/// \defgroup datatype Datatype
/// Datatypes.
/// \ingroup meta

/// \defgroup list list_like
/// \ingroup datatype

/// \defgroup integral Integer sequence
/// Equivalent to C++14's `std::integer_sequence`
/// \ingroup datatype

/// \defgroup extension Extension
/// Extend meta with your own datatypes.
/// \ingroup datatype

/// \defgroup math Math
/// Integral constant arithmetic.
/// \ingroup meta

/// \defgroup lazy_trait lazy
/// \ingroup trait

/// \defgroup lazy_invocation lazy
/// \ingroup invocation

/// \defgroup lazy_composition lazy
/// \ingroup composition

/// \defgroup lazy_logical lazy
/// \ingroup logical

/// \defgroup lazy_query lazy
/// \ingroup query

/// \defgroup lazy_transformation lazy
/// \ingroup transformation

/// \defgroup lazy_list lazy
/// \ingroup list

/// \defgroup lazy_datatype lazy
/// \ingroup datatype

/// \defgroup lazy_math lazy
/// \ingroup math

/// Tiny metaprogramming library
namespace meta
{
    namespace detail
    {
        /// Returns a \p T nullptr
        template <typename T>
        constexpr T *_nullptr_v()
        {
            return nullptr;
        }

#if META_CXX_VARIABLE_TEMPLATES
        template <typename T>
        META_INLINE_VAR constexpr T *nullptr_v = nullptr;
#endif
    } // namespace detail

    /// An empty type.
    /// \ingroup datatype
    struct nil_
    {
    };

    /// Type alias for \p T::type.
    /// \ingroup invocation
    template <META_TYPE_CONSTRAINT(trait) T>
    using _t = typename T::type;

#if META_CXX_VARIABLE_TEMPLATES || defined(META_DOXYGEN_INVOKED)
    /// Variable alias for \c T::type::value
    /// \note Requires C++14 or greater.
    /// \ingroup invocation
    template <META_TYPE_CONSTRAINT(integral) T>
    constexpr typename T::type::value_type _v = T::type::value;
#endif

    /// Lazy versions of meta actions
    namespace lazy
    {
        /// \sa `meta::_t`
        /// \ingroup lazy_invocation
        template <typename T>
        using _t = defer<_t, T>;
    } // namespace lazy

    /// An integral constant wrapper for \c std::size_t.
    /// \ingroup integral
    template <std::size_t N>
    using size_t = std::integral_constant<std::size_t, N>;

    /// An integral constant wrapper for \c bool.
    /// \ingroup integral
    template <bool B>
    using bool_ = std::integral_constant<bool, B>;

    /// An integral constant wrapper for \c int.
    /// \ingroup integral
    template <int I>
    using int_ = std::integral_constant<int, I>;

    /// An integral constant wrapper for \c char.
    /// \ingroup integral
    template <char Ch>
    using char_ = std::integral_constant<char, Ch>;

    ///////////////////////////////////////////////////////////////////////////////////////////
    // Math operations
    /// An integral constant wrapper around the result of incrementing the wrapped integer \c
    /// T::type::value.
    template <META_TYPE_CONSTRAINT(integral) T>
    using inc = std::integral_constant<decltype(T::type::value + 1), T::type::value + 1>;

    /// An integral constant wrapper around the result of decrementing the wrapped integer \c
    /// T::type::value.
    template <META_TYPE_CONSTRAINT(integral) T>
    using dec = std::integral_constant<decltype(T::type::value - 1), T::type::value - 1>;

    /// An integral constant wrapper around the result of adding the two wrapped integers
    /// \c T::type::value and \c U::type::value.
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral) T, META_TYPE_CONSTRAINT(integral) U>
    using plus = std::integral_constant<decltype(T::type::value + U::type::value),
                                        T::type::value + U::type::value>;

    /// An integral constant wrapper around the result of subtracting the two wrapped integers
    /// \c T::type::value and \c U::type::value.
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral) T, META_TYPE_CONSTRAINT(integral) U>
    using minus = std::integral_constant<decltype(T::type::value - U::type::value),
                                            T::type::value - U::type::value>;

    /// An integral constant wrapper around the result of multiplying the two wrapped integers
    /// \c T::type::value and \c U::type::value.
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral) T, META_TYPE_CONSTRAINT(integral) U>
    using multiplies = std::integral_constant<decltype(T::type::value * U::type::value),
                                                T::type::value * U::type::value>;

    /// An integral constant wrapper around the result of dividing the two wrapped integers \c
    /// T::type::value and \c U::type::value.
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral) T, META_TYPE_CONSTRAINT(integral) U>
    using divides = std::integral_constant<decltype(T::type::value / U::type::value),
                                            T::type::value / U::type::value>;

    /// An integral constant wrapper around the result of negating the wrapped integer
    /// \c T::type::value.
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral) T>
    using negate = std::integral_constant<decltype(-T::type::value), -T::type::value>;

    /// An integral constant wrapper around the remainder of dividing the two wrapped integers
    /// \c T::type::value and \c U::type::value.
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral) T, META_TYPE_CONSTRAINT(integral) U>
    using modulus = std::integral_constant<decltype(T::type::value % U::type::value),
                                            T::type::value % U::type::value>;

    /// A Boolean integral constant wrapper around the result of comparing \c T::type::value and
    /// \c U::type::value for equality.
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral) T, META_TYPE_CONSTRAINT(integral) U>
    using equal_to = bool_<T::type::value == U::type::value>;

    /// A Boolean integral constant wrapper around the result of comparing \c T::type::value and
    /// \c U::type::value for inequality.
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral) T, META_TYPE_CONSTRAINT(integral) U>
    using not_equal_to = bool_<T::type::value != U::type::value>;

    /// A Boolean integral constant wrapper around \c true if \c T::type::value is greater than
    /// \c U::type::value; \c false, otherwise.
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral) T, META_TYPE_CONSTRAINT(integral) U>
    using greater = bool_<(T::type::value > U::type::value)>;

    /// A Boolean integral constant wrapper around \c true if \c T::type::value is less than \c
    /// U::type::value; \c false, otherwise.
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral) T, META_TYPE_CONSTRAINT(integral) U>
    using less = bool_<(T::type::value < U::type::value)>;

    /// A Boolean integral constant wrapper around \c true if \c T::type::value is greater than
    /// or equal to \c U::type::value; \c false, otherwise.
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral) T, META_TYPE_CONSTRAINT(integral) U>
    using greater_equal = bool_<(T::type::value >= U::type::value)>;

    /// A Boolean integral constant wrapper around \c true if \c T::type::value is less than or
    /// equal to \c U::type::value; \c false, otherwise.
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral) T, META_TYPE_CONSTRAINT(integral) U>
    using less_equal = bool_<(T::type::value <= U::type::value)>;

    /// An integral constant wrapper around the result of bitwise-and'ing the two wrapped
    /// integers \c T::type::value and \c U::type::value.
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral) T, META_TYPE_CONSTRAINT(integral) U>
    using bit_and = std::integral_constant<decltype(T::type::value & U::type::value),
                                            T::type::value & U::type::value>;

    /// An integral constant wrapper around the result of bitwise-or'ing the two wrapped
    /// integers \c T::type::value and \c U::type::value.
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral) T, META_TYPE_CONSTRAINT(integral) U>
    using bit_or = std::integral_constant<decltype(T::type::value | U::type::value),
                                            T::type::value | U::type::value>;

    /// An integral constant wrapper around the result of bitwise-exclusive-or'ing the two
    /// wrapped integers \c T::type::value and \c U::type::value.
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral) T, META_TYPE_CONSTRAINT(integral) U>
    using bit_xor = std::integral_constant<decltype(T::type::value ^ U::type::value),
                                            T::type::value ^ U::type::value>;

    /// An integral constant wrapper around the result of bitwise-complementing the wrapped
    /// integer \c T::type::value.
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral) T>
    using bit_not = std::integral_constant<decltype(~T::type::value), ~T::type::value>;

    namespace lazy
    {
        /// \sa 'meta::int'
        /// \ingroup lazy_math
        template <typename T>
        using inc = defer<inc, T>;

        /// \sa 'meta::dec'
        /// \ingroup lazy_math
        template <typename T>
        using dec = defer<dec, T>;

        /// \sa 'meta::plus'
        /// \ingroup lazy_math
        template <typename T, typename U>
        using plus = defer<plus, T, U>;

        /// \sa 'meta::minus'
        /// \ingroup lazy_math
        template <typename T, typename U>
        using minus = defer<minus, T, U>;

        /// \sa 'meta::multiplies'
        /// \ingroup lazy_math
        template <typename T, typename U>
        using multiplies = defer<multiplies, T, U>;

        /// \sa 'meta::divides'
        /// \ingroup lazy_math
        template <typename T, typename U>
        using divides = defer<divides, T, U>;

        /// \sa 'meta::negate'
        /// \ingroup lazy_math
        template <typename T>
        using negate = defer<negate, T>;

        /// \sa 'meta::modulus'
        /// \ingroup lazy_math
        template <typename T, typename U>
        using modulus = defer<modulus, T, U>;

        /// \sa 'meta::equal_to'
        /// \ingroup lazy_math
        template <typename T, typename U>
        using equal_to = defer<equal_to, T, U>;

        /// \sa 'meta::not_equal_t'
        /// \ingroup lazy_math
        template <typename T, typename U>
        using not_equal_to = defer<not_equal_to, T, U>;

        /// \sa 'meta::greater'
        /// \ingroup lazy_math
        template <typename T, typename U>
        using greater = defer<greater, T, U>;

        /// \sa 'meta::less'
        /// \ingroup lazy_math
        template <typename T, typename U>
        using less = defer<less, T, U>;

        /// \sa 'meta::greater_equal'
        /// \ingroup lazy_math
        template <typename T, typename U>
        using greater_equal = defer<greater_equal, T, U>;

        /// \sa 'meta::less_equal'
        /// \ingroup lazy_math
        template <typename T, typename U>
        using less_equal = defer<less_equal, T, U>;

        /// \sa 'meta::bit_and'
        /// \ingroup lazy_math
        template <typename T, typename U>
        using bit_and = defer<bit_and, T, U>;

        /// \sa 'meta::bit_or'
        /// \ingroup lazy_math
        template <typename T, typename U>
        using bit_or = defer<bit_or, T, U>;

        /// \sa 'meta::bit_xor'
        /// \ingroup lazy_math
        template <typename T, typename U>
        using bit_xor = defer<bit_xor, T, U>;

        /// \sa 'meta::bit_not'
        /// \ingroup lazy_math
        template <typename T>
        using bit_not = defer<bit_not, T>;
    } // namespace lazy

    /// \cond
    namespace detail
    {
        enum class indices_strategy_
        {
            done,
            repeat,
            recurse
        };

        constexpr indices_strategy_ strategy_(std::size_t cur, std::size_t end)
        {
            return cur >= end ? indices_strategy_::done
                              : cur * 2 <= end ? indices_strategy_::repeat
                                               : indices_strategy_::recurse;
        }

        template <typename T>
        constexpr std::size_t range_distance_(T begin, T end)
        {
            return begin <= end ? static_cast<std::size_t>(end - begin)
                                : throw "The start of the integer_sequence must not be "
                                        "greater than the end";
        }

        template <std::size_t End, typename State, indices_strategy_ Status_>
        struct make_indices_
        {
            using type = State;
        };

        template <typename T, T, typename>
        struct coerce_indices_
        {
        };
    } // namespace detail
    /// \endcond

    ///////////////////////////////////////////////////////////////////////////////////////////
    // integer_sequence
#if !META_CXX_INTEGER_SEQUENCE
    /// A container for a sequence of compile-time integer constants.
    /// \ingroup integral
    template <typename T, T... Is>
    struct integer_sequence
    {
        using value_type = T;
        /// \return `sizeof...(Is)`
        static constexpr std::size_t size() noexcept { return sizeof...(Is); }
    };
#endif

    ///////////////////////////////////////////////////////////////////////////////////////////
    // index_sequence
    /// A container for a sequence of compile-time integer constants of type
    /// \c std::size_t
    /// \ingroup integral
    template <std::size_t... Is>
    using index_sequence = integer_sequence<std::size_t, Is...>;

#if META_HAS_MAKE_INTEGER_SEQ && !defined(META_DOXYGEN_INVOKED)
    // Implement make_integer_sequence and make_index_sequence with the
    // __make_integer_seq builtin on compilers that provide it. (Redirect
    // through decltype to workaround suspected clang bug.)
    /// \cond
    namespace detail
    {
        template <typename T, T N>
        __make_integer_seq<integer_sequence, T, N> make_integer_sequence_();
    }
    /// \endcond

    template <typename T, T N>
    using make_integer_sequence = decltype(detail::make_integer_sequence_<T, N>());

    template <std::size_t N>
    using make_index_sequence = make_integer_sequence<std::size_t, N>;
#else
    /// Generate \c index_sequence containing integer constants [0,1,2,...,N-1].
    /// \par Complexity
    /// `O(log(N))`.
    /// \ingroup integral
    template <std::size_t N>
    using make_index_sequence =
        _t<detail::make_indices_<N, index_sequence<0>, detail::strategy_(1, N)>>;

    /// Generate \c integer_sequence containing integer constants [0,1,2,...,N-1].
    /// \par Complexity
    /// `O(log(N))`.
    /// \ingroup integral
    template <typename T, T N>
    using make_integer_sequence =
        _t<detail::coerce_indices_<T, 0, make_index_sequence<static_cast<std::size_t>(N)>>>;
#endif

    ///////////////////////////////////////////////////////////////////////////////////////////
    // integer_range
    /// Makes the integer sequence `[From, To)`.
    /// \par Complexity
    /// `O(log(To - From))`.
    /// \ingroup integral
    template <typename T, T From, T To>
    using integer_range =
        _t<detail::coerce_indices_<T, From,
                                    make_index_sequence<detail::range_distance_(From, To)>>>;

    /// \cond
    namespace detail
    {
        template <typename, typename>
        struct concat_indices_
        {
        };

        template <std::size_t... Is, std::size_t... Js>
        struct concat_indices_<index_sequence<Is...>, index_sequence<Js...>>
        {
            using type = index_sequence<Is..., (Js + sizeof...(Is))...>;
        };

        template <>
        struct make_indices_<0u, index_sequence<0>, indices_strategy_::done>
        {
            using type = index_sequence<>;
        };

        template <std::size_t End, std::size_t... Values>
        struct make_indices_<End, index_sequence<Values...>, indices_strategy_::repeat>
          : make_indices_<End, index_sequence<Values..., (Values + sizeof...(Values))...>,
                          detail::strategy_(sizeof...(Values) * 2, End)>
        {
        };

        template <std::size_t End, std::size_t... Values>
        struct make_indices_<End, index_sequence<Values...>, indices_strategy_::recurse>
          : concat_indices_<index_sequence<Values...>,
                            make_index_sequence<End - sizeof...(Values)>>
        {
        };

        template <typename T, T Offset, std::size_t... Values>
        struct coerce_indices_<T, Offset, index_sequence<Values...>>
        {
            using type =
                integer_sequence<T, static_cast<T>(static_cast<T>(Values) + Offset)...>;
        };
    } // namespace detail
    /// \endcond

    /// Evaluate the invocable \p Fn with the arguments \p Args.
    /// \ingroup invocation
    template <META_TYPE_CONSTRAINT(invocable) Fn, typename... Args>
    using invoke = typename Fn::template invoke<Args...>;

    /// Lazy versions of meta actions
    namespace lazy
    {
        /// \sa `meta::invoke`
        /// \ingroup lazy_invocation
        template <typename Fn, typename... Args>
        using invoke = defer<invoke, Fn, Args...>;
    } // namespace lazy

    /// A trait that always returns its argument \p T. It is also an invocable
    /// that always returns \p T.
    /// \ingroup trait
    /// \ingroup invocation
    template <typename T>
    struct id
    {
#if defined(META_WORKAROUND_CWG_1558) && !defined(META_DOXYGEN_INVOKED)
        // Redirect through decltype for compilers that have not
        // yet implemented CWG 1558:
        static id impl(void *);

        template <typename... Ts>
        using invoke = _t<decltype(id::impl(static_cast<list<Ts...> *>(nullptr)))>;
#else
        template <typename...>
        using invoke = T;
#endif

        using type = T;
    };

    /// An alias for type \p T. Useful in non-deduced contexts.
    /// \ingroup trait
    template <typename T>
    using id_t = _t<id<T>>;

    namespace lazy
    {
        /// \sa `meta::id`
        /// \ingroup lazy_trait
        /// \ingroup lazy_invocation
        template <typename T>
        using id = defer<id, T>;
    } // namespace lazy

    /// An alias for `void`.
    /// \ingroup trait
#if defined(META_WORKAROUND_CWG_1558) && !defined(META_DOXYGEN_INVOKED)
    // Redirect through decltype for compilers that have not
    // yet implemented CWG 1558:
    template <typename... Ts>
    using void_ = invoke<id<void>, Ts...>;
#else
    template <typename...>
    using void_ = void;
#endif

#if META_CXX_VARIABLE_TEMPLATES
#ifdef META_CONCEPT
    /// `true` if `T::type` exists and names a type; `false` otherwise.
    /// \ingroup trait
    template <typename T>
    META_INLINE_VAR constexpr bool is_trait_v = trait<T>;

    /// `true` if `T::invoke` exists and names a class template; `false` otherwise.
    /// \ingroup trait
    template <typename T>
    META_INLINE_VAR constexpr bool is_callable_v = invocable<T>;
#else   // ^^^ Concepts / No concepts vvv
    /// \cond
    namespace detail
    {
        template <typename, typename = void>
        META_INLINE_VAR constexpr bool is_trait_ = false;

        template <typename T>
        META_INLINE_VAR constexpr bool is_trait_<T, void_<typename T::type>> = true;

        template <typename, typename = void>
        META_INLINE_VAR constexpr bool is_callable_ = false;

        template <typename T>
        META_INLINE_VAR constexpr bool is_callable_<T, void_<quote<T::template invoke>>> = true;
    } // namespace detail
    /// \endcond

    /// `true` if `T::type` exists and names a type; `false` otherwise.
    /// \ingroup trait
    template <typename T>
    META_INLINE_VAR constexpr bool is_trait_v = detail::is_trait_<T>;

    /// `true` if `T::invoke` exists and names a class template; `false` otherwise.
    /// \ingroup trait
    template <typename T>
    META_INLINE_VAR constexpr bool is_callable_v = detail::is_callable_<T>;
#endif  // Concepts vs. variable templates

    /// An alias for `std::true_type` if `T::type` exists and names a type; otherwise, it's an
    /// alias for `std::false_type`.
    /// \ingroup trait
    template <typename T>
    using is_trait = bool_<is_trait_v<T>>;

    /// An alias for `std::true_type` if `T::invoke` exists and names a class template;
    /// otherwise, it's an alias for `std::false_type`.
    /// \ingroup trait
    template <typename T>
    using is_callable = bool_<is_callable_v<T>>;
#else   // ^^^ META_CXX_VARIABLE_TEMPLATES / !META_CXX_VARIABLE_TEMPLATES vvv
    /// \cond
    namespace detail
    {
        template <typename, typename = void>
        struct is_trait_
        {
            using type = std::false_type;
        };

        template <typename T>
        struct is_trait_<T, void_<typename T::type>>
        {
            using type = std::true_type;
        };

        template <typename, typename = void>
        struct is_callable_
        {
            using type = std::false_type;
        };

        template <typename T>
        struct is_callable_<T, void_<quote<T::template invoke>>>
        {
            using type = std::true_type;
        };
    } // namespace detail
    /// \endcond

    template <typename T>
    using is_trait = _t<detail::is_trait_<T>>;

    /// An alias for `std::true_type` if `T::invoke` exists and names a class
    /// template or alias template; otherwise, it's an alias for
    /// `std::false_type`.
    /// \ingroup trait
    template <typename T>
    using is_callable = _t<detail::is_callable_<T>>;
#endif

    /// \cond
    namespace detail
    {
#ifdef META_CONCEPT
        template <template <typename...> class, typename...>
        struct defer_
        {
        };

        template <template <typename...> class C, typename... Ts>
        requires valid<C, Ts...> struct defer_<C, Ts...>
        {
            using type = C<Ts...>;
        };

        template <typename T, template <T...> class, T...>
        struct defer_i_
        {
        };

        template <typename T, template <T...> class C, T... Is>
        requires valid_i<T, C, Is...> struct defer_i_<T, C, Is...>
        {
            using type = C<Is...>;
        };
#elif defined(META_WORKAROUND_MSVC_703656) // ^^^ Concepts / MSVC workaround vvv
        template <typename, template <typename...> class, typename...>
        struct _defer_
        {
        };

        template <template <typename...> class C, typename... Ts>
        struct _defer_<void_<C<Ts...>>, C, Ts...>
        {
            using type = C<Ts...>;
        };

        template <template <typename...> class C, typename... Ts>
        using defer_ = _defer_<void, C, Ts...>;

        template <typename, typename T, template <T...> class, T...>
        struct _defer_i_
        {
        };

        template <typename T, template <T...> class C, T... Is>
        struct _defer_i_<void_<C<Is...>>, T, C, Is...>
        {
            using type = C<Is...>;
        };

        template <typename T, template <T...> class C, T... Is>
        using defer_i_ = _defer_i_<void, T, C, Is...>;
#else                             // ^^^ workaround ^^^ / vvv no workaround vvv
        template <template <typename...> class C, typename... Ts,
                    template <typename...> class D = C>
        id<D<Ts...>> try_defer_(int);
        template <template <typename...> class C, typename... Ts>
        nil_ try_defer_(long);

        template <template <typename...> class C, typename... Ts>
        using defer_ = decltype(detail::try_defer_<C, Ts...>(0));

        template <typename T, template <T...> class C, T... Is, template <T...> class D = C>
        id<D<Is...>> try_defer_i_(int);
        template <typename T, template <T...> class C, T... Is>
        nil_ try_defer_i_(long);

        template <typename T, template <T...> class C, T... Is>
        using defer_i_ = decltype(detail::try_defer_i_<T, C, Is...>(0));
#endif                            // Concepts vs. MSVC vs. Other

        template <typename T>
        using _t_t = _t<_t<T>>;
    } // namespace detail
    /// \endcond

    ///////////////////////////////////////////////////////////////////////////////////////////
    // defer
    /// A wrapper that defers the instantiation of a template \p C with type parameters \p Ts in
    /// a \c lambda or \c let expression.
    ///
    /// In the code below, the lambda would ideally be written as
    /// `lambda<_a,_b,push_back<_a,_b>>`, however this fails since `push_back` expects its first
    /// argument to be a list, not a placeholder. Instead, we express it using \c defer as
    /// follows:
    ///
    /// \code
    /// template <typename L>
    /// using reverse = reverse_fold<L, list<>, lambda<_a, _b, defer<push_back, _a, _b>>>;
    /// \endcode
    ///
    /// \ingroup invocation
    template <template <typename...> class C, typename... Ts>
    struct defer : detail::defer_<C, Ts...>
    {
    };

    ///////////////////////////////////////////////////////////////////////////////////////////
    // defer_i
    /// A wrapper that defers the instantiation of a template \p C with integral constant
    /// parameters \p Is in a \c lambda or \c let expression.
    /// \sa `defer`
    /// \ingroup invocation
    template <typename T, template <T...> class C, T... Is>
    struct defer_i : detail::defer_i_<T, C, Is...>
    {
    };

    ///////////////////////////////////////////////////////////////////////////////////////////
    // defer_trait
    /// A wrapper that defers the instantiation of a trait \p C with type parameters \p Ts in a
    /// \c lambda or \c let expression.
    /// \sa `defer`
    /// \ingroup invocation
    template <template <typename...> class C, typename... Ts>
    using defer_trait = defer<detail::_t_t, detail::defer_<C, Ts...>>;

    ///////////////////////////////////////////////////////////////////////////////////////////
    // defer_trait_i
    /// A wrapper that defers the instantiation of a trait \p C with integral constant
    /// parameters \p Is in a \c lambda or \c let expression.
    /// \sa `defer_i`
    /// \ingroup invocation
    template <typename T, template <T...> class C, T... Is>
    using defer_trait_i = defer<detail::_t_t, detail::defer_i_<T, C, Is...>>;

    /// An alias that computes the size of the type \p T.
    /// \par Complexity
    /// `O(1)`.
    /// \ingroup trait
    template <typename T>
    using sizeof_ = meta::size_t<sizeof(T)>;

    /// An alias that computes the alignment required for any instance of the type \p T.
    /// \par Complexity
    /// `O(1)`.
    /// \ingroup trait
    template <typename T>
    using alignof_ = meta::size_t<alignof(T)>;

    namespace lazy
    {
        /// \sa `meta::sizeof_`
        /// \ingroup lazy_trait
        template <typename T>
        using sizeof_ = defer<sizeof_, T>;

        /// \sa `meta::alignof_`
        /// \ingroup lazy_trait
        template <typename T>
        using alignof_ = defer<alignof_, T>;
    } // namespace lazy

#if META_CXX_VARIABLE_TEMPLATES
    /// is
    /// Test whether a type \p T is an instantiation of class
    /// template \p C.
    /// \ingroup trait
    template <typename T, template <typename...> class C>
    using is = bool_<is_v<T, C>>;
#else
    /// is
    /// \cond
    namespace detail
    {
        template <typename, template <typename...> class>
        struct is_ : std::false_type
        {
        };

        template <typename... Ts, template <typename...> class C>
        struct is_<C<Ts...>, C> : std::true_type
        {
        };
    } // namespace detail
    /// \endcond

    /// Test whether a type \c T is an instantiation of class
    /// template \c C.
    /// \ingroup trait
    template <typename T, template <typename...> class C>
    using is = _t<detail::is_<T, C>>;
#endif

    /// Compose the Invocables \p Fns in the parameter pack \p Ts.
    /// \ingroup composition
    template <META_TYPE_CONSTRAINT(invocable)... Fns>
    struct compose_
    {
    };

    template <META_TYPE_CONSTRAINT(invocable) Fn0>
    struct compose_<Fn0>
    {
        template <typename... Ts>
        using invoke = invoke<Fn0, Ts...>;
    };

    template <META_TYPE_CONSTRAINT(invocable) Fn0, META_TYPE_CONSTRAINT(invocable)... Fns>
    struct compose_<Fn0, Fns...>
    {
        template <typename... Ts>
        using invoke = invoke<Fn0, invoke<compose_<Fns...>, Ts...>>;
    };

    template <typename... Fns>
    using compose = compose_<Fns...>;

    namespace lazy
    {
        /// \sa 'meta::compose'
        /// \ingroup lazy_composition
        template <typename... Fns>
        using compose = defer<compose, Fns...>;
    } // namespace lazy

    /// Turn a template \p C into an invocable.
    /// \ingroup composition
    template <template <typename...> class C>
    struct quote
    {
        // Indirection through defer here needed to avoid Core issue 1430
        // https://wg21.link/cwg1430
        template <typename... Ts>
        using invoke = _t<defer<C, Ts...>>;
    };

    /// Turn a template \p C taking literals of type \p T into a
    /// invocable.
    /// \ingroup composition
    template <typename T, template <T...> class C>
    struct quote_i
    {
        // Indirection through defer_i here needed to avoid Core issue 1430
        // https://wg21.link/cwg1430
        template <META_TYPE_CONSTRAINT(integral)... Ts>
        using invoke = _t<defer_i<T, C, Ts::type::value...>>;
    };

#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ == 4 && __GNUC_MINOR__ <= 8 && \
!defined(META_DOXYGEN_INVOKED)
    template <template <typename...> class C>
    struct quote_trait
    {
        template <typename... Ts>
        using invoke = _t<invoke<quote<C>, Ts...>>;
    };

    template <typename T, template <T...> class C>
    struct quote_trait_i
    {
        template <typename... Ts>
        using invoke = _t<invoke<quote_i<T, C>, Ts...>>;
    };
#else
    // clang-format off
    /// Turn a trait template \p C into an invocable.
    /// \code
    /// static_assert(std::is_same_v<invoke<quote_trait<std::add_const>, int>, int const>, "");
    /// \endcode
    /// \ingroup composition
    template <template <typename...> class C>
    using quote_trait = compose<quote<_t>, quote<C>>;

    /// Turn a trait template \p C taking literals of type \p T into an invocable.
    /// \ingroup composition
    template <typename T, template <T...> class C>
    using quote_trait_i = compose<quote<_t>, quote_i<T, C>>;
    // clang-format on
#endif

    /// An invocable that partially applies the invocable
    /// \p Fn by binding the arguments \p Ts to the \e front of \p Fn.
    /// \ingroup composition
    template <META_TYPE_CONSTRAINT(invocable) Fn, typename... Ts>
    struct bind_front
    {
        template <typename... Us>
        using invoke = invoke<Fn, Ts..., Us...>;
    };

    /// An invocable that partially applies the invocable \p Fn by binding the
    /// arguments \p Us to the \e back of \p Fn.
    /// \ingroup composition
    template <META_TYPE_CONSTRAINT(invocable) Fn, typename... Us>
    struct bind_back
    {
        template <typename... Ts>
        using invoke = invoke<Fn, Ts..., Us...>;
    };

    namespace lazy
    {
        /// \sa 'meta::bind_front'
        /// \ingroup lazy_composition
        template <typename Fn, typename... Ts>
        using bind_front = defer<bind_front, Fn, Ts...>;

        /// \sa 'meta::bind_back'
        /// \ingroup lazy_composition
        template <typename Fn, typename... Ts>
        using bind_back = defer<bind_back, Fn, Ts...>;
    } // namespace lazy

    /// Extend meta with your own datatypes.
    namespace extension
    {
        /// A trait that unpacks the types in the type list \p L into the invocable
        /// \p Fn.
        /// \ingroup extension
        template <META_TYPE_CONSTRAINT(invocable) Fn, typename L>
        struct apply
        {
        };

        template <META_TYPE_CONSTRAINT(invocable) Fn, typename Ret, typename... Args>
        struct apply<Fn, Ret(Args...)> : lazy::invoke<Fn, Ret, Args...>
        {
        };

        template <META_TYPE_CONSTRAINT(invocable) Fn, template <typename...> class T,
                    typename... Ts>
        struct apply<Fn, T<Ts...>> : lazy::invoke<Fn, Ts...>
        {
        };

        template <META_TYPE_CONSTRAINT(invocable) Fn, typename T, T... Is>
        struct apply<Fn, integer_sequence<T, Is...>>
          : lazy::invoke<Fn, std::integral_constant<T, Is>...>
        {
        };
    } // namespace extension

    /// Applies the invocable \p Fn using the types in the type list \p L as
    /// arguments.
    /// \ingroup invocation
    template <META_TYPE_CONSTRAINT(invocable) Fn, typename L>
    using apply = _t<extension::apply<Fn, L>>;

    namespace lazy
    {
        template <typename Fn, typename L>
        using apply = defer<apply, Fn, L>;
    }

    /// An invocable that takes a bunch of arguments, bundles them into a type
    /// list, and then calls the invocable \p Fn with the type list \p Q.
    /// \ingroup composition
    template <META_TYPE_CONSTRAINT(invocable) Fn,
                META_TYPE_CONSTRAINT(invocable) Q = quote<list>>
    using curry = compose<Fn, Q>;

    /// An invocable that takes a type list, unpacks the types, and then
    /// calls the invocable \p Fn with the types.
    /// \ingroup composition
    template <META_TYPE_CONSTRAINT(invocable) Fn>
    using uncurry = bind_front<quote<apply>, Fn>;

    namespace lazy
    {
        /// \sa 'meta::curry'
        /// \ingroup lazy_composition
        template <typename Fn, typename Q = quote<list>>
        using curry = defer<curry, Fn, Q>;

        /// \sa 'meta::uncurry'
        /// \ingroup lazy_composition
        template <typename Fn>
        using uncurry = defer<uncurry, Fn>;
    } // namespace lazy

    /// An invocable that reverses the order of the first two arguments.
    /// \ingroup composition
    template <META_TYPE_CONSTRAINT(invocable) Fn>
    struct flip
    {
    private:
        template <typename... Ts>
        struct impl
        {
        };
        template <typename A, typename B, typename... Ts>
        struct impl<A, B, Ts...> : lazy::invoke<Fn, B, A, Ts...>
        {
        };

    public:
        template <typename... Ts>
        using invoke = _t<impl<Ts...>>;
    };

    namespace lazy
    {
        /// \sa 'meta::flip'
        /// \ingroup lazy_composition
        template <typename Fn>
        using flip = defer<flip, Fn>;
    } // namespace lazy

    /// \cond
    namespace detail
    {
        template <typename...>
        struct on_
        {
        };
        template <typename Fn, typename... Gs>
        struct on_<Fn, Gs...>
        {
            template <typename... Ts>
            using invoke = invoke<Fn, invoke<compose<Gs...>, Ts>...>;
        };
    } // namespace detail
    /// \endcond

    /// Use as `on<Fn, Gs...>`. Creates an invocable that applies invocable \c Fn to the
    /// result of applying invocable `compose<Gs...>` to all the arguments.
    /// \ingroup composition
    template <META_TYPE_CONSTRAINT(invocable)... Fns>
    using on_ = detail::on_<Fns...>;

    template <typename... Fns>
    using on = on_<Fns...>;

    namespace lazy
    {
        /// \sa 'meta::on'
        /// \ingroup lazy_composition
        template <typename Fn, typename G>
        using on = defer<on, Fn, G>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // conditional_t
    /// \cond
    namespace detail
    {
        template <bool>
        struct _cond
        {
            template <typename Then, typename Else>
            using invoke = Then;
        };
        template <>
        struct _cond<false>
        {
            template <typename Then, typename Else>
            using invoke = Else;
        };
    } // namespace detail
    /// \endcond

    /// Select one type or another depending on a compile-time Boolean.
    /// \ingroup logical
    template <bool If, typename Then, typename Else = void>
    using conditional_t = typename detail::_cond<If>::template invoke<Then, Else>;

    ///////////////////////////////////////////////////////////////////////////////////////////
    // if_
    /// \cond
    namespace detail
    {
#ifdef META_CONCEPT
        template <typename...>
        struct _if_
        {
        };

        template <typename If>
            requires integral<If>
        struct _if_<If> : std::enable_if<_v<If>>
        {
        };

        template <typename If, typename Then>
            requires integral<If>
        struct _if_<If, Then> : std::enable_if<_v<If>, Then>
        {
        };

        template <typename If, typename Then, typename Else>
            requires integral<If>
        struct _if_<If, Then, Else> : std::conditional<_v<If>, Then, Else>
        {
        };
#elif defined(__clang__)
        // Clang is faster with this implementation
        template <typename, typename = bool>
        struct _if_
        {
        };

        template <typename If>
        struct _if_<list<If>, decltype(bool(If::type::value))> : std::enable_if<If::type::value>
        {
        };

        template <typename If, typename Then>
        struct _if_<list<If, Then>, decltype(bool(If::type::value))>
          : std::enable_if<If::type::value, Then>
        {
        };

        template <typename If, typename Then, typename Else>
        struct _if_<list<If, Then, Else>, decltype(bool(If::type::value))>
          : std::conditional<If::type::value, Then, Else>
        {
        };
#else
        // GCC seems to prefer this implementation
        template <typename, typename = std::true_type>
        struct _if_
        {
        };

        template <typename If>
        struct _if_<list<If>, bool_<If::type::value>>
        {
            using type = void;
        };

        template <typename If, typename Then>
        struct _if_<list<If, Then>, bool_<If::type::value>>
        {
            using type = Then;
        };

        template <typename If, typename Then, typename Else>
        struct _if_<list<If, Then, Else>, bool_<If::type::value>>
        {
            using type = Then;
        };

        template <typename If, typename Then, typename Else>
        struct _if_<list<If, Then, Else>, bool_<!If::type::value>>
        {
            using type = Else;
        };
#endif
    } // namespace detail
        /// \endcond

    /// Select one type or another depending on a compile-time Boolean.
    /// \ingroup logical
#ifdef META_CONCEPT
    template <typename... Args>
    using if_ = _t<detail::_if_<Args...>>;

    /// Select one type or another depending on a compile-time Boolean.
    /// \ingroup logical
    template <bool If, typename... Args>
    using if_c = _t<detail::_if_<bool_<If>, Args...>>;
#else
    template <typename... Args>
    using if_ = _t<detail::_if_<list<Args...>>>;

    template <bool If, typename... Args>
    using if_c = _t<detail::_if_<list<bool_<If>, Args...>>>;
#endif

    namespace lazy
    {
        /// \sa 'meta::if_'
        /// \ingroup lazy_logical
        template <typename... Args>
        using if_ = defer<if_, Args...>;

        /// \sa 'meta::if_c'
        /// \ingroup lazy_logical
        template <bool If, typename... Args>
        using if_c = if_<bool_<If>, Args...>;
    } // namespace lazy

    /// \cond
    namespace detail
    {
#ifdef META_CONCEPT
        template <typename...>
        struct _and_
        {
        };

        template <>
        struct _and_<> : std::true_type
        {
        };

        template <typename B, typename... Bs>
            requires integral<B> && (bool(B::type::value))
        struct _and_<B, Bs...> : _and_<Bs...>
        {
        };

        template <typename B, typename... Bs>
            requires integral<B> && (!bool(B::type::value))
        struct _and_<B, Bs...> : std::false_type
        {
        };

        template <typename...>
        struct _or_
        {
        };

        template <>
        struct _or_<> : std::false_type
        {
        };

        template <typename B, typename... Bs>
            requires integral<B> && (bool(B::type::value))
        struct _or_<B, Bs...> : std::true_type
        {
        };

        template <typename B, typename... Bs>
            requires integral<B> && (!bool(B::type::value))
        struct _or_<B, Bs...> : _or_<Bs...>
        {
        };
#else
        template <bool>
        struct _and_
        {
            template <typename...>
            using invoke = std::true_type;
        };

        template <>
        struct _and_<false>
        {
            template <typename B, typename... Bs>
            using invoke = invoke<
                if_c<!B::type::value, id<std::false_type>, _and_<0 == sizeof...(Bs)>>,
                Bs...>;
        };

        template <bool>
        struct _or_
        {
            template <typename = void>
            using invoke = std::false_type;
        };

        template <>
        struct _or_<false>
        {
            template <typename B, typename... Bs>
            using invoke = invoke<
                if_c<B::type::value, id<std::true_type>, _or_<0 == sizeof...(Bs)>>,
                Bs...>;
        };
#endif
    } // namespace detail
    /// \endcond

    /// Logically negate the Boolean parameter
    /// \ingroup logical
    template <bool B>
    using not_c = bool_<!B>;

    /// Logically negate the integral constant-wrapped Boolean parameter.
    /// \ingroup logical
    template <META_TYPE_CONSTRAINT(integral) B>
    using not_ = not_c<B::type::value>;

#if META_CXX_FOLD_EXPRESSIONS && !defined(META_WORKAROUND_GCC_UNKNOWN1)
    template <bool... Bs>
    META_INLINE_VAR constexpr bool and_v = (true && ... && Bs);

    /// Logically AND together all the Boolean parameters
    /// \ingroup logical
    template <bool... Bs>
#if defined(META_WORKAROUND_MSVC_756112) || defined(META_WORKAROUND_GCC_86356)
    using and_c = bool_<and_v<Bs...>>;
#else
    using and_c = bool_<(true && ... && Bs)>;
#endif
#else
#if defined(META_WORKAROUND_GCC_66405)
    template <bool... Bs>
    using and_c = meta::bool_<
        META_IS_SAME(integer_sequence<bool, true, Bs...>,
                     integer_sequence<bool, Bs..., true>)>;
#else
    template <bool... Bs>
    struct and_c
      : meta::bool_<
            META_IS_SAME(integer_sequence<bool, Bs...>,
                         integer_sequence<bool, (Bs || true)...>)>
    {};
#endif
#if META_CXX_VARIABLE_TEMPLATES
    template <bool... Bs>
    META_INLINE_VAR constexpr bool and_v =
        META_IS_SAME(integer_sequence<bool, Bs...>,
                     integer_sequence<bool, (Bs || true)...>);
#endif
#endif

    /// Logically AND together all the integral constant-wrapped Boolean
    /// parameters, \e without short-circuiting.
    /// \ingroup logical
    template <META_TYPE_CONSTRAINT(integral)... Bs>
    using strict_and_ = and_c<Bs::type::value...>;

    template <typename... Bs>
    using strict_and = strict_and_<Bs...>;

    /// Logically AND together all the integral constant-wrapped Boolean
    /// parameters, \e with short-circuiting.
    /// \ingroup logical
    template <typename... Bs>
#ifdef META_CONCEPT
    using and_ = _t<detail::_and_<Bs...>>;
#else
    // Make a trip through defer<> to avoid CWG1430
    // https://wg21.link/cwg1430
    using and_ = _t<defer<detail::_and_<0 == sizeof...(Bs)>::template invoke, Bs...>>;
#endif

    /// Logically OR together all the Boolean parameters
    /// \ingroup logical
#if META_CXX_FOLD_EXPRESSIONS && !defined(META_WORKAROUND_GCC_UNKNOWN1)
    template <bool... Bs>
    META_INLINE_VAR constexpr bool or_v = (false || ... || Bs);

    template <bool... Bs>
#if defined(META_WORKAROUND_MSVC_756112) || defined(META_WORKAROUND_GCC_86356)
    using or_c = bool_<or_v<Bs...>>;
#else
    using or_c = bool_<(false || ... || Bs)>;
#endif
#else
    template <bool... Bs>
    struct or_c
      : meta::bool_<
            !META_IS_SAME(integer_sequence<bool, Bs...>,
                          integer_sequence<bool, (Bs && false)...>)>
    {};
#if META_CXX_VARIABLE_TEMPLATES
    template <bool... Bs>
    META_INLINE_VAR constexpr bool or_v =
        !META_IS_SAME(integer_sequence<bool, Bs...>,
                      integer_sequence<bool, (Bs && false)...>);
#endif
#endif

    /// Logically OR together all the integral constant-wrapped Boolean
    /// parameters, \e without short-circuiting.
    /// \ingroup logical
    template <META_TYPE_CONSTRAINT(integral)... Bs>
    using strict_or_ = or_c<Bs::type::value...>;

    template <typename... Bs>
    using strict_or = strict_or_<Bs...>;

    /// Logically OR together all the integral constant-wrapped Boolean
    /// parameters, \e with short-circuiting.
    /// \ingroup logical
    template <typename... Bs>
#ifdef META_CONCEPT
    using or_ = _t<detail::_or_<Bs...>>;
#else
    // Make a trip through defer<> to avoid CWG1430
    // https://wg21.link/cwg1430
    using or_ = _t<defer<detail::_or_<0 == sizeof...(Bs)>::template invoke, Bs...>>;
#endif

    namespace lazy
    {
        /// \sa 'meta::and_'
        /// \ingroup lazy_logical
        template <typename... Bs>
        using and_ = defer<and_, Bs...>;

        /// \sa 'meta::or_'
        /// \ingroup lazy_logical
        template <typename... Bs>
        using or_ = defer<or_, Bs...>;

        /// \sa 'meta::not_'
        /// \ingroup lazy_logical
        template <typename B>
        using not_ = defer<not_, B>;

        /// \sa 'meta::strict_and'
        /// \ingroup lazy_logical
        template <typename... Bs>
        using strict_and = defer<strict_and, Bs...>;

        /// \sa 'meta::strict_or'
        /// \ingroup lazy_logical
        template <typename... Bs>
        using strict_or = defer<strict_or, Bs...>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // fold
    /// \cond
    namespace detail
    {
        template <typename, typename, typename>
        struct fold_
        {
        };

        template <typename Fn, typename T0, typename T1, typename T2, typename T3, typename T4,
                    typename T5, typename T6, typename T7, typename T8, typename T9>
        struct compose10_
        {
            template <typename X, typename Y>
            using F = invoke<Fn, X, Y>;

            template <typename S>
            using invoke =
                F<F<F<F<F<F<F<F<F<F<_t<S>, T0>, T1>, T2>, T3>, T4>, T5>, T6>, T7>, T8>, T9>;
        };

#ifdef META_CONCEPT
        template <typename Fn>
        struct compose_
        {
            template <typename X, typename Y>
            using F = invoke<Fn, X, Y>;

            template <typename T0, typename T1, typename T2, typename T3, typename T4,
                        typename T5, typename T6, typename T7, typename T8, typename T9,
                        typename State>
            using invoke =
                F<F<F<F<F<F<F<F<F<F<State, T0>, T1>, T2>, T3>, T4>, T5>, T6>, T7>, T8>, T9>;
        };

        template <typename State, typename Fn>
        struct fold_<list<>, State, Fn>
        {
            using type = State;
        };

        template <typename Head, typename... Tail, typename State, typename Fn>
        requires valid<invoke, Fn, State, Head>
        struct fold_<list<Head, Tail...>, State, Fn>
          : fold_<list<Tail...>, invoke<Fn, State, Head>, Fn>
        {
        };

        template <typename T0, typename T1, typename T2, typename T3, typename T4, typename T5,
                    typename T6, typename T7, typename T8, typename T9, typename... Tail,
                    typename State, typename Fn>
        requires valid<invoke, compose_<Fn>, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, State>
        struct fold_<list<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, Tail...>, State, Fn>
          : fold_<list<Tail...>,
                  invoke<compose_<Fn>, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, State>, Fn>
        {
        };
#else   // ^^^ Concepts / no Concepts vvv
        template <typename Fn, typename T0>
        struct compose1_
        {
            template <typename X>
            using invoke = invoke<Fn, _t<X>, T0>;
        };

        template <typename State, typename Fn>
        struct fold_<list<>, State, Fn> : State
        {
        };

        template <typename Head, typename... Tail, typename State, typename Fn>
        struct fold_<list<Head, Tail...>, State, Fn>
          : fold_<list<Tail...>, lazy::invoke<compose1_<Fn, Head>, State>, Fn>
        {
        };

        template <typename T0, typename T1, typename T2, typename T3, typename T4, typename T5,
                    typename T6, typename T7, typename T8, typename T9, typename... Tail,
                    typename State, typename Fn>
        struct fold_<list<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, Tail...>, State, Fn>
          : fold_<list<Tail...>,
                  lazy::invoke<compose10_<Fn, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>, State>, Fn>
        {
        };
#endif  // META_CONCEPT
    } // namespace detail
    /// \endcond

    /// Return a new \c meta::list constructed by doing a left fold of the list \p L using
    /// binary invocable \p Fn and initial state \p State. That is, the \c State(N) for
    /// the list element \c A(N) is computed by `Fn(State(N-1), A(N)) -> State(N)`.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) L, typename State, META_TYPE_CONSTRAINT(invocable) Fn>
#ifdef META_CONCEPT
    using fold = _t<detail::fold_<L, State, Fn>>;
#else
    using fold = _t<detail::fold_<L, id<State>, Fn>>;
#endif

    /// An alias for `meta::fold`.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) L, typename State, META_TYPE_CONSTRAINT(invocable) Fn>
    using accumulate = fold<L, State, Fn>;

    namespace lazy
    {
        /// \sa 'meta::foldl'
        /// \ingroup lazy_transformation
        template <typename L, typename State, typename Fn>
        using fold = defer<fold, L, State, Fn>;

        /// \sa 'meta::accumulate'
        /// \ingroup lazy_transformation
        template <typename L, typename State, typename Fn>
        using accumulate = defer<accumulate, L, State, Fn>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // reverse_fold
    /// \cond
    namespace detail
    {
        template <typename, typename, typename>
        struct reverse_fold_
        {
        };

        template <typename State, typename Fn>
        struct reverse_fold_<list<>, State, Fn>
        {
            using type = State;
        };

#ifdef META_CONCEPT
        template <typename Head, typename... L, typename State, typename Fn>
        requires trait<reverse_fold_<list<L...>, State, Fn>> struct reverse_fold_<
            list<Head, L...>, State, Fn>
          : lazy::invoke<Fn, _t<reverse_fold_<list<L...>, State, Fn>>, Head>
        {
        };
#else
        template <typename Head, typename... Tail, typename State, typename Fn>
        struct reverse_fold_<list<Head, Tail...>, State, Fn>
          : lazy::invoke<compose1_<Fn, Head>, reverse_fold_<list<Tail...>, State, Fn>>
        {
        };
#endif

        template <typename T0, typename T1, typename T2, typename T3, typename T4, typename T5,
                    typename T6, typename T7, typename T8, typename T9, typename... Tail,
                    typename State, typename Fn>
        struct reverse_fold_<list<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, Tail...>, State, Fn>
          : lazy::invoke<compose10_<Fn, T9, T8, T7, T6, T5, T4, T3, T2, T1, T0>,
                            reverse_fold_<list<Tail...>, State, Fn>>
        {
        };
    } // namespace detail
    /// \endcond

    /// Return a new \c meta::list constructed by doing a right fold of the list \p L using
    /// binary invocable \p Fn and initial state \p State. That is, the \c State(N) for the list
    /// element \c A(N) is computed by `Fn(A(N), State(N+1)) -> State(N)`.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) L, typename State, META_TYPE_CONSTRAINT(invocable) Fn>
    using reverse_fold = _t<detail::reverse_fold_<L, State, Fn>>;

    namespace lazy
    {
        /// \sa 'meta::foldr'
        /// \ingroup lazy_transformation
        template <typename L, typename State, typename Fn>
        using reverse_fold = defer<reverse_fold, L, State, Fn>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // npos
    /// A special value used to indicate no matches. It equals the maximum
    /// value representable by std::size_t.
    /// \ingroup list
    using npos = meta::size_t<std::size_t(-1)>;

    ///////////////////////////////////////////////////////////////////////////////////////////
    // list
    /// A list of types.
    /// \ingroup list
    template <typename... Ts>
    struct list
    {
        using type = list;
        /// \return `sizeof...(Ts)`
        static constexpr std::size_t size() noexcept { return sizeof...(Ts); }
    };

    ///////////////////////////////////////////////////////////////////////////////////////////
    // size
    /// An integral constant wrapper that is the size of the \c meta::list
    /// \p L.
    /// \ingroup list
    template <META_TYPE_CONSTRAINT(list_like) L>
    using size = meta::size_t<L::size()>;

    namespace lazy
    {
        /// \sa 'meta::size'
        /// \ingroup lazy_list
        template <typename L>
        using size = defer<size, L>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // concat
    /// \cond
    namespace detail
    {
        template <typename... Lists>
        struct concat_
        {
        };

        template <>
        struct concat_<>
        {
            using type = list<>;
        };

        template <typename... L1>
        struct concat_<list<L1...>>
        {
            using type = list<L1...>;
        };

        template <typename... L1, typename... L2>
        struct concat_<list<L1...>, list<L2...>>
        {
            using type = list<L1..., L2...>;
        };

        template <typename... L1, typename... L2, typename... L3>
        struct concat_<list<L1...>, list<L2...>, list<L3...>>
        {
            using type = list<L1..., L2..., L3...>;
        };

        template <typename... L1, typename... L2, typename... L3, typename... Rest>
        struct concat_<list<L1...>, list<L2...>, list<L3...>, Rest...>
          : concat_<list<L1..., L2..., L3...>, Rest...>
        {
        };

        template <typename... L1, typename... L2, typename... L3, typename... L4,
                    typename... L5, typename... L6, typename... L7, typename... L8,
                    typename... L9, typename... L10, typename... Rest>
        struct concat_<list<L1...>, list<L2...>, list<L3...>, list<L4...>, list<L5...>,
                        list<L6...>, list<L7...>, list<L8...>, list<L9...>, list<L10...>,
                        Rest...>
          : concat_<list<L1..., L2..., L3..., L4..., L5..., L6..., L7..., L8..., L9..., L10...>,
                    Rest...>
        {
        };
    } // namespace detail
    /// \endcond

    /// Concatenates several lists into a single list.
    /// \pre The parameters must all be instantiations of \c meta::list.
    /// \par Complexity
    /// `O(L)` where `L` is the number of lists in the list of lists.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like)... Ls>
    using concat_ = _t<detail::concat_<Ls...>>;

    template <typename... Lists>
    using concat = concat_<Lists...>;

    namespace lazy
    {
        /// \sa 'meta::concat'
        /// \ingroup lazy_transformation
        template <typename... Lists>
        using concat = defer<concat, Lists...>;
    } // namespace lazy

    /// Joins a list of lists into a single list.
    /// \pre The parameter must be an instantiation of \c meta::list\<T...\>
    /// where each \c T is itself an instantiation of \c meta::list.
    /// \par Complexity
    /// `O(L)` where `L` is the number of lists in the list of
    /// lists.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) ListOfLists>
    using join = apply<quote<concat>, ListOfLists>;

    namespace lazy
    {
        /// \sa 'meta::join'
        /// \ingroup lazy_transformation
        template <typename ListOfLists>
        using join = defer<join, ListOfLists>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // transform
    /// \cond
    namespace detail
    {
#ifdef META_CONCEPT
        template <typename... Args>
        struct transform_
        {
        };

        template <typename... Ts, typename Fn>
            requires invocable<Fn> && and_v<valid<invoke, Fn, Ts>...>
        struct transform_<list<Ts...>, Fn>
        {
            using type = list<invoke<Fn, Ts>...>;
        };

        template <typename... Ts, typename... Us, typename Fn>
            requires invocable<Fn> && and_v<valid<invoke, Fn, Ts, Us>...>
        struct transform_<list<Ts...>, list<Us...>, Fn>
        {
            using type = list<invoke<Fn, Ts, Us>...>;
        };
#else
        template <typename, typename = void>
        struct transform_
        {
        };

        template <typename... Ts, typename Fn>
        struct transform_<list<list<Ts...>, Fn>, void_<invoke<Fn, Ts>...>>
        {
            using type = list<invoke<Fn, Ts>...>;
        };

        template <typename... Ts0, typename... Ts1, typename Fn>
        struct transform_<list<list<Ts0...>, list<Ts1...>, Fn>,
                            void_<invoke<Fn, Ts0, Ts1>...>>
        {
            using type = list<invoke<Fn, Ts0, Ts1>...>;
        };
#endif
    } // namespace detail
        /// \endcond

    /// Return a new \c meta::list constructed by transforming all the
    /// elements in \p L with the unary invocable \p Fn. \c transform can
    /// also be called with two lists of the same length and a binary
    /// invocable, in which case it returns a new list constructed with the
    /// results of calling \c Fn with each element in the lists, pairwise.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup transformation
#ifdef META_CONCEPT
    template <typename... Args>
    using transform = _t<detail::transform_<Args...>>;
#else
    template <typename... Args>
    using transform = _t<detail::transform_<list<Args...>>>;
#endif

    namespace lazy
    {
        /// \sa 'meta::transform'
        /// \ingroup lazy_transformation
        template <typename... Args>
        using transform = defer<transform, Args...>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // repeat_n
    /// \cond
    namespace detail
    {
        template <typename T, std::size_t>
        using first_ = T;

        template <typename T, typename Ints>
        struct repeat_n_c_
        {
        };

        template <typename T, std::size_t... Is>
        struct repeat_n_c_<T, index_sequence<Is...>>
        {
            using type = list<first_<T, Is>...>;
        };
    } // namespace detail
    /// \endcond

    /// Generate `list<T,T,T...T>` of size \p N arguments.
    /// \par Complexity
    /// `O(log N)`.
    /// \ingroup list
    template <std::size_t N, typename T = void>
    using repeat_n_c = _t<detail::repeat_n_c_<T, make_index_sequence<N>>>;

    /// Generate `list<T,T,T...T>` of size \p N arguments.
    /// \par Complexity
    /// `O(log N)`.
    /// \ingroup list
    template <META_TYPE_CONSTRAINT(integral) N, typename T = void>
    using repeat_n = repeat_n_c<N::type::value, T>;

    namespace lazy
    {
        /// \sa 'meta::repeat_n'
        /// \ingroup lazy_list
        template <typename N, typename T = void>
        using repeat_n = defer<repeat_n, N, T>;

        /// \sa 'meta::repeat_n_c'
        /// \ingroup lazy_list
        template <std::size_t N, typename T = void>
        using repeat_n_c = defer<repeat_n, meta::size_t<N>, T>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // at
    /// \cond
    namespace detail
    {
#if META_HAS_TYPE_PACK_ELEMENT && !defined(META_DOXYGEN_INVOKED)
        template <typename L, std::size_t N, typename = void>
        struct at_
        {
        };

        template <typename... Ts, std::size_t N>
        struct at_<list<Ts...>, N, void_<__type_pack_element<N, Ts...>>>
        {
            using type = __type_pack_element<N, Ts...>;
        };
#else
        template <typename VoidPtrs>
        struct at_impl_;

        template <typename... VoidPtrs>
        struct at_impl_<list<VoidPtrs...>>
        {
            static nil_ eval(...);

            template <typename T, typename... Us>
            static T eval(VoidPtrs..., T *, Us *...);
        };

        template <typename L, std::size_t N>
        struct at_
        {
        };

        template <typename... Ts, std::size_t N>
        struct at_<list<Ts...>, N>
          : decltype(at_impl_<repeat_n_c<N, void *>>::eval(static_cast<id<Ts> *>(nullptr)...))
        {
        };
#endif    // META_HAS_TYPE_PACK_ELEMENT
    } // namespace detail
    /// \endcond

    /// Return the \p N th element in the \c meta::list \p L.
    /// \par Complexity
    /// Amortized `O(1)`.
    /// \ingroup list
    template <META_TYPE_CONSTRAINT(list_like) L, std::size_t N>
    using at_c = _t<detail::at_<L, N>>;

    /// Return the \p N th element in the \c meta::list \p L.
    /// \par Complexity
    /// Amortized `O(1)`.
    /// \ingroup list
    template <META_TYPE_CONSTRAINT(list_like) L, META_TYPE_CONSTRAINT(integral) N>
    using at = at_c<L, N::type::value>;

    namespace lazy
    {
        /// \sa 'meta::at'
        /// \ingroup lazy_list
        template <typename L, typename N>
        using at = defer<at, L, N>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // drop
    /// \cond
    namespace detail
    {
        ///////////////////////////////////////////////////////////////////////////////////////
        /// drop_impl_
        template <typename VoidPtrs>
        struct drop_impl_
        {
            static nil_ eval(...);
        };

        template <typename... VoidPtrs>
        struct drop_impl_<list<VoidPtrs...>>
        {
            static nil_ eval(...);

            template <typename... Ts>
            static id<list<Ts...>> eval(VoidPtrs..., id<Ts> *...);
        };

        template <>
        struct drop_impl_<list<>>
        {
            template <typename... Ts>
            static id<list<Ts...>> eval(id<Ts> *...);
        };

        template <typename L, std::size_t N>
        struct drop_
        {
        };

        template <typename... Ts, std::size_t N>
        struct drop_<list<Ts...>, N>
#if META_CXX_VARIABLE_TEMPLATES
          : decltype(drop_impl_<repeat_n_c<N, void *>>::eval(detail::nullptr_v<id<Ts>>...))
#else
          : decltype(drop_impl_<repeat_n_c<N, void *>>::eval(detail::_nullptr_v<id<Ts>>()...))
#endif
        {
        };
    } // namespace detail
    /// \endcond

    /// Return a new \c meta::list by removing the first \p N elements from \p L.
    /// \par Complexity
    /// `O(1)`.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) L, std::size_t N>
    using drop_c = _t<detail::drop_<L, N>>;

    /// Return a new \c meta::list by removing the first \p N elements from \p L.
    /// \par Complexity
    /// `O(1)`.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) L, META_TYPE_CONSTRAINT(integral) N>
    using drop = drop_c<L, N::type::value>;

    namespace lazy
    {
        /// \sa 'meta::drop'
        /// \ingroup lazy_transformation
        template <typename L, typename N>
        using drop = defer<drop, L, N>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // front
    /// \cond
    namespace detail
    {
        template <typename L>
        struct front_
        {
        };

        template <typename Head, typename... Tail>
        struct front_<list<Head, Tail...>>
        {
            using type = Head;
        };
    } // namespace detail
    /// \endcond

    /// Return the first element in \c meta::list \p L.
    /// \par Complexity
    /// `O(1)`.
    /// \ingroup list
    template <META_TYPE_CONSTRAINT(list_like) L>
    using front = _t<detail::front_<L>>;

    namespace lazy
    {
        /// \sa 'meta::front'
        /// \ingroup lazy_list
        template <typename L>
        using front = defer<front, L>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // back
    /// \cond
    namespace detail
    {
        template <typename L>
        struct back_
        {
        };

        template <typename Head, typename... Tail>
        struct back_<list<Head, Tail...>>
        {
            using type = at_c<list<Head, Tail...>, sizeof...(Tail)>;
        };
    } // namespace detail
    /// \endcond

    /// Return the last element in \c meta::list \p L.
    /// \par Complexity
    /// Amortized `O(1)`.
    /// \ingroup list
    template <META_TYPE_CONSTRAINT(list_like) L>
    using back = _t<detail::back_<L>>;

    namespace lazy
    {
        /// \sa 'meta::back'
        /// \ingroup lazy_list
        template <typename L>
        using back = defer<back, L>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // push_front
    /// Return a new \c meta::list by adding the element \c T to the front of \p L.
    /// \par Complexity
    /// `O(1)`.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) L, typename... Ts>
    using push_front = apply<bind_front<quote<list>, Ts...>, L>;

    namespace lazy
    {
        /// \sa 'meta::push_front'
        /// \ingroup lazy_transformation
        template <typename... Ts>
        using push_front = defer<push_front, Ts...>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // pop_front
    /// \cond
    namespace detail
    {
        template <typename L>
        struct pop_front_
        {
        };

        template <typename Head, typename... L>
        struct pop_front_<list<Head, L...>>
        {
            using type = list<L...>;
        };
    } // namespace detail
    /// \endcond

    /// Return a new \c meta::list by removing the first element from the
    /// front of \p L.
    /// \par Complexity
    /// `O(1)`.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) L>
    using pop_front = _t<detail::pop_front_<L>>;

    namespace lazy
    {
        /// \sa 'meta::pop_front'
        /// \ingroup lazy_transformation
        template <typename L>
        using pop_front = defer<pop_front, L>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // push_back
    /// Return a new \c meta::list by adding the element \c T to the back of \p L.
    /// \par Complexity
    /// `O(1)`.
    /// \note \c pop_back not provided because it cannot be made to meet the
    /// complexity guarantees one would expect.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) L, typename... Ts>
    using push_back = apply<bind_back<quote<list>, Ts...>, L>;

    namespace lazy
    {
        /// \sa 'meta::push_back'
        /// \ingroup lazy_transformation
        template <typename... Ts>
        using push_back = defer<push_back, Ts...>;
    } // namespace lazy

    /// \cond
    namespace detail
    {
        template <typename T, typename U>
        using min_ = if_<less<U, T>, U, T>;

        template <typename T, typename U>
        using max_ = if_<less<U, T>, T, U>;
    } // namespace detail
    /// \endcond

    /// An integral constant wrapper around the minimum of `Ts::type::value...`
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral)... Ts>
    using min_ = fold<pop_front<list<Ts...>>, front<list<Ts...>>, quote<detail::min_>>;

    template <typename... Ts>
    using min = min_<Ts...>;

    /// An integral constant wrapper around the maximum of `Ts::type::value...`
    /// \ingroup math
    template <META_TYPE_CONSTRAINT(integral)... Ts>
    using max_ = fold<pop_front<list<Ts...>>, front<list<Ts...>>, quote<detail::max_>>;

    template <typename... Ts>
    using max = max_<Ts...>;

    namespace lazy
    {
        /// \sa 'meta::min'
        /// \ingroup lazy_math
        template <typename... Ts>
        using min = defer<min, Ts...>;

        /// \sa 'meta::max'
        /// \ingroup lazy_math
        template <typename... Ts>
        using max = defer<max, Ts...>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // empty
    /// An Boolean integral constant wrapper around \c true if \p L is an
    /// empty type list; \c false, otherwise.
    /// \par Complexity
    /// `O(1)`.
    /// \ingroup list
    template <META_TYPE_CONSTRAINT(list_like) L>
    using empty = bool_<0 == size<L>::type::value>;

    namespace lazy
    {
        /// \sa 'meta::empty'
        /// \ingroup lazy_list
        template <typename L>
        using empty = defer<empty, L>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // pair
    /// A list with exactly two elements
    /// \ingroup list
    template <typename F, typename S>
    using pair = list<F, S>;

    /// Retrieve the first element of the \c pair \p Pair
    /// \ingroup list
    template <typename Pair>
    using first = front<Pair>;

    /// Retrieve the first element of the \c pair \p Pair
    /// \ingroup list
    template <typename Pair>
    using second = front<pop_front<Pair>>;

    namespace lazy
    {
        /// \sa 'meta::first'
        /// \ingroup lazy_list
        template <typename Pair>
        using first = defer<first, Pair>;

        /// \sa 'meta::second'
        /// \ingroup lazy_list
        template <typename Pair>
        using second = defer<second, Pair>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // find_index
    /// \cond
    namespace detail
    {
        // With thanks to Peter Dimov:
        constexpr std::size_t find_index_i_(bool const *const first, bool const *const last,
                                            std::size_t N = 0)
        {
            return first == last ? npos::value
                                 : *first ? N : find_index_i_(first + 1, last, N + 1);
        }

        template <typename L, typename T>
        struct find_index_
        {
        };

        template <typename V>
        struct find_index_<list<>, V>
        {
            using type = npos;
        };

        template <typename... T, typename V>
        struct find_index_<list<T...>, V>
        {
#ifdef META_WORKAROUND_LLVM_28385
            static constexpr bool s_v[sizeof...(T)] = {META_IS_SAME(T, V)...};
#else
            static constexpr bool s_v[] = {META_IS_SAME(T, V)...};
#endif
            using type = size_t<find_index_i_(s_v, s_v + sizeof...(T))>;
        };
    } // namespace detail
    /// \endcond

    /// Finds the index of the first occurrence of the type \p T within the list \p L.
    /// Returns `#meta::npos` if the type \p T was not found.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup query
    /// \sa `meta::npos`
    template <META_TYPE_CONSTRAINT(list_like) L, typename T>
    using find_index = _t<detail::find_index_<L, T>>;

    namespace lazy
    {
        /// \sa 'meta::find_index'
        /// \ingroup lazy_query
        template <typename L, typename T>
        using find_index = defer<find_index, L, T>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // reverse_find_index
    /// \cond
    namespace detail
    {
        // With thanks to Peter Dimov:
        constexpr std::size_t reverse_find_index_i_(bool const *const first,
                                                    bool const *const last, std::size_t N)
        {
            return first == last
                ? npos::value
                : *(last - 1) ? N - 1 : reverse_find_index_i_(first, last - 1, N - 1);
        }

        template <typename L, typename T>
        struct reverse_find_index_
        {
        };

        template <typename V>
        struct reverse_find_index_<list<>, V>
        {
            using type = npos;
        };

        template <typename... T, typename V>
        struct reverse_find_index_<list<T...>, V>
        {
#ifdef META_WORKAROUND_LLVM_28385
            static constexpr bool s_v[sizeof...(T)] = {META_IS_SAME(T, V)...};
#else
            static constexpr bool s_v[] = {META_IS_SAME(T, V)...};
#endif
            using type = size_t<reverse_find_index_i_(s_v, s_v + sizeof...(T), sizeof...(T))>;
        };
    } // namespace detail
    /// \endcond

    /// Finds the index of the last occurrence of the type \p T within the
    /// list \p L. Returns `#meta::npos` if the type \p T was not found.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup query
    /// \sa `#meta::npos`
    template <META_TYPE_CONSTRAINT(list_like) L, typename T>
    using reverse_find_index = _t<detail::reverse_find_index_<L, T>>;

    namespace lazy
    {
        /// \sa 'meta::reverse_find_index'
        /// \ingroup lazy_query
        template <typename L, typename T>
        using reverse_find_index = defer<reverse_find_index, L, T>;
    } // namespace lazy

    ////////////////////////////////////////////////////////////////////////////////////
    // find
    /// Return the tail of the list \p L starting at the first occurrence of
    /// \p T, if any such element exists; the empty list, otherwise.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup query
    template <META_TYPE_CONSTRAINT(list_like) L, typename T>
    using find = drop<L, min<find_index<L, T>, size<L>>>;

    namespace lazy
    {
        /// \sa 'meta::find'
        /// \ingroup lazy_query
        template <typename L, typename T>
        using find = defer<find, L, T>;
    } // namespace lazy

    ////////////////////////////////////////////////////////////////////////////////////
    // reverse_find
    /// \cond
    namespace detail
    {
        template <typename L, typename T, typename State = list<>>
        struct reverse_find_
        {
        };

        template <typename T, typename State>
        struct reverse_find_<list<>, T, State>
        {
            using type = State;
        };

        template <typename Head, typename... L, typename T, typename State>
        struct reverse_find_<list<Head, L...>, T, State> : reverse_find_<list<L...>, T, State>
        {
        };

        template <typename... L, typename T, typename State>
        struct reverse_find_<list<T, L...>, T, State>
          : reverse_find_<list<L...>, T, list<T, L...>>
        {
        };
    } // namespace detail
    /// \endcond

    /// Return the tail of the list \p L starting at the last occurrence of \p T, if any such
    /// element exists; the empty list, otherwise.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup query
    template <META_TYPE_CONSTRAINT(list_like) L, typename T>
    using reverse_find = drop<L, min<reverse_find_index<L, T>, size<L>>>;

    namespace lazy
    {
        /// \sa 'meta::rfind'
        /// \ingroup lazy_query
        template <typename L, typename T>
        using reverse_find = defer<reverse_find, L, T>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // find_if
    /// \cond
    namespace detail
    {
#ifdef META_CONCEPT
        template <typename L, typename Fn>
        struct find_if_
        {
        };

        template <typename Fn>
        struct find_if_<list<>, Fn>
        {
            using type = list<>;
        };

        template <typename Head, typename... L, typename Fn>
        requires integral<invoke<Fn, Head>>
        struct find_if_<list<Head, L...>, Fn>
          : if_<invoke<Fn, Head>, id<list<Head, L...>>, find_if_<list<L...>, Fn>>
        {
        };
#else
        constexpr bool const *find_if_i_(bool const *const begin, bool const *const end)
        {
            return begin == end || *begin ? begin : find_if_i_(begin + 1, end);
        }

        template <typename L, typename Fn, typename = void>
        struct find_if_
        {
        };

        template <typename Fn>
        struct find_if_<list<>, Fn>
        {
            using type = list<>;
        };

        template <typename... L, typename Fn>
        struct find_if_<list<L...>, Fn,
                        void_<integer_sequence<bool, bool(invoke<Fn, L>::type::value)...>>>
        {
#ifdef META_WORKAROUND_LLVM_28385
            static constexpr bool s_v[sizeof...(L)] = {invoke<Fn, L>::type::value...};
#else
            static constexpr bool s_v[] = {invoke<Fn, L>::type::value...};
#endif
            using type =
                drop_c<list<L...>, detail::find_if_i_(s_v, s_v + sizeof...(L)) - s_v>;
        };
#endif
    } // namespace detail
    /// \endcond

    /// Return the tail of the list \p L starting at the first element `A`
    /// such that `invoke<Fn, A>::%value` is \c true, if any such element
    /// exists; the empty list, otherwise.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup query
    template <META_TYPE_CONSTRAINT(list_like) L, META_TYPE_CONSTRAINT(invocable) Fn>
    using find_if = _t<detail::find_if_<L, Fn>>;

    namespace lazy
    {
        /// \sa 'meta::find_if'
        /// \ingroup lazy_query
        template <typename L, typename Fn>
        using find_if = defer<find_if, L, Fn>;
    } // namespace lazy

    ////////////////////////////////////////////////////////////////////////////////////
    // reverse_find_if
    /// \cond
    namespace detail
    {
#ifdef META_CONCEPT
        template <typename L, typename Fn, typename State = list<>>
        struct reverse_find_if_
        {
        };

        template <typename Fn, typename State>
        struct reverse_find_if_<list<>, Fn, State>
        {
            using type = State;
        };

        template <typename Head, typename... L, typename Fn, typename State>
        requires integral<invoke<Fn, Head>>
        struct reverse_find_if_<list<Head, L...>, Fn, State>
          : reverse_find_if_<list<L...>, Fn, if_<invoke<Fn, Head>, list<Head, L...>, State>>
        {
        };
#else
        constexpr bool const *reverse_find_if_i_(bool const *const begin, bool const *const pos,
                                                    bool const *const end)
        {
            return begin == pos
                ? end
                : *(pos - 1) ? pos - 1 : reverse_find_if_i_(begin, pos - 1, end);
        }

        template <typename L, typename Fn, typename = void>
        struct reverse_find_if_
        {
        };

        template <typename Fn>
        struct reverse_find_if_<list<>, Fn>
        {
            using type = list<>;
        };

        template <typename... L, typename Fn>
        struct reverse_find_if_<
            list<L...>, Fn,
            void_<integer_sequence<bool, bool(invoke<Fn, L>::type::value)...>>>
        {
#ifdef META_WORKAROUND_LLVM_28385
            static constexpr bool s_v[sizeof...(L)] = {invoke<Fn, L>::type::value...};
#else
            static constexpr bool s_v[] = {invoke<Fn, L>::type::value...};
#endif
            using type =
                drop_c<list<L...>, detail::reverse_find_if_i_(s_v, s_v + sizeof...(L),
                                                                    s_v + sizeof...(L)) -
                                            s_v>;
        };
#endif
    } // namespace detail
    /// \endcond

    /// Return the tail of the list \p L starting at the last element `A`
    /// such that `invoke<Fn, A>::%value` is \c true, if any such element
    /// exists; the empty list, otherwise.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup query
    template <META_TYPE_CONSTRAINT(list_like) L, META_TYPE_CONSTRAINT(invocable) Fn>
    using reverse_find_if = _t<detail::reverse_find_if_<L, Fn>>;

    namespace lazy
    {
        /// \sa 'meta::rfind_if'
        /// \ingroup lazy_query
        template <typename L, typename Fn>
        using reverse_find_if = defer<reverse_find_if, L, Fn>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // replace
    /// \cond
    namespace detail
    {
        template <typename L, typename T, typename U>
        struct replace_
        {
        };

        template <typename... L, typename T, typename U>
        struct replace_<list<L...>, T, U>
        {
            using type = list<if_c<META_IS_SAME(T, L), U, L>...>;
        };
    } // namespace detail
    /// \endcond

    /// Return a new \c meta::list where all instances of type \p T have
    /// been replaced with \p U.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) L, typename T, typename U>
    using replace = _t<detail::replace_<L, T, U>>;

    namespace lazy
    {
        /// \sa 'meta::replace'
        /// \ingroup lazy_transformation
        template <typename L, typename T, typename U>
        using replace = defer<replace, T, U>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // replace_if
    /// \cond
    namespace detail
    {
#ifdef META_CONCEPT
        template <typename L, typename C, typename U>
        struct replace_if_
        {
        };

        template <typename... L, typename C, typename U>
        requires and_v<integral<invoke<C, L>>...>
        struct replace_if_<list<L...>, C, U>
        {
            using type = list<if_<invoke<C, L>, U, L>...>;
        };
#else
        template <typename L, typename C, typename U, typename = void>
        struct replace_if_
        {
        };

        template <typename... L, typename C, typename U>
        struct replace_if_<list<L...>, C, U,
                            void_<integer_sequence<bool, bool(invoke<C, L>::type::value)...>>>
        {
            using type = list<if_<invoke<C, L>, U, L>...>;
        };
#endif
    } // namespace detail
    /// \endcond

    /// Return a new \c meta::list where all elements \c A of the list \p L
    /// for which `invoke<C,A>::%value` is \c true have been replaced with
    /// \p U.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) L, typename C, typename U>
    using replace_if = _t<detail::replace_if_<L, C, U>>;

    namespace lazy
    {
        /// \sa 'meta::replace_if'
        /// \ingroup lazy_transformation
        template <typename L, typename C, typename U>
        using replace_if = defer<replace_if, C, U>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////
    // count
    namespace detail
    {
        template <typename, typename>
        struct count_
        {
        };

#if (defined(META_CONCEPT) || META_CXX_VARIABLE_TEMPLATES) && META_CXX_FOLD_EXPRESSIONS
        template <typename... Ts, typename T>
        struct count_<list<Ts...>, T>
        {
            using type = meta::size_t<((std::size_t)META_IS_SAME(T, Ts) + ...)>;
        };
#else
        constexpr std::size_t count_i_(bool const *const begin, bool const *const end,
                                       std::size_t n)
        {
            return begin == end ? n : detail::count_i_(begin + 1, end, n + *begin);
        }

        template <typename T>
        struct count_<list<>, T>
        {
            using type = meta::size_t<0>;
        };

        template <typename... L, typename T>
        struct count_<list<L...>, T>
        {
#ifdef META_WORKAROUND_LLVM_28385
            static constexpr bool s_v[sizeof...(L)] = {META_IS_SAME(T, L)...};
#else
            static constexpr bool s_v[] = {META_IS_SAME(T, L)...};
#endif
            using type = meta::size_t<detail::count_i_(s_v, s_v + sizeof...(L), 0u)>;
        };
#endif
    } // namespace detail

    /// Count the number of times a type \p T appears in the list \p L.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup query
    template <META_TYPE_CONSTRAINT(list_like) L, typename T>
    using count = _t<detail::count_<L, T>>;

    namespace lazy
    {
        /// \sa `meta::count`
        /// \ingroup lazy_query
        template <typename L, typename T>
        using count = defer<count, L, T>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////
    // count_if
    namespace detail
    {
#if defined(META_CONCEPT) && META_CXX_FOLD_EXPRESSIONS
        template <typename, typename>
        struct count_if_
        {
        };

        template <typename... Ts, typename Fn>
        requires (integral<invoke<Fn, Ts>> && ...)
        struct count_if_<list<Ts...>, Fn>
        {
            using type = meta::size_t<((std::size_t)(bool)_v<invoke<Fn, Ts>> + ...)>;
        };
#else
        template <typename L, typename Fn, typename = void>
        struct count_if_
        {
        };

        template <typename Fn>
        struct count_if_<list<>, Fn>
        {
            using type = meta::size_t<0>;
        };

        template <typename... L, typename Fn>
        struct count_if_<list<L...>, Fn,
                            void_<integer_sequence<bool, bool(invoke<Fn, L>::type::value)...>>>
        {
#if META_CXX_FOLD_EXPRESSIONS
            using type = meta::size_t<((std::size_t)(bool)invoke<Fn, L>::type::value + ...)>;
#else
#ifdef META_WORKAROUND_LLVM_28385
            static constexpr bool s_v[sizeof...(L)] = {invoke<Fn, L>::type::value...};
#else
            static constexpr bool s_v[] = {invoke<Fn, L>::type::value...};
#endif
            using type = meta::size_t<detail::count_i_(s_v, s_v + sizeof...(L), 0u)>;
#endif  // META_CXX_FOLD_EXPRESSIONS
        };
#endif  // META_CONCEPT
    } // namespace detail

    /// Count the number of times the predicate \p Fn evaluates to true for all the elements in
    /// the list \p L.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup query
    template <META_TYPE_CONSTRAINT(list_like) L, META_TYPE_CONSTRAINT(invocable) Fn>
    using count_if = _t<detail::count_if_<L, Fn>>;

    namespace lazy
    {
        /// \sa `meta::count_if`
        /// \ingroup lazy_query
        template <typename L, typename Fn>
        using count_if = defer<count_if, L, Fn>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // filter
    /// \cond
    namespace detail
    {
        template <typename Pred>
        struct filter_
        {
            template <typename A>
            using invoke = if_c<invoke<Pred, A>::type::value, list<A>, list<>>;
        };
    } // namespace detail
    /// \endcond

    /// Returns a new meta::list where only those elements of \p L that satisfy the
    /// Callable \p Pred such that `invoke<Pred,A>::%value` is \c true are present.
    /// That is, those elements that don't satisfy the \p Pred are "removed".
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup transformation
    template <typename L, typename Pred>
    using filter = join<transform<L, detail::filter_<Pred>>>;

    namespace lazy
    {
        /// \sa 'meta::filter'
        /// \ingroup lazy_transformation
        template <typename L, typename Fn>
        using filter = defer<filter, L, Fn>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // static_const
    ///\cond
    namespace detail
    {
        template <typename T>
        struct static_const
        {
            static constexpr T value{};
        };

        // Avoid potential ODR violations with global objects:
        template <typename T>
        constexpr T static_const<T>::value;
    } // namespace detail

    ///\endcond

    ///////////////////////////////////////////////////////////////////////////////////////////
    // for_each
    /// \cond
    namespace detail
    {
        struct for_each_fn
        {
            template <class Fn, class... Args>
            constexpr auto operator()(list<Args...>, Fn f) const -> Fn
            {
                return (void)std::initializer_list<int>{((void)f(Args{}), 0)...}, f;
            }
        };
    } // namespace detail
    /// \endcond

#if META_CXX_INLINE_VARIABLES
    /// `for_each(L, Fn)` calls the \p Fn for each
    /// argument in the \p L.
    /// \ingroup runtime
    inline constexpr detail::for_each_fn for_each{};
#else
    ///\cond
    namespace
    {
        /// \endcond

        /// `for_each(List, UnaryFunction)` calls the \p UnaryFunction for each
        /// argument in the \p List.
        /// \ingroup runtime
        constexpr auto &&for_each = detail::static_const<detail::for_each_fn>::value;

        /// \cond
    }
    /// \endcond
#endif

    ///////////////////////////////////////////////////////////////////////////////////////////
    // transpose
    /// Given a list of lists of types \p ListOfLists, transpose the elements from the lists.
    /// \par Complexity
    /// `O(N * M)`, where `N` is the size of the outer list, and
    /// `M` is the size of the inner lists.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) ListOfLists>
    using transpose = fold<ListOfLists, repeat_n<size<front<ListOfLists>>, list<>>,
                            bind_back<quote<transform>, quote<push_back>>>;

    namespace lazy
    {
        /// \sa 'meta::transpose'
        /// \ingroup lazy_transformation
        template <typename ListOfLists>
        using transpose = defer<transpose, ListOfLists>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // zip_with
    /// Given a list of lists of types \p ListOfLists and an invocable \p Fn, construct a new
    /// list by calling \p Fn with the elements from the lists pairwise.
    /// \par Complexity
    /// `O(N * M)`, where `N` is the size of the outer list, and
    /// `M` is the size of the inner lists.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(invocable) Fn, META_TYPE_CONSTRAINT(list_like) ListOfLists>
    using zip_with = transform<transpose<ListOfLists>, uncurry<Fn>>;

    namespace lazy
    {
        /// \sa 'meta::zip_with'
        /// \ingroup lazy_transformation
        template <typename Fn, typename ListOfLists>
        using zip_with = defer<zip_with, Fn, ListOfLists>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // zip
    /// Given a list of lists of types \p ListOfLists, construct a new list by grouping the
    /// elements from the lists pairwise into `meta::list`s.
    /// \par Complexity
    /// `O(N * M)`, where `N` is the size of the outer list, and `M`
    /// is the size of the inner lists.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) ListOfLists>
    using zip = transpose<ListOfLists>;

    namespace lazy
    {
        /// \sa 'meta::zip'
        /// \ingroup lazy_transformation
        template <typename ListOfLists>
        using zip = defer<zip, ListOfLists>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // as_list
    /// \cond
    namespace detail
    {
        template <typename T>
        using uncvref_t = _t<std::remove_cv<_t<std::remove_reference<T>>>>;

        // Indirection here needed to avoid Core issue 1430
        // https://wg21.link/cwg1430
        template <typename Sequence>
        struct as_list_ : lazy::invoke<uncurry<quote<list>>, Sequence>
        {
        };
    } // namespace detail
    /// \endcond

    /// Turn a type into an instance of \c meta::list in a way determined by
    /// \c meta::apply.
    /// \ingroup list
    template <typename Sequence>
    using as_list = _t<detail::as_list_<detail::uncvref_t<Sequence>>>;

    namespace lazy
    {
        /// \sa 'meta::as_list'
        /// \ingroup lazy_list
        template <typename Sequence>
        using as_list = defer<as_list, Sequence>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // reverse
    /// \cond
    namespace detail
    {
        template <typename L, typename State = list<>>
        struct reverse_ : lazy::fold<L, State, quote<push_front>>
        {
        };

        template <typename T0, typename T1, typename T2, typename T3, typename T4, typename T5,
                    typename T6, typename T7, typename T8, typename T9, typename... Ts,
                    typename... Us>
        struct reverse_<list<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, Ts...>, list<Us...>>
          : reverse_<list<Ts...>, list<T9, T8, T7, T6, T5, T4, T3, T2, T1, T0, Us...>>
        {
        };
    }
    /// \endcond

    /// Return a new \c meta::list by reversing the elements in the list \p L.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) L>
    using reverse = _t<detail::reverse_<L>>;

    namespace lazy
    {
        /// \sa 'meta::reverse'
        /// \ingroup lazy_transformation
        template <typename L>
        using reverse = defer<reverse, L>;
    } // namespace lazy

    /// Logically negate the result of invocable \p Fn.
    /// \ingroup trait
    template <META_TYPE_CONSTRAINT(invocable) Fn>
    using not_fn = compose<quote<not_>, Fn>;

    namespace lazy
    {
        /// \sa 'meta::not_fn'
        /// \ingroup lazy_trait
        template <typename Fn>
        using not_fn = defer<not_fn, Fn>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // all_of
    /// A Boolean integral constant wrapper around \c true if `invoke<Fn, A>::%value` is \c true
    /// for all elements \c A in \c meta::list \p L; \c false, otherwise.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup query
    template <META_TYPE_CONSTRAINT(list_like) L, META_TYPE_CONSTRAINT(invocable) Fn>
    using all_of = empty<find_if<L, not_fn<Fn>>>;

    namespace lazy
    {
        /// \sa 'meta::all_of'
        /// \ingroup lazy_query
        template <typename L, typename Fn>
        using all_of = defer<all_of, L, Fn>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // any_of
    /// A Boolean integral constant wrapper around \c true if `invoke<Fn, A>::%value` is
    /// \c true for any element \c A in \c meta::list \p L; \c false, otherwise.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup query
    template <META_TYPE_CONSTRAINT(list_like) L, META_TYPE_CONSTRAINT(invocable) Fn>
    using any_of = not_<empty<find_if<L, Fn>>>;

    namespace lazy
    {
        /// \sa 'meta::any_of'
        /// \ingroup lazy_query
        template <typename L, typename Fn>
        using any_of = defer<any_of, L, Fn>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // none_of
    /// A Boolean integral constant wrapper around \c true if `invoke<Fn, A>::%value` is
    /// \c false for all elements \c A in \c meta::list \p L; \c false, otherwise.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup query
    template <META_TYPE_CONSTRAINT(list_like) L, META_TYPE_CONSTRAINT(invocable) Fn>
    using none_of = empty<find_if<L, Fn>>;

    namespace lazy
    {
        /// \sa 'meta::none_of'
        /// \ingroup lazy_query
        template <typename L, META_TYPE_CONSTRAINT(invocable) Fn>
        using none_of = defer<none_of, L, Fn>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // in
    /// A Boolean integral constant wrapper around \c true if there is at least one occurrence
    /// of \p T in \p L.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup query
    template <META_TYPE_CONSTRAINT(list_like) L, typename T>
    using in = not_<empty<find<L, T>>>;

    namespace lazy
    {
        /// \sa 'meta::in'
        /// \ingroup lazy_query
        template <typename L, typename T>
        using in = defer<in, L, T>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // inherit
    /// \cond
    namespace detail
    {
        template <typename L>
        struct inherit_
        {
        };

        template <typename... L>
        struct inherit_<list<L...>> : L...
        {
            using type = inherit_;
        };
    } // namespace detail
    /// \endcond

    /// A type that inherits from all the types in the list
    /// \pre The types in the list must be unique
    /// \pre All the types in the list must be non-final class types
    /// \ingroup datatype
    template <META_TYPE_CONSTRAINT(list_like) L>
    using inherit = meta::_t<detail::inherit_<L>>;

    namespace lazy
    {
        /// \sa 'meta::inherit'
        /// \ingroup lazy_datatype
        template <typename L>
        using inherit = defer<inherit, L>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // unique
    /// \cond
    namespace detail
    {
        template <typename Set, typename T>
        struct in_
        {
        };

        template <typename... Set, typename T>
        struct in_<list<Set...>, T> : bool_<META_IS_BASE_OF(id<T>, inherit<list<id<Set>...>>)>
        {
        };

        template <typename Set, typename T>
        struct insert_back_
        {
        };

        template <typename... Set, typename T>
        struct insert_back_<list<Set...>, T>
        {
            using type = if_<in_<list<Set...>, T>, list<Set...>, list<Set..., T>>;
        };
    } // namespace detail
    /// \endcond

    /// Return a new \c meta::list where all duplicate elements have been removed.
    /// \par Complexity
    /// `O(N^2)`.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) L>
    using unique = fold<L, list<>, quote_trait<detail::insert_back_>>;

    namespace lazy
    {
        /// \sa 'meta::unique'
        /// \ingroup lazy_transformation
        template <typename L>
        using unique = defer<unique, L>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // partition
    /// \cond
    namespace detail
    {
        template <typename Fn>
        struct partition_
        {
#ifdef META_CONCEPT
            template <typename, typename>
#else
            template <typename, typename, typename = void>
#endif
            struct impl
            {
            };
            template <typename... Yes, typename... No, typename A>
#ifdef META_CONCEPT
            requires integral<invoke<Fn, A>>
            struct impl<pair<list<Yes...>, list<No...>>, A>
#else
            struct impl<pair<list<Yes...>, list<No...>>, A,
                        void_<bool_<invoke<Fn, A>::type::value>>>
#endif
            {
                using type = if_<meta::invoke<Fn, A>, pair<list<Yes..., A>, list<No...>>,
                                    pair<list<Yes...>, list<No..., A>>>;
            };

            template <typename State, typename A>
            using invoke = _t<impl<State, A>>;
        };
    } // namespace detail
    /// \endcond

    /// Returns a pair of lists, where the elements of \p L that satisfy the
    /// invocable \p Fn such that `invoke<Fn,A>::%value` is \c true are present in the
    /// first list and the rest are in the second.
    /// \par Complexity
    /// `O(N)`.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) L, META_TYPE_CONSTRAINT(invocable) Fn>
    using partition = fold<L, pair<list<>, list<>>, detail::partition_<Fn>>;

    namespace lazy
    {
        /// \sa 'meta::partition'
        /// \ingroup lazy_transformation
        template <typename L, typename Fn>
        using partition = defer<partition, L, Fn>;
    } // namespace lazy

    ///////////////////////////////////////////////////////////////////////////////////////////
    // sort
    /// \cond
    namespace detail
    {
        template <META_TYPE_CONSTRAINT(invocable) Fn, typename A, typename B, typename... Ts>
        using part_ = partition<list<B, Ts...>, bind_back<Fn, A>>;
#ifdef META_CONCEPT
        template <typename L, typename Fn>
            requires list_like<L> && invocable<Fn>
#else
        template <typename, typename, typename = void>
#endif
        struct sort_
        {
        };
        template <typename Fn>
        struct sort_<list<>, Fn>
        {
            using type = list<>;
        };

        template <typename A, typename Fn>
        struct sort_<list<A>, Fn>
        {
            using type = list<A>;
        };

        template <typename A, typename B, typename... Ts, typename Fn>
#ifdef META_CONCEPT
            requires trait<sort_<first<part_<Fn, A, B, Ts...>>, Fn>> &&
                trait<sort_<second<part_<Fn, A, B, Ts...>>, Fn>>
        struct sort_<list<A, B, Ts...>, Fn>
#else
        struct sort_<
            list<A, B, Ts...>, Fn,
            void_<_t<sort_<first<part_<Fn, A, B, Ts...>>, Fn>>>>
#endif
        {
            using P = part_<Fn, A, B, Ts...>;
            using type = concat<_t<sort_<first<P>, Fn>>, list<A>, _t<sort_<second<P>, Fn>>>;
        };
    } // namespace detail
    /// \endcond

    // clang-format off
    /// Return a new \c meta::list that is sorted according to invocable predicate \p Fn.
    /// \par Complexity
    /// Expected: `O(N log N)`
    /// Worst case: `O(N^2)`.
    /// \code
    /// using L0 = list<char[5], char[3], char[2], char[6], char[1], char[5], char[10]>;
    /// using L1 = meta::sort<L0, lambda<_a, _b, lazy::less<lazy::sizeof_<_a>, lazy::sizeof_<_b>>>>;
    /// static_assert(std::is_same_v<L1, list<char[1], char[2], char[3], char[5], char[5], char[6], char[10]>>, "");
    /// \endcode
    /// \ingroup transformation
    // clang-format on
    template <META_TYPE_CONSTRAINT(list_like) L, META_TYPE_CONSTRAINT(invocable) Fn>
    using sort = _t<detail::sort_<L, Fn>>;

    namespace lazy
    {
        /// \sa 'meta::sort'
        /// \ingroup lazy_transformation
        template <typename L, typename Fn>
        using sort = defer<sort, L, Fn>;
    } // namespace lazy

    ////////////////////////////////////////////////////////////////////////////
    // lambda_
    /// \cond
    namespace detail
    {
        template <typename T, int = 0>
        struct protect_;

        template <typename, int = 0>
        struct vararg_;

        template <typename T, int = 0>
        struct is_valid_;

        // Returns which branch to evaluate
        template <typename If, typename... Ts>
#ifdef META_CONCEPT
        using lazy_if_ = lazy::_t<defer<_if_, If, protect_<Ts>...>>;
#else
        using lazy_if_ = lazy::_t<defer<_if_, list<If, protect_<Ts>...>>>;
#endif

        template <typename A, typename T, typename Fn, typename Ts>
        struct subst1_
        {
            using type = list<list<T>>;
        };
        template <typename T, typename Fn, typename Ts>
        struct subst1_<Fn, T, Fn, Ts>
        {
            using type = list<>;
        };
        template <typename A, typename T, typename Fn, typename Ts>
        struct subst1_<vararg_<A>, T, Fn, Ts>
        {
            using type = list<Ts>;
        };

        template <typename As, typename Ts>
        using substitutions_ = push_back<
            join<transform<
                concat<As, repeat_n_c<size<Ts>{} + 2 - size<As>{}, back<As>>>,
                concat<Ts, repeat_n_c<2, back<As>>>,
                bind_back<quote_trait<subst1_>, back<As>, drop_c<Ts, size<As>{} - 2>>>>,
            list<back<As>>>;

#if 0//def META_CONCEPT
        template <list_like As, list_like Ts>
        requires (_v<size<Ts>> + 2 >= _v<size<As>>)
        using substitutions = substitutions_<As, Ts>;
#else // ^^^ concepts / no concepts vvv
        template <typename As, typename Ts>
        using substitutions =
#ifdef META_WORKAROUND_MSVC_702792
            invoke<if_c<(size<Ts>::value + 2 >= size<As>::value), quote<substitutions_>>, As,
                    Ts>;
#else // ^^^ workaround ^^^ / vvv no workaround vvv
            invoke<if_c<(size<Ts>{} + 2 >= size<As>{}), quote<substitutions_>>, As, Ts>;
#endif // META_WORKAROUND_MSVC_702792
#endif // META_CONCEPT

        template <typename T>
        struct is_vararg_ : std::false_type
        {
        };
        template <typename T>
        struct is_vararg_<vararg_<T>> : std::true_type
        {
        };

        template <META_TYPE_CONSTRAINT(list_like) Tags>
        using is_variadic_ = is_vararg_<at<push_front<Tags, void>, dec<size<Tags>>>>;

        template <META_TYPE_CONSTRAINT(list_like) Tags, bool IsVariadic = is_variadic_<Tags>::value>
        struct lambda_;

        // Non-variadic lambda implementation
        template <typename... As>
        struct lambda_<list<As...>, false>
        {
        private:
            static constexpr std::size_t arity = sizeof...(As) - 1;
            using Tags = list<As...>; // Includes the lambda body as the last arg!
            using Fn = back<Tags>;
            template <typename T, META_TYPE_CONSTRAINT(list_like) Args>
            struct impl;
            template <typename T, META_TYPE_CONSTRAINT(list_like) Args>
            using lazy_impl_ = lazy::_t<defer<impl, T, protect_<Args>>>;
#if 0//def META_CONCEPT
            template <typename, list_like>
#else
            template <typename, typename, typename = void>
#endif
            struct subst_
            {
            };
            template <template <typename...> class C, typename... Ts, typename Args>
#if 0//def META_CONCEPT
            requires valid<C, _t<impl<Ts, Args>>...> struct subst_<defer<C, Ts...>, Args>
#else
            struct subst_<defer<C, Ts...>, Args, void_<C<_t<impl<Ts, Args>>...>>>
#endif
            {
                using type = C<_t<impl<Ts, Args>>...>;
            };
            template <typename T, template <T...> class C, T... Is, typename Args>
#if 0//def META_CONCEPT
            requires valid_i<T, C, Is...> struct subst_<defer_i<T, C, Is...>, Args>
#else
            struct subst_<defer_i<T, C, Is...>, Args, void_<C<Is...>>>
#endif
            {
                using type = C<Is...>;
            };
            template <typename T, META_TYPE_CONSTRAINT(list_like) Args>
            struct impl : if_c<(reverse_find_index<Tags, T>() != npos()),
                                lazy::at<Args, reverse_find_index<Tags, T>>, id<T>>
            {
            };
            template <typename T, typename Args>
            struct impl<protect_<T>, Args>
            {
                using type = T;
            };
            template <typename T, typename Args>
            struct impl<is_valid_<T>, Args>
            {
                using type = is_trait<impl<T, Args>>;
            };
            template <typename If, typename... Ts, typename Args>
            struct impl<defer<if_, If, Ts...>, Args> // Short-circuit if_
              : impl<lazy_impl_<lazy_if_<If, Ts...>, Args>, Args>
            {
            };
            template <typename B, typename... Bs, typename Args>
            struct impl<defer<and_, B, Bs...>, Args> // Short-circuit and_
              : impl<lazy_impl_<lazy_if_<B, lazy::and_<Bs...>, protect_<std::false_type>>, Args>,
                     Args>
            {
            };
            template <typename B, typename... Bs, typename Args>
            struct impl<defer<or_, B, Bs...>, Args> // Short-circuit or_
              : impl<lazy_impl_<lazy_if_<B, protect_<std::true_type>, lazy::or_<Bs...>>, Args>,
                     Args>
            {
            };
            template <template <typename...> class C, typename... Ts, typename Args>
            struct impl<defer<C, Ts...>, Args> : subst_<defer<C, Ts...>, Args>
            {
            };
            template <typename T, template <T...> class C, T... Is, typename Args>
            struct impl<defer_i<T, C, Is...>, Args> : subst_<defer_i<T, C, Is...>, Args>
            {
            };
            template <template <typename...> class C, typename... Ts, typename Args>
            struct impl<C<Ts...>, Args> : subst_<defer<C, Ts...>, Args>
            {
            };
            template <typename... Ts, typename Args>
            struct impl<lambda_<list<Ts...>, false>, Args>
            {
                using type = compose<uncurry<lambda_<list<As..., Ts...>, false>>,
                                        curry<bind_front<quote<concat>, Args>>>;
            };
            template <typename... Bs, typename Args>
            struct impl<lambda_<list<Bs...>, true>, Args>
            {
                using type = compose<typename lambda_<list<As..., Bs...>, true>::thunk,
                                        bind_front<quote<concat>, transform<Args, quote<list>>>,
                                        curry<bind_front<quote<substitutions>, list<Bs...>>>>;
            };

        public:
            template <typename... Ts>
#ifdef META_CONCEPT
                requires (sizeof...(Ts) == arity) using invoke = _t<impl<Fn, list<Ts..., Fn>>>;
#else
            using invoke = _t<if_c<sizeof...(Ts) == arity, impl<Fn, list<Ts..., Fn>>>>;
#endif
        };

        // Lambda with variadic placeholder (broken out due to less efficient compile-time
        // resource usage)
        template <typename... As>
        struct lambda_<list<As...>, true>
        {
        private:
            template <META_TYPE_CONSTRAINT(list_like) T, bool IsVar>
            friend struct lambda_;
            using Tags = list<As...>; // Includes the lambda body as the last arg!
            template <typename T, META_TYPE_CONSTRAINT(list_like) Args>
            struct impl;
            template <META_TYPE_CONSTRAINT(list_like) Args>
            using eval_impl_ = bind_back<quote_trait<impl>, Args>;
            template <typename T, META_TYPE_CONSTRAINT(list_like) Args>
            using lazy_impl_ = lazy::_t<defer<impl, T, protect_<Args>>>;
            template <template <typename...> class C, META_TYPE_CONSTRAINT(list_like) Args,
                        META_TYPE_CONSTRAINT(list_like) Ts>
            using try_subst_ = apply<quote<C>, join<transform<Ts, eval_impl_<Args>>>>;
#if 0//def META_CONCEPT
            template <typename, list_like>
#else
            template <typename, typename, typename = void>
#endif
            struct subst_
            {
            };
            template <template <typename...> class C, typename... Ts, typename Args>
#if 0//def META_CONCEPT
            requires is_true<try_subst_<C, Args, list<Ts...>>> struct subst_<defer<C, Ts...>, Args>
#else
            struct subst_<defer<C, Ts...>, Args, void_<try_subst_<C, Args, list<Ts...>>>>
#endif
            {
                using type = list<try_subst_<C, Args, list<Ts...>>>;
            };
            template <typename T, template <T...> class C, T... Is, typename Args>
#if 0//def META_CONCEPT
            requires valid_i<T, C, Is...> struct subst_<defer_i<T, C, Is...>, Args>
#else
            struct subst_<defer_i<T, C, Is...>, Args, void_<C<Is...>>>
#endif
            {
                using type = list<C<Is...>>;
            };
            template <typename T, META_TYPE_CONSTRAINT(list_like) Args>
            struct impl : if_c<(reverse_find_index<Tags, T>() != npos()),
                                lazy::at<Args, reverse_find_index<Tags, T>>, id<list<T>>>
            {
            };
            template <typename T, typename Args>
            struct impl<protect_<T>, Args>
            {
                using type = list<T>;
            };
            template <typename T, typename Args>
            struct impl<is_valid_<T>, Args>
            {
                using type = list<is_trait<impl<T, Args>>>;
            };
            template <typename If, typename... Ts, typename Args>
            struct impl<defer<if_, If, Ts...>, Args> // Short-circuit if_
              : impl<lazy_impl_<lazy_if_<If, Ts...>, Args>, Args>
            {
            };
            template <typename B, typename... Bs, typename Args>
            struct impl<defer<and_, B, Bs...>, Args> // Short-circuit and_
              : impl<lazy_impl_<lazy_if_<B, lazy::and_<Bs...>, protect_<std::false_type>>, Args>,
                     Args>
            {
            };
            template <typename B, typename... Bs, typename Args>
            struct impl<defer<or_, B, Bs...>, Args> // Short-circuit or_
              : impl<lazy_impl_<lazy_if_<B, protect_<std::true_type>, lazy::or_<Bs...>>, Args>,
                     Args>
            {
            };
            template <template <typename...> class C, typename... Ts, typename Args>
            struct impl<defer<C, Ts...>, Args> : subst_<defer<C, Ts...>, Args>
            {
            };
            template <typename T, template <T...> class C, T... Is, typename Args>
            struct impl<defer_i<T, C, Is...>, Args> : subst_<defer_i<T, C, Is...>, Args>
            {
            };
            template <template <typename...> class C, typename... Ts, typename Args>
            struct impl<C<Ts...>, Args> : subst_<defer<C, Ts...>, Args>
            {
            };
            template <typename... Bs, bool IsVar, typename Args>
            struct impl<lambda_<list<Bs...>, IsVar>, Args>
            {
                using type =
                    list<compose<typename lambda_<list<As..., Bs...>, true>::thunk,
                                    bind_front<quote<concat>, Args>,
                                    curry<bind_front<quote<substitutions>, list<Bs...>>>>>;
            };
            struct thunk
            {
                template <typename S, typename R = _t<impl<back<Tags>, S>>>
#ifdef META_CONCEPT
                    requires (_v<size<R>> == 1) using invoke = front<R>;
#else
                using invoke = if_c<size<R>{} == 1, front<R>>;
#endif
            };

        public:
            template <typename... Ts>
            using invoke = invoke<thunk, substitutions<Tags, list<Ts...>>>;
        };
    } // namespace detail
    /// \endcond

    ///////////////////////////////////////////////////////////////////////////////////////////
    // lambda
    /// For creating anonymous Invocables.
    /// \code
    /// using L = lambda<_a, _b, std::pair<_b, std::pair<_a, _a>>>;
    /// using P = invoke<L, int, short>;
    /// static_assert(std::is_same_v<P, std::pair<short, std::pair<int, int>>>, "");
    /// \endcode
    /// \ingroup trait
    template <typename... Ts>
#ifdef META_CONCEPT
        requires (sizeof...(Ts) > 0) using lambda = detail::lambda_<list<Ts...>>;
#else
    using lambda = if_c<(sizeof...(Ts) > 0), detail::lambda_<list<Ts...>>>;
#endif

    ///////////////////////////////////////////////////////////////////////////////////////////
    // is_valid
    /// For testing whether a deferred computation will succeed in a \c let or a \c lambda.
    /// \ingroup trait
    template <typename T>
    using is_valid = detail::is_valid_<T>;

    ///////////////////////////////////////////////////////////////////////////////////////////
    // vararg
    /// For defining variadic placeholders.
    template <typename T>
    using vararg = detail::vararg_<T>;

    ///////////////////////////////////////////////////////////////////////////////////////////
    // protect
    /// For preventing the evaluation of a nested `defer`ed computation in a \c let or
    /// \c lambda expression.
    template <typename T>
    using protect = detail::protect_<T>;

    ///////////////////////////////////////////////////////////////////////////////////////////
    // var
    /// For use when defining local variables in \c meta::let expressions
    /// \sa `meta::let`
    template <typename Tag, typename Value>
    struct var;

    /// \cond
    namespace detail
    {
        template <typename...>
        struct let_
        {
        };
        template <typename Fn>
        struct let_<Fn>
        {
            using type = lazy::invoke<lambda<Fn>>;
        };
        template <typename Tag, typename Value, typename... Rest>
        struct let_<var<Tag, Value>, Rest...>
        {
            using type = lazy::invoke<lambda<Tag, _t<let_<Rest...>>>, Value>;
        };
    } // namespace detail
    /// \endcond

    /// A lexically scoped expression with local variables.
    ///
    /// \code
    /// template <typename T, typename L>
    /// using find_index_ = let<
    ///     var<_a, L>,
    ///     var<_b, lazy::find<_a, T>>,
    ///     lazy::if_<
    ///         std::is_same<_b, list<>>,
    ///         meta::npos,
    ///         lazy::minus<lazy::size<_a>, lazy::size<_b>>>>;
    /// static_assert(find_index_<int, list<short, int, float>>{} == 1, "");
    /// static_assert(find_index_<double, list<short, int, float>>{} == meta::npos{}, "");
    /// \endcode
    /// \ingroup trait
    template <typename... As>
    using let = _t<_t<detail::let_<As...>>>;

    namespace lazy
    {
        /// \sa `meta::let`
        /// \ingroup lazy_trait
        template <typename... As>
        using let = defer<let, As...>;
    } // namespace lazy

    // Some argument placeholders for use in \c lambda expressions.
    /// \ingroup trait
    inline namespace placeholders
    {
        // regular placeholders:
        struct _a;
        struct _b;
        struct _c;
        struct _d;
        struct _e;
        struct _f;
        struct _g;
        struct _h;
        struct _i;

        // variadic placeholders:
        using _args = vararg<void>;
        using _args_a = vararg<_a>;
        using _args_b = vararg<_b>;
        using _args_c = vararg<_c>;
    } // namespace placeholders

    ///////////////////////////////////////////////////////////////////////////////////////////
    // cartesian_product
    /// \cond
    namespace detail
    {
        template <typename M2, typename M>
        struct cartesian_product_fn
        {
            template <typename X>
            struct lambda0
            {
                template <typename Xs>
                using lambda1 = list<push_front<Xs, X>>;
                using type = join<transform<M2, quote<lambda1>>>;
            };
            using type = join<transform<M, quote_trait<lambda0>>>;
        };
    } // namespace detail
    /// \endcond

    /// Given a list of lists \p ListOfLists, return a new list of lists that is the Cartesian
    /// Product. Like the `sequence` function from the Haskell Prelude.
    /// \par Complexity
    /// `O(N * M)`, where `N` is the size of the outer list, and
    /// `M` is the size of the inner lists.
    /// \ingroup transformation
    template <META_TYPE_CONSTRAINT(list_like) ListOfLists>
    using cartesian_product =
        reverse_fold<ListOfLists, list<list<>>, quote_trait<detail::cartesian_product_fn>>;

    namespace lazy
    {
        /// \sa 'meta::cartesian_product'
        /// \ingroup lazy_transformation
        template <typename ListOfLists>
        using cartesian_product = defer<cartesian_product, ListOfLists>;
    } // namespace lazy

    /// \cond
    ///////////////////////////////////////////////////////////////////////////////////////////
    // add_const_if
    namespace detail
    {
        template <bool>
        struct add_const_if
        {
            template <typename T>
            using invoke = T const;
        };
        template <>
        struct add_const_if<false>
        {
            template <typename T>
            using invoke = T;
        };
    } // namespace detail
    template <bool If>
    using add_const_if_c = detail::add_const_if<If>;
    template <META_TYPE_CONSTRAINT(integral) If>
    using add_const_if = add_const_if_c<If::type::value>;
    /// \endcond

    /// \cond
    ///////////////////////////////////////////////////////////////////////////////////////////
    // const_if
    template <bool If, typename T>
    using const_if_c = typename add_const_if_c<If>::template invoke<T>;
    template <typename If, typename T>
    using const_if = typename add_const_if<If>::template invoke<T>;
    /// \endcond

    /// \cond
    namespace detail
    {
        template <typename State, typename Ch>
        using atoi_ = if_c<(Ch::value >= '0' && Ch::value <= '9'),
                            std::integral_constant<typename State::value_type,
                                                    State::value * 10 + (Ch::value - '0')>>;
    }
    /// \endcond

    inline namespace literals
    {
        /// A user-defined literal that generates objects of type \c meta::size_t.
        /// \ingroup integral
        template <char... Chs>
        constexpr fold<list<char_<Chs>...>, meta::size_t<0>, quote<detail::atoi_>>
            operator"" _z()
        {
            return {};
        }
    } // namespace literals
} // namespace meta

/// \cond
// Non-portable forward declarations of standard containers
#ifndef META_NO_STD_FORWARD_DECLARATIONS
#if defined(__apple_build_version__) || (defined(__clang__) && __clang_major__ < 6)
META_BEGIN_NAMESPACE_STD
META_BEGIN_NAMESPACE_VERSION
template <class>
class META_TEMPLATE_VIS allocator;
template <class, class>
struct META_TEMPLATE_VIS pair;
template <class>
struct META_TEMPLATE_VIS hash;
template <class>
struct META_TEMPLATE_VIS less;
template <class>
struct META_TEMPLATE_VIS equal_to;
template <class>
struct META_TEMPLATE_VIS char_traits;
#if defined(_GLIBCXX_USE_CXX11_ABI) && _GLIBCXX_USE_CXX11_ABI
inline namespace __cxx11 {
#endif
template <class, class, class>
class META_TEMPLATE_VIS basic_string;
#if defined(_GLIBCXX_USE_CXX11_ABI) && _GLIBCXX_USE_CXX11_ABI
}
#endif
META_END_NAMESPACE_VERSION
META_BEGIN_NAMESPACE_CONTAINER
#if defined(__GLIBCXX__)
inline namespace __cxx11 {
#endif
template <class, class>
class META_TEMPLATE_VIS list;
#if defined(__GLIBCXX__)
}
#endif
template <class, class>
class META_TEMPLATE_VIS forward_list;
template <class, class>
class META_TEMPLATE_VIS vector;
template <class, class>
class META_TEMPLATE_VIS deque;
template <class, class, class, class>
class META_TEMPLATE_VIS map;
template <class, class, class, class>
class META_TEMPLATE_VIS multimap;
template <class, class, class>
class META_TEMPLATE_VIS set;
template <class, class, class>
class META_TEMPLATE_VIS multiset;
template <class, class, class, class, class>
class META_TEMPLATE_VIS unordered_map;
template <class, class, class, class, class>
class META_TEMPLATE_VIS unordered_multimap;
template <class, class, class, class>
class META_TEMPLATE_VIS unordered_set;
template <class, class, class, class>
class META_TEMPLATE_VIS unordered_multiset;
template <class, class>
class META_TEMPLATE_VIS queue;
template <class, class, class>
class META_TEMPLATE_VIS priority_queue;
template <class, class>
class META_TEMPLATE_VIS stack;
META_END_NAMESPACE_CONTAINER
META_END_NAMESPACE_STD

namespace meta
{
    namespace detail
    {
        template <typename T, typename A = std::allocator<T>>
        using std_list = std::list<T, A>;
        template <typename T, typename A = std::allocator<T>>
        using std_forward_list = std::forward_list<T, A>;
        template <typename T, typename A = std::allocator<T>>
        using std_vector = std::vector<T, A>;
        template <typename T, typename A = std::allocator<T>>
        using std_deque = std::deque<T, A>;
        template <typename T, typename C = std::char_traits<T>, typename A = std::allocator<T>>
        using std_basic_string = std::basic_string<T, C, A>;
        template <typename K, typename V, typename C = std::less<K>,
                    typename A = std::allocator<std::pair<K const, V>>>
        using std_map = std::map<K, V, C, A>;
        template <typename K, typename V, typename C = std::less<K>,
                    typename A = std::allocator<std::pair<K const, V>>>
        using std_multimap = std::multimap<K, V, C, A>;
        template <typename K, typename C = std::less<K>, typename A = std::allocator<K>>
        using std_set = std::set<K, C, A>;
        template <typename K, typename C = std::less<K>, typename A = std::allocator<K>>
        using std_multiset = std::multiset<K, C, A>;
        template <typename K, typename V, typename H = std::hash<K>,
                    typename C = std::equal_to<K>,
                    typename A = std::allocator<std::pair<K const, V>>>
        using std_unordered_map = std::unordered_map<K, V, H, C, A>;
        template <typename K, typename V, typename H = std::hash<K>,
                    typename C = std::equal_to<K>,
                    typename A = std::allocator<std::pair<K const, V>>>
        using std_unordered_multimap = std::unordered_multimap<K, V, H, C, A>;
        template <typename K, typename H = std::hash<K>, typename C = std::equal_to<K>,
                    typename A = std::allocator<K>>
        using std_unordered_set = std::unordered_set<K, H, C, A>;
        template <typename K, typename H = std::hash<K>, typename C = std::equal_to<K>,
                    typename A = std::allocator<K>>
        using std_unordered_multiset = std::unordered_multiset<K, H, C, A>;
        template <typename T, typename C = std_deque<T>>
        using std_queue = std::queue<T, C>;
        template <typename T, typename C = std_vector<T>,
                    class D = std::less<typename C::value_type>>
        using std_priority_queue = std::priority_queue<T, C, D>;
        template <typename T, typename C = std_deque<T>>
        using std_stack = std::stack<T, C>;
    } // namespace detail

    template <>
    struct quote<::std::list> : quote<detail::std_list>
    {
    };
    template <>
    struct quote<::std::deque> : quote<detail::std_deque>
    {
    };
    template <>
    struct quote<::std::forward_list> : quote<detail::std_forward_list>
    {
    };
    template <>
    struct quote<::std::vector> : quote<detail::std_vector>
    {
    };
    template <>
    struct quote<::std::basic_string> : quote<detail::std_basic_string>
    {
    };
    template <>
    struct quote<::std::map> : quote<detail::std_map>
    {
    };
    template <>
    struct quote<::std::multimap> : quote<detail::std_multimap>
    {
    };
    template <>
    struct quote<::std::set> : quote<detail::std_set>
    {
    };
    template <>
    struct quote<::std::multiset> : quote<detail::std_multiset>
    {
    };
    template <>
    struct quote<::std::unordered_map> : quote<detail::std_unordered_map>
    {
    };
    template <>
    struct quote<::std::unordered_multimap> : quote<detail::std_unordered_multimap>
    {
    };
    template <>
    struct quote<::std::unordered_set> : quote<detail::std_unordered_set>
    {
    };
    template <>
    struct quote<::std::unordered_multiset> : quote<detail::std_unordered_multiset>
    {
    };
    template <>
    struct quote<::std::queue> : quote<detail::std_queue>
    {
    };
    template <>
    struct quote<::std::priority_queue> : quote<detail::std_priority_queue>
    {
    };
    template <>
    struct quote<::std::stack> : quote<detail::std_stack>
    {
    };
} // namespace meta

#endif
#endif
/// \endcond

#ifdef __clang__
#pragma GCC diagnostic pop
#endif
#endif
