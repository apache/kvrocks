/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2013-present
//  Copyright Casey Carter 2016
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_ITERATOR_ACCESS_HPP
#define RANGES_V3_ITERATOR_ACCESS_HPP

#include <iterator>
#include <type_traits>
#include <utility>

#include <std/detail/associated_types.hpp>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/utility/move.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/utility/swap.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-iterator
    /// @{

    /// \cond
    namespace detail
    {
        template<typename I,
#ifdef RANGES_WORKAROUND_MSVC_683388
                 typename R = meta::conditional_t<
                     std::is_pointer<uncvref_t<I>>::value &&
                         std::is_array<std::remove_pointer_t<uncvref_t<I>>>::value,
                     std::add_lvalue_reference_t<std::remove_pointer_t<uncvref_t<I>>>,
                     decltype(*std::declval<I &>())>,
#else
                 typename R = decltype(*std::declval<I &>()),
#endif
                 typename = R &>
        using iter_reference_t_ = R;

#if defined(RANGES_DEEP_STL_INTEGRATION) && RANGES_DEEP_STL_INTEGRATION && \
    !defined(RANGES_DOXYGEN_INVOKED)
        template<typename T>
        using iter_value_t_ =
            typename meta::conditional_t<
                is_std_iterator_traits_specialized_v<T>,
                std::iterator_traits<T>,
                indirectly_readable_traits<T>>::value_type;
#else
        template<typename T>
        using iter_value_t_ = typename indirectly_readable_traits<T>::value_type;
#endif
    } // namespace detail
    /// \endcond

    template<typename R>
    using iter_reference_t = detail::iter_reference_t_<R>;

    template<typename R>
    using iter_value_t = detail::iter_value_t_<uncvref_t<R>>;

    /// \cond
    namespace _iter_move_
    {
#if RANGES_BROKEN_CPO_LOOKUP
        void iter_move(); // unqualified name lookup block
#endif

        template<typename T>
        decltype(iter_move(std::declval<T>())) try_adl_iter_move_(int);

        template<typename T>
        void try_adl_iter_move_(long);

        template<typename T>
        RANGES_INLINE_VAR constexpr bool is_adl_indirectly_movable_v =
            !RANGES_IS_SAME(void, decltype(_iter_move_::try_adl_iter_move_<T>(42)));

        struct fn
        {
            // clang-format off
            template<typename I,
                     typename = detail::enable_if_t<is_adl_indirectly_movable_v<I &>>>
#ifndef RANGES_WORKAROUND_CLANG_23135
            constexpr
#endif // RANGES_WORKAROUND_CLANG_23135
            auto CPP_auto_fun(operator())(I &&i)(const)
            (
                return iter_move(i)
            )

            template<
                typename I,
                typename = detail::enable_if_t<!is_adl_indirectly_movable_v<I &>>,
                typename R = iter_reference_t<I>>
#ifndef RANGES_WORKAROUND_CLANG_23135
            constexpr
#endif // RANGES_WORKAROUND_CLANG_23135
            auto CPP_auto_fun(operator())(I &&i)(const)
            (
                return static_cast<aux::move_t<R>>(aux::move(*i))
            )
            // clang-format on
        };
    } // namespace _iter_move_
    /// \endcond

    RANGES_DEFINE_CPO(_iter_move_::fn, iter_move)

    /// \cond
    namespace detail
    {
        template<typename I, typename O>
        auto is_indirectly_movable_(I & (*i)(), O & (*o)(), iter_value_t<I> * v = nullptr)
            -> always_<std::true_type,
                       decltype(iter_value_t<I>(iter_move(i()))),
                       decltype(*v = iter_move(i())),
                       decltype(*o() = (iter_value_t<I> &&) * v),
                       decltype(*o() = iter_move(i()))>;
        template<typename I, typename O>
        auto is_indirectly_movable_(...) -> std::false_type;

        template<typename I, typename O>
        auto is_nothrow_indirectly_movable_(iter_value_t<I> * v) -> meta::bool_<
            noexcept(iter_value_t<I>(iter_move(std::declval<I &>()))) &&
            noexcept(*v = iter_move(std::declval<I &>())) &&
            noexcept(*std::declval<O &>() = (iter_value_t<I> &&) * v) &&
            noexcept(*std::declval<O &>() = iter_move(std::declval<I &>()))>;
        template<typename I, typename O>
        auto is_nothrow_indirectly_movable_(...) -> std::false_type;
    } // namespace detail
    /// \endcond

    template<typename I, typename O>
    RANGES_INLINE_VAR constexpr bool is_indirectly_movable_v =
        decltype(detail::is_indirectly_movable_<I, O>(nullptr, nullptr))::value;

    template<typename I, typename O>
    RANGES_INLINE_VAR constexpr bool is_nothrow_indirectly_movable_v =
        decltype(detail::is_nothrow_indirectly_movable_<I, O>(nullptr))::value;

    template<typename I, typename O>
    struct is_indirectly_movable : meta::bool_<is_indirectly_movable_v<I, O>>
    {};

    template<typename I, typename O>
    struct is_nothrow_indirectly_movable
      : meta::bool_<is_nothrow_indirectly_movable_v<I, O>>
    {};

    /// \cond
    namespace _iter_swap_
    {
        struct nope
        {};

        // Q: Should std::reference_wrapper be considered a proxy wrt swapping rvalues?
        // A: No. Its operator= is currently defined to reseat the references, so
        //    std::swap(ra, rb) already means something when ra and rb are (lvalue)
        //    reference_wrappers. That reseats the reference wrappers but leaves the
        //    referents unmodified. Treating rvalue reference_wrappers differently would
        //    be confusing.

        // Q: Then why is it OK to "re"-define swap for pairs and tuples of references?
        // A: Because as defined above, swapping an rvalue tuple of references has the
        //    same semantics as swapping an lvalue tuple of references. Rather than
        //    reseat the references, assignment happens *through* the references.

        // Q: But I have an iterator whose operator* returns an rvalue
        //    std::reference_wrapper<T>. How do I make it model indirectly_swappable?
        // A: With an overload of iter_swap.

        // Intentionally create an ambiguity with std::iter_swap, which is
        // unconstrained.
        template<typename T, typename U>
        nope iter_swap(T, U) = delete;

#ifdef RANGES_WORKAROUND_MSVC_895622
        nope iter_swap();
#endif

        template<typename T, typename U>
        decltype(iter_swap(std::declval<T>(), std::declval<U>())) try_adl_iter_swap_(int);

        template<typename T, typename U>
        nope try_adl_iter_swap_(long);

        // Test whether an overload of iter_swap for a T and a U can be found
        // via ADL with the iter_swap overload above participating in the
        // overload set. This depends on user-defined iter_swap overloads
        // being a better match than the overload in namespace std.
        template<typename T, typename U>
        RANGES_INLINE_VAR constexpr bool is_adl_indirectly_swappable_v =
            !RANGES_IS_SAME(nope, decltype(_iter_swap_::try_adl_iter_swap_<T, U>(42)));

        struct fn
        {
            // *If* a user-defined iter_swap is found via ADL, call that:
            template<typename T, typename U>
            constexpr detail::enable_if_t<is_adl_indirectly_swappable_v<T, U>> operator()(
                T && t, U && u) const noexcept(noexcept(iter_swap((T &&) t, (U &&) u)))
            {
                (void)iter_swap((T &&) t, (U &&) u);
            }

            // *Otherwise*, for readable types with swappable reference
            // types, call ranges::swap(*a, *b)
            template<typename I0, typename I1>
            constexpr detail::enable_if_t<
                !is_adl_indirectly_swappable_v<I0, I1> &&
                is_swappable_with<iter_reference_t<I0>, iter_reference_t<I1>>::value>
            operator()(I0 && a, I1 && b) const noexcept(noexcept(ranges::swap(*a, *b)))
            {
                ranges::swap(*a, *b);
            }

            // *Otherwise*, for readable types that are mutually
            // indirectly_movable_storable, implement as:
            //      iter_value_t<T0> tmp = iter_move(a);
            //      *a = iter_move(b);
            //      *b = std::move(tmp);
            template<typename I0, typename I1>
            constexpr detail::enable_if_t<
                !is_adl_indirectly_swappable_v<I0, I1> &&
                !is_swappable_with<iter_reference_t<I0>, iter_reference_t<I1>>::value &&
                is_indirectly_movable_v<I0, I1> && is_indirectly_movable_v<I1, I0>>
            operator()(I0 && a, I1 && b) const
                noexcept(is_nothrow_indirectly_movable_v<I0, I1> &&
                             is_nothrow_indirectly_movable_v<I1, I0>)
            {
                iter_value_t<I0> v0 = iter_move(a);
                *a = iter_move(b);
                *b = detail::move(v0);
            }
        };
    } // namespace _iter_swap_
    /// \endcond

    /// \relates _iter_swap_::fn
    RANGES_DEFINE_CPO(_iter_swap_::fn, iter_swap)

    /// \cond
    namespace detail
    {
        template<typename T, typename U>
        auto is_indirectly_swappable_(T & (*t)(), U & (*u)())
            -> detail::always_<std::true_type, decltype(iter_swap(t(), u()))>;
        template<typename T, typename U>
        auto is_indirectly_swappable_(...) -> std::false_type;

        template<typename T, typename U>
        auto is_nothrow_indirectly_swappable_(int)
            -> meta::bool_<noexcept(iter_swap(std::declval<T &>(), std::declval<U &>()))>;
        template<typename T, typename U>
        auto is_nothrow_indirectly_swappable_(long) -> std::false_type;
    } // namespace detail
    /// \endcond

    template<typename T, typename U>
    RANGES_INLINE_VAR constexpr bool is_indirectly_swappable_v =
        decltype(detail::is_indirectly_swappable_<T, U>(nullptr, nullptr))::value;

    template<typename T, typename U>
    RANGES_INLINE_VAR constexpr bool is_nothrow_indirectly_swappable_v =
        decltype(detail::is_nothrow_indirectly_swappable_<T, U>(0))::value;

    template<typename T, typename U>
    struct is_indirectly_swappable : meta::bool_<is_indirectly_swappable_v<T, U>>
    {};

    template<typename T, typename U>
    struct is_nothrow_indirectly_swappable
      : meta::bool_<is_nothrow_indirectly_swappable_v<T, U>>
    {};

    namespace cpp20
    {
        using ranges::iter_move;
        using ranges::iter_reference_t;
        using ranges::iter_swap;
        using ranges::iter_value_t;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif // RANGES_V3_ITERATOR_ACCESS_HPP
