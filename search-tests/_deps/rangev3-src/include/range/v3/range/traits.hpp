/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2013-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_RANGE_TRAITS_HPP
#define RANGES_V3_RANGE_TRAITS_HPP

#include <array>
#include <iterator>
#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/range/access.hpp>
#include <range/v3/range/primitives.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
        template<typename I, typename S>
        using common_iterator_impl_t =
            enable_if_t<(bool)(input_or_output_iterator<I> && sentinel_for<S, I>),
                        common_iterator<I, S>>;
    }
    /// \endcond

    /// \addtogroup group-range
    /// @{
    template<typename I, typename S>
    using common_iterator_t = meta::conditional_t<std::is_same<I, S>::value, I,
                                                detail::common_iterator_impl_t<I, S>>;

    /// \cond
    namespace detail
    {
        template<typename I, typename S>
        using cpp17_iterator_t =
            meta::conditional_t<std::is_integral<iter_difference_t<I>>::value,
                      common_iterator_t<I, S>, cpp17_iterator<common_iterator_t<I, S>>>;
    }
    /// \endcond

    // Aliases (SFINAE-able)
    template<typename Rng>
    using range_difference_t = iter_difference_t<iterator_t<Rng>>;

    template<typename Rng>
    using range_value_t = iter_value_t<iterator_t<Rng>>;

    template<typename Rng>
    using range_reference_t = iter_reference_t<iterator_t<Rng>>;

    template<typename Rng>
    using range_rvalue_reference_t = iter_rvalue_reference_t<iterator_t<Rng>>;

    template<typename Rng>
    using range_common_reference_t = iter_common_reference_t<iterator_t<Rng>>;

    template<typename Rng>
    using range_size_t = decltype(ranges::size(std::declval<Rng &>()));

    /// \cond
    template<typename Rng>
    using range_difference_type_t RANGES_DEPRECATED(
        "range_difference_type_t is deprecated. Use the range_difference_t instead.") =
        iter_difference_t<iterator_t<Rng>>;

    template<typename Rng>
    using range_value_type_t RANGES_DEPRECATED(
        "range_value_type_t is deprecated. Use the range_value_t instead.") =
        iter_value_t<iterator_t<Rng>>;

    template<typename Rng>
    using range_category_t RANGES_DEPRECATED(
        "range_category_t is deprecated. Use the range concepts instead.") =
        meta::_t<detail::iterator_category<iterator_t<Rng>>>;

    template<typename Rng>
    using range_size_type_t RANGES_DEPRECATED(
        "range_size_type_t is deprecated. Use range_size_t instead.") =
        detail::iter_size_t<iterator_t<Rng>>;
    /// \endcond

    template<typename Rng>
    using range_common_iterator_t = common_iterator_t<iterator_t<Rng>, sentinel_t<Rng>>;

    /// \cond
    namespace detail
    {
        template<typename Rng>
        using range_cpp17_iterator_t = cpp17_iterator_t<iterator_t<Rng>, sentinel_t<Rng>>;

        std::integral_constant<cardinality, finite> test_cardinality(void *);
        template<cardinality Card>
        std::integral_constant<cardinality, Card> test_cardinality(basic_view<Card> *);
        template<typename T, std::size_t N>
        std::integral_constant<cardinality, static_cast<cardinality>(N)> test_cardinality(
            T (*)[N]);
        template<typename T, std::size_t N>
        std::integral_constant<cardinality, static_cast<cardinality>(N)> test_cardinality(
            std::array<T, N> *);
    } // namespace detail
    /// \endcond

    // User customization point for specifying the cardinality of ranges:
    template<typename Rng, typename Void /*= void*/>
    struct range_cardinality
      : meta::conditional_t<RANGES_IS_SAME(Rng, uncvref_t<Rng>),
                          decltype(detail::test_cardinality(
                              static_cast<uncvref_t<Rng> *>(nullptr))),
                          range_cardinality<uncvref_t<Rng>>>
    {};

    /// @}
    namespace cpp20
    {
        using ranges::range_difference_t;
        using ranges::range_reference_t;
        using ranges::range_rvalue_reference_t;
        using ranges::range_value_t;
    } // namespace cpp20
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
