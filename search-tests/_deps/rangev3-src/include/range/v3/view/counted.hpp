/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
#ifndef RANGES_V3_VIEW_COUNTED_HPP
#define RANGES_V3_VIEW_COUNTED_HPP

#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/counted_iterator.hpp>
#include <range/v3/iterator/default_sentinel.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/interface.hpp>
#include <range/v3/view/subrange.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{
    template<typename I>
    struct counted_view : view_interface<counted_view<I>, finite>
    {
    private:
        friend range_access;
        I it_;
        iter_difference_t<I> n_;

    public:
        counted_view() = default;
        counted_view(I it, iter_difference_t<I> n)
          : it_(it)
          , n_(n)
        {
            RANGES_EXPECT(0 <= n_);
        }
        counted_iterator<I> begin() const
        {
            return make_counted_iterator(it_, n_);
        }
        default_sentinel_t end() const
        {
            return {};
        }
        auto size() const
        {
            return static_cast<detail::iter_size_t<I>>(n_);
        }
    };

    template<typename I>
    RANGES_INLINE_VAR constexpr bool enable_borrowed_range<counted_view<I>> = true;

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template<typename I>
    counted_view(I, iter_difference_t<I>)
        -> counted_view<I>;
#endif

    namespace views
    {
        struct cpp20_counted_fn
        {
            template(typename I)(
                requires input_or_output_iterator<I> AND (!random_access_iterator<I>)) //
            subrange<counted_iterator<I>, default_sentinel_t> //
            operator()(I it, iter_difference_t<I> n) const
            {
                return {make_counted_iterator(std::move(it), n), default_sentinel};
            }
            template(typename I)(
                requires random_access_iterator<I>)
            subrange<I> operator()(I it, iter_difference_t<I> n) const
            {
                return {it, it + n};
            }
        };

        struct counted_fn
        {
            template(typename I)(
                requires input_or_output_iterator<I> AND (!random_access_iterator<I>)) //
            counted_view<I> operator()(I it, iter_difference_t<I> n) const
            {
                return {std::move(it), n};
            }
            template(typename I)(
                requires random_access_iterator<I>)
            subrange<I> operator()(I it, iter_difference_t<I> n) const
            {
                return {it, it + n};
            }
        };

        /// \relates counted_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(counted_fn, counted)
    } // namespace views

    namespace cpp20
    {
        namespace views
        {
            RANGES_INLINE_VARIABLE(ranges::views::cpp20_counted_fn, counted)
        }
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>
#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::counted_view)

#endif
