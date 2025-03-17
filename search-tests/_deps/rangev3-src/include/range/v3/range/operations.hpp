/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//  Copyright Gonzalo Brito Gadeschi 2017
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_RANGE_OPERATIONS_HPP
#define RANGES_V3_RANGE_OPERATIONS_HPP

#include <stdexcept>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/operations.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// Checked indexed range access.
    ///
    /// \ingroup group-range
    struct at_fn
    {
        /// \return `begin(rng)[n]`
        template(typename Rng)(
            requires random_access_range<Rng> AND sized_range<Rng> AND
                borrowed_range<Rng>)
        constexpr range_reference_t<Rng> //
        operator()(Rng && rng, range_difference_t<Rng> n) const
        {
            // Workaround https://gcc.gnu.org/bugzilla/show_bug.cgi?id=67371 in GCC 5
            check_throw(rng, n);
            return ranges::begin(rng)[n];
        }

    private:
        template<typename Rng>
        static constexpr void check_throw(Rng && rng, range_difference_t<Rng> n)
        {
            (n < 0 || n >= ranges::distance(rng)) ? throw std::out_of_range("ranges::at")
                                                  : void(0);
        }
    };

    /// Checked indexed range access.
    ///
    /// \ingroup group-range
    /// \sa `at_fn`
    RANGES_INLINE_VARIABLE(at_fn, at)

    /// Unchecked indexed range access.
    ///
    /// \ingroup group-range
    struct index_fn
    {
        /// \return `begin(rng)[n]`
        template(typename Rng, typename Int)(
            requires random_access_range<Rng> AND integral<Int> AND borrowed_range<Rng>)
        constexpr range_reference_t<Rng> operator()(Rng && rng, Int n) const //
        {
            using D = range_difference_t<Rng>;
            RANGES_EXPECT(0 <= static_cast<D>(n));
            RANGES_EXPECT(!(bool)sized_range<Rng> ||
                          static_cast<D>(n) < ranges::distance(rng));
            return ranges::begin(rng)[static_cast<D>(n)];
        }
    };

    /// Unchecked indexed range access.
    ///
    /// \ingroup group-range
    /// \sa `index_fn`
    RANGES_INLINE_VARIABLE(index_fn, index)

    /// \ingroup group-range
    struct back_fn
    {
        /// \return `*prev(end(rng))`
        template(typename Rng)(
            requires common_range<Rng> AND bidirectional_range<Rng> AND
                borrowed_range<Rng>)
        constexpr range_reference_t<Rng> operator()(Rng && rng) const
        {
            return *prev(end(rng));
        }
    };

    /// \ingroup group-range
    /// \sa `back_fn`
    RANGES_INLINE_VARIABLE(back_fn, back)

    /// \ingroup group-range
    struct front_fn
    {
        /// \return `*begin(rng)`
        template(typename Rng)(
            requires forward_range<Rng> AND borrowed_range<Rng>)
        constexpr range_reference_t<Rng> operator()(Rng && rng) const
        {
            return *begin(rng);
        }
    };

    /// \ingroup group-range
    /// \sa `front_fn`
    RANGES_INLINE_VARIABLE(front_fn, front)
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
