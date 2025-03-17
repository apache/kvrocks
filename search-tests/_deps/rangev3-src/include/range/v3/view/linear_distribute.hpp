/// \file
// Range v3 library
//
//  Copyright Casey Carter 2017
//  Copyright Gonzalo Brito Gadeschi 2017
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_VIEW_LINEAR_DISTRIBUTE_HPP
#define RANGES_V3_VIEW_LINEAR_DISTRIBUTE_HPP

#include <type_traits>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/default_sentinel.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/facade.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    namespace views
    {
        /// \addtogroup group-views
        /// @{

        template<typename T>
        struct linear_distribute_view : view_facade<linear_distribute_view<T>, finite>
        {
            CPP_assert(std::is_arithmetic<T>());

        private:
            friend range_access;
            using Calc = meta::conditional_t<std::is_floating_point<T>::value, T, double>;

            T from_, to_;
            Calc delta_;
            std::ptrdiff_t n_;

            constexpr T read() const noexcept
            {
                return from_;
            }
            constexpr bool equal(default_sentinel_t) const noexcept
            {
                return n_ == 0;
            }
            constexpr bool equal(linear_distribute_view const & other) const noexcept
            {
                bool const eq = n_ == other.n_;
                RANGES_DIAGNOSTIC_PUSH
                RANGES_DIAGNOSTIC_IGNORE_FLOAT_EQUAL
                RANGES_EXPECT(to_ == other.to_);
                RANGES_EXPECT(!eq || from_ == other.from_);
                RANGES_DIAGNOSTIC_POP
                return eq;
            }
            constexpr void next() noexcept
            {
                RANGES_EXPECT(n_ > 0);
                --n_;
                if(n_ == 0)
                {
                    from_ = to_;
                }
                else
                {
                    from_ = T(to_ - (delta_ * Calc(n_ - 1)));
                }
            }

        public:
            constexpr linear_distribute_view() = default;
            constexpr linear_distribute_view(T from, T to, std::ptrdiff_t n) noexcept
              : from_(from)
              , to_(to)
              , delta_(n > 1 ? (to - from) / Calc(n - 1) : 0)
              , n_(n)
            {
                RANGES_EXPECT(n_ > 0);
                RANGES_EXPECT(to_ >= from_);
            }
            constexpr std::size_t size() const noexcept
            {
                return static_cast<std::size_t>(n_);
            }
        };

        /// Distributes `n` values linearly in the closed interval [`from`, `to`].
        ///
        /// \pre `from <= to && n > 0`
        ///
        /// If `from == to`, returns n-times `to`.
        /// If `n == 1` returns `to`.
        struct linear_distribute_fn
        {
            template(typename T)(
                requires std::is_arithmetic<T>::value)
            constexpr auto operator()(T from, T to, std::ptrdiff_t n) const
            {
                return linear_distribute_view<T>{from, to, n};
            }
        };

        /// \relates linear_distribute_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(linear_distribute_fn, linear_distribute)
    } // namespace views
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
