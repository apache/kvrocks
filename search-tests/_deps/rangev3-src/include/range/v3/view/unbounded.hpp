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

#ifndef RANGES_V3_VIEW_UNBOUNDED_HPP
#define RANGES_V3_VIEW_UNBOUNDED_HPP

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/unreachable_sentinel.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/interface.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{
    template<typename I>
    struct unbounded_view : view_interface<unbounded_view<I>, infinite>
    {
    private:
        I it_;

    public:
        unbounded_view() = default;
        constexpr explicit unbounded_view(I it)
          : it_(detail::move(it))
        {}
        constexpr I begin() const
        {
            return it_;
        }
        constexpr unreachable_sentinel_t end() const
        {
            return {};
        }
    };

    template<typename I>
    RANGES_INLINE_VAR constexpr bool enable_borrowed_range<unbounded_view<I>> = true;

    namespace views
    {
        struct unbounded_fn
        {
            template(typename I)(
                requires input_iterator<I>)
            constexpr unbounded_view<I> operator()(I it) const
            {
                return unbounded_view<I>{detail::move(it)};
            }
        };

        /// \relates unbounded_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(unbounded_fn, unbounded)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>
#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::unbounded_view)

#endif
