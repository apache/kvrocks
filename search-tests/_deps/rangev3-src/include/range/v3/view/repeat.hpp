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

#ifndef RANGES_V3_VIEW_REPEAT_HPP
#define RANGES_V3_VIEW_REPEAT_HPP

#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/unreachable_sentinel.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/utility/semiregular_box.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/facade.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{

    // Ordinarily, a view shouldn't contain its elements. This is so that copying
    // and assigning ranges is O(1), and also so that in the event of element
    // mutation, all the copies of the range see the mutation the same way. The
    // repeat_view *does* own its lone element, though. This is OK because:
    //  - O(N) copying is fine when N==1 as it is in this case, and
    //  - The element is immutable, so there is no potential for incorrect
    //    semantics.
    template<typename Val>
    struct repeat_view : view_facade<repeat_view<Val>, infinite>
    {
    private:
        semiregular_box_t<Val> value_;
        friend range_access;

        struct cursor
        {
        private:
            Val const * value_;
            std::ptrdiff_t n_ = 0;

        public:
            cursor() = default;
            explicit cursor(Val const & value)
              : value_(std::addressof(value))
            {}
            Val const & read() const noexcept
            {
                return *value_;
            }
            bool equal(cursor const & that) const
            {
                return n_ == that.n_;
            }
            void next()
            {
                ++n_;
            }
            void prev()
            {
                --n_;
            }
            void advance(std::ptrdiff_t d)
            {
                n_ += d;
            }
            std::ptrdiff_t distance_to(cursor const & that) const
            {
                return that.n_ - n_;
            }
        };
        cursor begin_cursor() const
        {
            return cursor{value_};
        }
        unreachable_sentinel_t end_cursor() const
        {
            return unreachable;
        }

    public:
        repeat_view() = default;
        constexpr explicit repeat_view(Val value)
          : value_(detail::move(value))
        {}
    };

    namespace views
    {
        struct repeat_fn
        {
            template(typename Val)(
                requires copy_constructible<Val>)
            repeat_view<Val> operator()(Val value) const
            {
                return repeat_view<Val>{std::move(value)};
            }
        };

        /// \relates repeat_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(repeat_fn, repeat)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>
#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::repeat_view)

#endif
