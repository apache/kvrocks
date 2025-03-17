/// \file
// Range v3 library
//
//  Copyright Casey Carter 2018-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_VIEW_ENUMERATE_HPP
#define RANGES_V3_VIEW_ENUMERATE_HPP

#include <range/v3/core.hpp>
#include <range/v3/iterator/unreachable_sentinel.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/facade.hpp>
#include <range/v3/view/zip.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
        // Counts from zero up.
        // See https://github.com/ericniebler/range-v3/issues/1141
        // for why we don't just use iota_view.
        template<typename Size, typename Diff>
        struct index_view : view_facade<index_view<Size, Diff>, infinite>
        {
        private:
            friend range_access;

            struct cursor
            {
                using difference_type = Diff;

            private:
                friend range_access;
                Size index_{0};

                Size read() const
                {
                    return index_;
                }
                void next()
                {
                    ++index_;
                }
                bool equal(cursor const & that) const
                {
                    return that.index_ == index_;
                }
                void prev()
                {
                    --index_;
                }
                void advance(Diff n)
                {
                    index_ += static_cast<Size>(n);
                }
                Diff distance_to(cursor const & that) const
                {
                    return static_cast<Diff>(static_cast<Diff>(that.index_) -
                                             static_cast<Diff>(index_));
                }

            public:
                cursor() = default;
            };
            cursor begin_cursor() const
            {
                return cursor{};
            }
            unreachable_sentinel_t end_cursor() const
            {
                return unreachable;
            }

        public:
            index_view() = default;
        };

    } // namespace detail

    template<typename Size, typename Diff>
    RANGES_INLINE_VAR constexpr bool enable_borrowed_range<detail::index_view<Size, Diff>> =
        true;

    /// \endcond
    /// \addtogroup group-views
    /// @{
    namespace views
    {
        /// Lazily pairs each element in a source range with
        /// its corresponding index.
        struct enumerate_fn
        {
            template(typename Rng)(
                requires viewable_range<Rng>)
            auto operator()(Rng && rng) const
            {
                using D = range_difference_t<Rng>;
                using S = detail::iter_size_t<iterator_t<Rng>>;
                return zip(detail::index_view<S, D>(), all(static_cast<Rng &&>(rng)));
            }
        };

        /// \relates enumerate_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(view_closure<enumerate_fn>, enumerate)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
