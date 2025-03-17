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

#ifndef RANGES_V3_VIEW_REF_HPP
#define RANGES_V3_VIEW_REF_HPP

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/range/access.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/addressof.hpp>
#include <range/v3/view/interface.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    template<typename Rng>
    struct ref_view;

    template<typename Rng>
    RANGES_INLINE_VAR constexpr bool enable_borrowed_range<ref_view<Rng>> = true;

    /// \addtogroup group-views
    /// @{
    template<typename Rng>
    struct ref_view : view_interface<ref_view<Rng>, range_cardinality<Rng>::value>
    {
    private:
        CPP_assert(range<Rng>);
        static_assert(std::is_object<Rng>::value, "");
        Rng * rng_ = nullptr; // exposition only
    public:
        constexpr ref_view() noexcept = default;
        constexpr ref_view(Rng & rng) noexcept
          : rng_(detail::addressof(rng))
        {}
        constexpr Rng & base() const noexcept
        {
            return *rng_;
        }
        constexpr iterator_t<Rng> begin() const noexcept(noexcept(ranges::begin(*rng_)))
        {
            return ranges::begin(*rng_);
        }
        constexpr sentinel_t<Rng> end() const noexcept(noexcept(ranges::end(*rng_)))
        {
            return ranges::end(*rng_);
        }
        CPP_member
        constexpr auto empty() const noexcept(noexcept(ranges::empty(*rng_)))
            -> CPP_ret(bool)(
                requires detail::can_empty_<Rng>)
        {
            return ranges::empty(*rng_);
        }
        CPP_auto_member
        constexpr auto CPP_fun(size)()(const //
            noexcept(noexcept(ranges::size(*rng_))) //
            requires sized_range<Rng>)
        {
            return ranges::size(*rng_);
        }
        CPP_auto_member
        constexpr auto CPP_fun(data)()(const //
            noexcept(noexcept(ranges::data(*rng_))) //
            requires contiguous_range<Rng>)
        {
            return ranges::data(*rng_);
        }
    };

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template(typename R)(
        requires range<R>)
    ref_view(R &) //
        -> ref_view<R>;
#endif

    namespace views
    {
        struct ref_fn
        {
            template(typename Rng)(
                requires range<Rng>)
            constexpr ref_view<Rng> operator()(Rng & rng) const noexcept
            {
                return ref_view<Rng>(rng);
            }
            template<typename Rng>
            void operator()(Rng const && rng) const = delete;
        };

        /// \relates const_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(ref_fn, ref)
    } // namespace views

    namespace cpp20
    {
        template(typename Rng)(
            requires std::is_object<Rng>::value) //
            using ref_view = ranges::ref_view<Rng>;
    }
} // namespace ranges

#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::ref_view)

#include <range/v3/detail/epilogue.hpp>

#endif
