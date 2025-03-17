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
#ifndef RANGES_V3_VIEW_ALL_HPP
#define RANGES_V3_VIEW_ALL_HPP

#include <type_traits>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/ref.hpp>
#include <range/v3/view/subrange.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{
    namespace views
    {
        struct all_fn
        {
        private:
            /// If it's a view already, pass it though.
            template<typename T>
            static constexpr auto from_range_(T && t, std::true_type, detail::ignore_t,
                                              detail::ignore_t)
            {
                return static_cast<T &&>(t);
            }

            /// If it is container-like, turn it into a view, being careful
            /// to preserve the Sized-ness of the range.
            template<typename T>
            static constexpr auto from_range_(T && t, std::false_type, std::true_type,
                                              detail::ignore_t)
            {
                return ranges::views::ref(t);
            }

            /// Not a view and not an lvalue? If it's a borrowed_range, then
            /// return a subrange holding the range's begin/end.
            template<typename T>
            static constexpr auto from_range_(T && t, std::false_type, std::false_type,
                                              std::true_type)
            {
                return make_subrange(static_cast<T &&>(t));
            }

        public:
            template(typename T)(
                requires range<T &> AND viewable_range<T>)
            constexpr auto operator()(T && t) const
            {
                return all_fn::from_range_(static_cast<T &&>(t),
                                           meta::bool_<view_<uncvref_t<T>>>{},
                                           std::is_lvalue_reference<T>{},
                                           meta::bool_<borrowed_range<T>>{});
            }

            template<typename T>
            RANGES_DEPRECATED("Passing a reference_wrapper to views::all is deprecated.")
            constexpr ref_view<T> operator()(std::reference_wrapper<T> r) const
            {
                return ranges::views::ref(r.get());
            }
        };

        /// \relates all_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(view_closure<all_fn>, all)

        template<typename Rng>
        using all_t = decltype(all(std::declval<Rng>()));
    } // namespace views

    template<typename Rng>
    struct identity_adaptor : Rng
    {
        CPP_assert(view_<Rng>);

        identity_adaptor() = default;
        constexpr explicit identity_adaptor(Rng const & rng)
          : Rng(rng)
        {}
        constexpr explicit identity_adaptor(Rng && rng)
          : Rng(detail::move(rng))
        {}
    };

    namespace cpp20
    {
        namespace views
        {
            using ranges::views::all;
            using ranges::views::all_t;
        }
        template(typename Rng)(
            requires viewable_range<Rng>)
        using all_view RANGES_DEPRECATED(
            "Please use ranges::cpp20::views::all_t instead.") =
                ranges::views::all_t<Rng>;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
