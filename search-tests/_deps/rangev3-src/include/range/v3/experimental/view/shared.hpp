/// \file
// Range v3 library
//
//  Copyright Filip Matzner 2017
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
#ifndef RANGES_V3_EXPERIMENTAL_VIEW_SHARED_HPP
#define RANGES_V3_EXPERIMENTAL_VIEW_SHARED_HPP

#include <memory>
#include <type_traits>

#include <meta/meta.hpp>

#include <range/v3/functional/compose.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/view/all.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{
    namespace experimental
    {
        template<typename Rng>
        struct shared_view
          : view_interface<shared_view<Rng>, range_cardinality<Rng>::value>
        {
        private:
            // shared storage
            std::shared_ptr<Rng> rng_ptr_;

        public:
            shared_view() = default;

            // construct from a range rvalue
            explicit shared_view(Rng rng)
              : rng_ptr_{std::make_shared<Rng>(std::move(rng))}
            {}

            // use the stored range's begin and end
            iterator_t<Rng> begin() const
            {
                return ranges::begin(*rng_ptr_);
            }
            sentinel_t<Rng> end() const
            {
                return ranges::end(*rng_ptr_);
            }

            CPP_auto_member
            auto CPP_fun(size)()(const
                requires sized_range<Rng>)
            {
                return ranges::size(*rng_ptr_);
            }
        };

        template<typename SharedFn>
        struct shared_closure;

        struct RANGES_STRUCT_WITH_ADL_BARRIER(shared_closure_base)
        {
            // Piping requires viewable_ranges.
            template(typename Rng, typename SharedFn)(
                requires range<Rng> AND (!viewable_range<Rng>) AND
                    constructible_from<detail::decay_t<Rng>, Rng>)
            friend constexpr auto operator|(Rng && rng, shared_closure<SharedFn> vw)
            {
                return static_cast<SharedFn &&>(vw)(static_cast<Rng &&>(rng));
            }

            template<typename SharedFn, typename Pipeable>
            friend constexpr auto operator|(shared_closure<SharedFn> sh, Pipeable pipe)
                -> CPP_broken_friend_ret(shared_closure<composed<Pipeable, SharedFn>>)(
                    requires (is_pipeable_v<Pipeable>))
            {
                return shared_closure<composed<Pipeable, SharedFn>>{compose(
                    static_cast<Pipeable &&>(pipe), static_cast<SharedFn &&>(sh))};
            }
        };

        template<typename SharedFn>
        struct shared_closure
          : shared_closure_base
          , SharedFn
        {
            shared_closure() = default;
            constexpr explicit shared_closure(SharedFn fn)
              : SharedFn(static_cast<SharedFn &&>(fn))
            {}
        };

        namespace views
        {
            struct shared_fn
            {
                template(typename Rng)(
                    requires range<Rng> AND (!viewable_range<Rng>)AND
                        constructible_from<detail::decay_t<Rng>, Rng>)
                shared_view<detail::decay_t<Rng>> operator()(Rng && rng) const
                {
                    return shared_view<detail::decay_t<Rng>>{static_cast<Rng &&>(rng)};
                }
            };

            /// \relates shared_fn
            /// \ingroup group-views
            RANGES_INLINE_VARIABLE(shared_closure<shared_fn>, shared)
        } // namespace views
    }     // namespace experimental

    template<typename SharedFn>
    RANGES_INLINE_VAR constexpr bool
        is_pipeable_v<experimental::shared_closure<SharedFn>> = true;
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
