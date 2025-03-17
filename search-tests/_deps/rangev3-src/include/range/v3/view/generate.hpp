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

#ifndef RANGES_V3_VIEW_GENERATE_HPP
#define RANGES_V3_VIEW_GENERATE_HPP

#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/unreachable_sentinel.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/optional.hpp>
#include <range/v3/utility/semiregular_box.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/facade.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{
    template<typename G>
    struct generate_view : view_facade<generate_view<G>, infinite>
    {
    private:
        friend range_access;
        using result_t = invoke_result_t<G &>;
        semiregular_box_t<G> gen_;
        detail::non_propagating_cache<result_t> val_;
        struct cursor
        {
        private:
            generate_view * view_;

        public:
            cursor() = default;
            explicit cursor(generate_view * view)
              : view_(view)
            {}
            result_t && read() const
            {
                if(!view_->val_)
                    view_->val_.emplace(view_->gen_());
                return static_cast<result_t &&>(static_cast<result_t &>(*view_->val_));
            }
            void next()
            {
                if(view_->val_)
                    view_->val_.reset();
                else
                    static_cast<void>(view_->gen_());
            }
        };
        cursor begin_cursor()
        {
            return cursor{this};
        }
        unreachable_sentinel_t end_cursor() const
        {
            return {};
        }

    public:
        generate_view() = default;
        explicit generate_view(G g)
          : gen_(std::move(g))
        {}
        result_t & cached()
        {
            return *val_;
        }
    };

    namespace views
    {
        struct generate_fn
        {
            template(typename G)(
                requires invocable<G &> AND copy_constructible<G> AND
                    std::is_object<detail::decay_t<invoke_result_t<G &>>>::value AND
                    constructible_from<detail::decay_t<invoke_result_t<G &>>,
                                       invoke_result_t<G &>> AND
                    assignable_from<detail::decay_t<invoke_result_t<G &>> &,
                                    invoke_result_t<G &>>)
            generate_view<G> operator()(G g) const
            {
                return generate_view<G>{std::move(g)};
            }
        };

        /// \relates generate_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(generate_fn, generate)
    } // namespace views
    /// \@}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>
#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::generate_view)

#endif
