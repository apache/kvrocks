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

#ifndef RANGES_V3_VIEW_GENERATE_N_HPP
#define RANGES_V3_VIEW_GENERATE_N_HPP

#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/default_sentinel.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/semiregular_box.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/facade.hpp>
#include <range/v3/view/generate.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{
    template<typename G>
    struct generate_n_view : view_facade<generate_n_view<G>, finite>
    {
    private:
        friend range_access;
        using result_t = invoke_result_t<G &>;
        semiregular_box_t<G> gen_;
        detail::non_propagating_cache<result_t> val_;
        std::size_t n_;
        struct cursor
        {
        private:
            generate_n_view * rng_;

        public:
            cursor() = default;
            explicit cursor(generate_n_view * rng)
              : rng_(rng)
            {}
            bool equal(default_sentinel_t) const
            {
                return 0 == rng_->n_;
            }
            result_t && read() const
            {
                if(!rng_->val_)
                    rng_->val_.emplace(rng_->gen_());
                return static_cast<result_t &&>(static_cast<result_t &>(*rng_->val_));
            }
            void next()
            {
                RANGES_EXPECT(0 != rng_->n_);
                if(rng_->val_)
                    rng_->val_.reset();
                else
                    static_cast<void>(rng_->gen_());
                --rng_->n_;
            }
        };
        cursor begin_cursor()
        {
            return cursor{this};
        }

    public:
        generate_n_view() = default;
        explicit generate_n_view(G g, std::size_t n)
          : gen_(std::move(g))
          , n_(n)
        {}
        result_t & cached()
        {
            return *val_;
        }
        std::size_t size() const
        {
            return n_;
        }
    };

    namespace views
    {
        struct generate_n_fn
        {
            template(typename G)(
                requires invocable<G &> AND copy_constructible<G> AND
                    std::is_object<detail::decay_t<invoke_result_t<G &>>>::value AND
                    constructible_from<detail::decay_t<invoke_result_t<G &>>,
                                       invoke_result_t<G &>> AND
                    assignable_from<detail::decay_t<invoke_result_t<G &>> &,
                                    invoke_result_t<G &>>)
            generate_n_view<G> operator()(G g, std::size_t n) const
            {
                return generate_n_view<G>{std::move(g), n};
            }
        };

        /// \relates generate_n_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(generate_n_fn, generate_n)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>
#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::generate_n_view)

#endif
