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
#ifndef RANGES_V3_ALGORITHM_SHUFFLE_HPP
#define RANGES_V3_ALGORITHM_SHUFFLE_HPP

#include <cstdint>
#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/dangling.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/random.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/utility/swap.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-algorithms
    /// @{
    RANGES_FUNC_BEGIN(shuffle)

        /// \brief function template \c shuffle
        template(typename I, typename S, typename Gen = detail::default_random_engine &)(
            requires random_access_iterator<I> AND sentinel_for<S, I> AND
                permutable<I> AND
                uniform_random_bit_generator<std::remove_reference_t<Gen>> AND
                convertible_to<invoke_result_t<Gen &>, iter_difference_t<I>>)
        I RANGES_FUNC(shuffle)(I const first,
                               S const last,
                               Gen && gen = detail::get_random_engine()) //
        {
            auto mid = first;
            if(mid == last)
                return mid;
            using D1 = iter_difference_t<I>;
            using D2 =
                meta::conditional_t<std::is_integral<D1>::value, D1, std::ptrdiff_t>;
            std::uniform_int_distribution<D2> uid{};
            using param_t = typename decltype(uid)::param_type;
            while(++mid != last)
            {
                RANGES_ENSURE(mid - first <= PTRDIFF_MAX);
                if(auto const i = uid(gen, param_t{0, D2(mid - first)}))
                    ranges::iter_swap(mid - i, mid);
            }
            return mid;
        }

        /// \overload
        template(typename Rng, typename Gen = detail::default_random_engine &)(
            requires random_access_range<Rng> AND permutable<iterator_t<Rng>> AND
                uniform_random_bit_generator<std::remove_reference_t<Gen>> AND
                convertible_to<invoke_result_t<Gen &>,
                               iter_difference_t<iterator_t<Rng>>>)
        borrowed_iterator_t<Rng> //
        RANGES_FUNC(shuffle)(Rng && rng, Gen && rand = detail::get_random_engine()) //
        {
            return (*this)(begin(rng), end(rng), static_cast<Gen &&>(rand));
        }

    RANGES_FUNC_END(shuffle)

    namespace cpp20
    {
        using ranges::shuffle;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
