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
#ifndef RANGES_V3_ALGORITHM_GENERATE_HPP
#define RANGES_V3_ALGORITHM_GENERATE_HPP

#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/result_types.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/functional/reference_wrapper.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/dangling.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-algorithms
    /// @{
    template<typename O, typename F>
    using generate_result = detail::out_fun_result<O, F>;

    RANGES_FUNC_BEGIN(generate)

        /// \brief function template \c generate_n
        template(typename O, typename S, typename F)(
            requires invocable<F &> AND output_iterator<O, invoke_result_t<F &>> AND
            sentinel_for<S, O>)
        constexpr generate_result<O, F> RANGES_FUNC(generate)(O first, S last, F fun) //
        {
            for(; first != last; ++first)
                *first = invoke(fun);
            return {detail::move(first), detail::move(fun)};
        }

        /// \overload
        template(typename Rng, typename F)(
            requires invocable<F &> AND output_range<Rng, invoke_result_t<F &>>)
        constexpr generate_result<borrowed_iterator_t<Rng>, F> //
        RANGES_FUNC(generate)(Rng && rng, F fun)
        {
            return {(*this)(begin(rng), end(rng), ref(fun)).out, detail::move(fun)};
        }

    RANGES_FUNC_END(generate)

    namespace cpp20
    {
        using ranges::generate;
        using ranges::generate_result;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
