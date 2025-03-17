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
#ifndef RANGES_V3_ALGORITHM_COPY_HPP
#define RANGES_V3_ALGORITHM_COPY_HPP

#include <functional>
#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/result_types.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/dangling.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/copy.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-algorithms
    /// @{
    template<typename I, typename O>
    using copy_result = detail::in_out_result<I, O>;

    RANGES_HIDDEN_DETAIL(namespace _copy CPP_PP_LBRACE())
    RANGES_FUNC_BEGIN(copy)

        /// \brief function template \c copy
        template(typename I, typename S, typename O)(
            requires input_iterator<I> AND sentinel_for<S, I> AND
            weakly_incrementable<O> AND indirectly_copyable<I, O>)
        constexpr copy_result<I, O> RANGES_FUNC(copy)(I first, S last, O out) //
        {
            for(; first != last; ++first, ++out)
                *out = *first;
            return {first, out};
        }

        /// \overload
        template(typename Rng, typename O)(
            requires input_range<Rng> AND weakly_incrementable<O> AND
            indirectly_copyable<iterator_t<Rng>, O>)
        constexpr copy_result<borrowed_iterator_t<Rng>, O> //
        RANGES_FUNC(copy)(Rng && rng, O out)  //
        {
            return (*this)(begin(rng), end(rng), std::move(out));
        }

    RANGES_FUNC_END(copy)
    RANGES_HIDDEN_DETAIL(CPP_PP_RBRACE())

#ifndef RANGES_DOXYGEN_INVOKED
    struct copy_fn
      : aux::copy_fn
      , _copy::copy_fn
    {
        using aux::copy_fn::operator();
        using _copy::copy_fn::operator();
    };
    RANGES_INLINE_VARIABLE(copy_fn, copy)
#endif

    namespace cpp20
    {
        using ranges::copy_result;
        using ranges::RANGES_HIDDEN_DETAIL(_copy::) copy;
    } // namespace cpp20

    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
