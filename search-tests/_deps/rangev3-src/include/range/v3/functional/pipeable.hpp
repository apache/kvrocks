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
#ifndef RANGES_V3_FUNCTIONAL_PIPEABLE_HPP
#define RANGES_V3_FUNCTIONAL_PIPEABLE_HPP

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/compose.hpp>
#include <range/v3/functional/concepts.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-functional
    /// @{

    struct pipeable_base;

    template<typename T>
    RANGES_INLINE_VAR constexpr bool is_pipeable_v = META_IS_BASE_OF(pipeable_base, T);

    template<typename T>
    RANGES_INLINE_VAR constexpr bool is_pipeable_v<T &> = META_IS_BASE_OF(pipeable_base,
                                                                          T);
    template<typename T>
    RANGES_INLINE_VAR constexpr bool is_pipeable_v<T &&> = META_IS_BASE_OF(pipeable_base,
                                                                           T);
    template<typename T>
    using is_pipeable = meta::bool_<is_pipeable_v<T>>;

    struct make_pipeable_fn
    {
        template<typename Fun, typename PipeableBase = pipeable_base>
        constexpr auto operator()(Fun fun) const
        {
            struct local
              : Fun
              , PipeableBase
            {
                constexpr explicit local(Fun && f)
                  : Fun(static_cast<Fun &&>(f))
                {}
            };
            return local{static_cast<Fun &&>(fun)};
        }
    };

    /// \ingroup group-functional
    /// \sa `make_pipeable_fn`
    RANGES_INLINE_VARIABLE(make_pipeable_fn, make_pipeable)

    struct pipeable_access
    {
        template<typename Pipeable>
        struct impl : Pipeable
        {
            using Pipeable::pipe;
        };
    };

    struct pipeable_base
    {
    private:
        friend pipeable_access;

        // Evaluate the pipe with an argument
        template(typename Arg, typename Pipe)(
            requires (!is_pipeable_v<Arg>) AND is_pipeable_v<Pipe> AND
            invocable<Pipe, Arg>) // clang-format off
        friend constexpr auto operator|(Arg &&arg, Pipe pipe) // clang-format off
        {
            return static_cast<Pipe &&>(pipe)(static_cast<Arg &&>(arg));
        }

        // Compose two pipes
        template(typename Pipe0, typename Pipe1)(
            requires is_pipeable_v<Pipe0> AND is_pipeable_v<Pipe1>) // clang-format off
        friend constexpr auto operator|(Pipe0 pipe0, Pipe1 pipe1) // clang-format on
        {
            return make_pipeable(compose(detail::move(pipe1), detail::move(pipe0)));
        }

        template<typename Arg, typename Pipe>
        friend auto operator|=(Arg & arg, Pipe pipe) //
            -> CPP_broken_friend_ret(Arg &)(
                requires (is_pipeable_v<Pipe>) &&
                    (!is_pipeable_v<Arg>) && invocable<Pipe, Arg &>)
        {
            static_cast<Pipe &&>(pipe)(arg);
            return arg;
        }

        // Default Pipe behavior just passes the argument to the pipe's function call
        // operator
        // clang-format off
        template<typename Arg, typename Pipe>
        static constexpr auto CPP_auto_fun(pipe)(Arg && arg, Pipe p)
        (
            return static_cast<Pipe &&>(p)(static_cast<Arg &&>(arg))
        )
        // clang-format on
    };

    template<typename>
    using pipeable RANGES_DEPRECATED("Please use pipeable_base instead") = pipeable_base;

    /// \endcond

    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
