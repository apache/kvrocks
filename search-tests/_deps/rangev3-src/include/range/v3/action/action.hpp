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

#ifndef RANGES_V3_CONTAINER_ACTION_HPP
#define RANGES_V3_CONTAINER_ACTION_HPP

#include <type_traits>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/action/concepts.hpp>
#include <range/v3/functional/compose.hpp>
#include <range/v3/functional/concepts.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/functional/reference_wrapper.hpp>
#include <range/v3/functional/pipeable.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/utility/move.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-actions
    /// @{
    struct make_action_closure_fn
    {
        template<typename Fun>
        constexpr actions::action_closure<Fun> operator()(Fun fun) const
        {
            return actions::action_closure<Fun>{static_cast<Fun &&>(fun)};
        }
    };

    /// \sa make_action_closure_fn
    RANGES_INLINE_VARIABLE(make_action_closure_fn, make_action_closure)

    /// \cond
    namespace detail
    {
        struct action_closure_base_
        {};
    }
    /// \endcond

    /// \concept invocable_action_closure_
    /// \brief The \c invocable_action_closure_ concept
    template(typename ActionFn, typename Rng)(
    concept (invocable_action_closure_)(ActionFn, Rng),
        !derived_from<invoke_result_t<ActionFn, Rng>, detail::action_closure_base_>
    );
    /// \concept invocable_action_closure
    /// \brief The \c invocable_action_closure concept
    template<typename ActionFn, typename Rng>
    CPP_concept invocable_action_closure =
        invocable<ActionFn, Rng> &&
        CPP_concept_ref(ranges::invocable_action_closure_, ActionFn, Rng);

    namespace actions
    {
        struct RANGES_STRUCT_WITH_ADL_BARRIER(action_closure_base)
          : detail::action_closure_base_
        {
            // clang-format off
            // Piping requires things are passed by value.
            template(typename Rng, typename ActionFn)(
                requires (!std::is_lvalue_reference<Rng>::value) AND
                range<Rng> AND invocable_action_closure<ActionFn, Rng &>)
            friend constexpr auto
            operator|(Rng && rng, action_closure<ActionFn> act)
            {
                return aux::move(static_cast<ActionFn &&>(act)(rng));
            }

#ifndef RANGES_WORKAROUND_CLANG_43400
            template<typename Rng, typename ActionFn>   // ******************************
            friend constexpr auto                       // ******************************
            operator|(Rng &,                            // ********* READ THIS **********
                      action_closure<ActionFn> const &) // ****** IF YOUR COMPILE *******
                -> CPP_broken_friend_ret(Rng)(          // ******** BREAKS HERE *********
                    requires range<Rng>) = delete;      // ******************************
            // **************************************************************************
            // *    When piping a range into an action, the range must be moved in.     *
            // **************************************************************************
#endif // RANGES_WORKAROUND_CLANG_43400

            template<typename ActionFn, typename Pipeable>
            friend constexpr auto operator|(action_closure<ActionFn> act, Pipeable pipe)
                -> CPP_broken_friend_ret(action_closure<composed<Pipeable, ActionFn>>)(
                    requires (is_pipeable_v<Pipeable>))
            {
                return make_action_closure(compose(static_cast<Pipeable &&>(pipe),
                                                   static_cast<ActionFn &&>(act)));
            }

            template<typename Rng, typename ActionFn>
            friend constexpr auto operator|=(Rng & rng, action_closure<ActionFn> act) //
                -> CPP_broken_friend_ret(Rng &)(
                    requires range<Rng> && invocable<ActionFn, Rng &>)
            {
                static_cast<ActionFn &&>(act)(rng);
                return rng;
            }
            // clang-format on
        };

#ifdef RANGES_WORKAROUND_CLANG_43400
        // clang-format off
        namespace RANGES_ADL_BARRIER_FOR(action_closure_base)
        {
            template(typename Rng, typename ActionFn)(  // *******************************
                requires range<Rng>)                    // *******************************
            constexpr Rng                               // ********** READ THIS **********
            operator|(Rng &,                            // ******* IF YOUR COMPILE *******
                      action_closure<ActionFn> const &) // ********* BREAKS HERE *********
                = delete;                               // *******************************
            // ***************************************************************************
            // *    When piping a range into an action, the range must be moved in.      *
            // ***************************************************************************
        } // namespace RANGES_ADL_BARRIER_FOR(action_closure_base)
        // clang-format on
#endif    // RANGES_WORKAROUND_CLANG_43400

        template<typename ActionFn>
        struct RANGES_EMPTY_BASES action_closure
          : action_closure_base
          , ActionFn
        {
            action_closure() = default;

            constexpr explicit action_closure(ActionFn fn)
              : ActionFn(static_cast<ActionFn &&>(fn))
            {}
        };

        /// \cond
        /// DEPRECATED STUFF
        struct action_access_
        {
            template<typename Action>
            struct impl
            {
                // clang-format off
                template<typename... Ts, typename A = Action>
                static constexpr auto CPP_auto_fun(bind)(Ts &&... ts)
                (
                    return A::bind(static_cast<Ts &&>(ts)...)
                )
                // clang-format on
            };
        };

        using action_access RANGES_DEPRECATED(
            "action_access and actions::action<> are deprecated. Please "
            "replace action<> with action_closure<> and discontinue use of "
            "action_access.") = action_access_;

        template<typename>
        struct old_action_;

        struct make_action_fn_
        {
            template<typename Fun>
            constexpr old_action_<Fun> operator()(Fun fun) const
            {
                return old_action_<Fun>{static_cast<Fun &&>(fun)};
            }
        };
        using make_action_fn RANGES_DEPRECATED(
            "make_action_fn is deprecated. Please use "
            "make_action_closure instead.") = make_action_fn_;

        namespace
        {
            RANGES_DEPRECATED(
                "make_action and actions::action<> has been deprecated. Please switch to "
                "make_action_closure and action::action_closure.")
            RANGES_INLINE_VAR constexpr auto & make_action =
                static_const<make_action_fn_>::value;
        } // namespace

        template<typename Action>
        struct old_action_ : pipeable_base
        {
        private:
            Action act_;
            friend pipeable_access;

        public:
            old_action_() = default;

            constexpr explicit old_action_(Action a) noexcept(
                std::is_nothrow_move_constructible<Action>::value)
              : act_(detail::move(a))
            {}

            // Calling directly requires things are passed by reference.
            template(typename Rng, typename... Rest)(
                requires range<Rng> AND invocable<Action const &, Rng &, Rest...>)
            invoke_result_t<Action const &, Rng &, Rest...> //
            operator()(Rng & rng, Rest &&... rest) const
            {
                return invoke(act_, rng, static_cast<Rest &&>(rest)...);
            }

            // Currying overload.
            // clang-format off
            template(typename... Rest, typename A = Action)(
                requires (sizeof...(Rest) != 0))
            auto CPP_auto_fun(operator())(Rest &&... rest)(const)
            (
                return make_action_fn_{}(
                    action_access_::impl<A>::bind(act_,
                                                  static_cast<Rest &&>(rest)...))
            )
            // clang-format on
        };

        template<typename Action>
        using action RANGES_DEPRECATED(
            "The actions::action<> template is deprecated. Please switch to "
            "action_closure") = old_action_<Action>;
        /// \endcond
    } // namespace actions

    template<typename ActionFn>
    RANGES_INLINE_VAR constexpr bool is_pipeable_v<actions::action_closure<ActionFn>> =
        true;
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
