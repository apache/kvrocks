# Control and Debug

Beyond the top-level grammar rule, which *has* to be supplied to a parsing run, and an action class template, which *can* be supplied to a parsing run, a third customisation point within the PEGTL allows the user to provide a *control* class to a parsing run.

(Actually the control class is a class template which takes a parsing rule as template argument, however in many cases there will be no specialisations, wherefore we will drop the distinction and pretend here that it is simply a class.)

Functions from the control class are called in strategic places during a parsing run and can be used to customise internal behaviour of the PEGTL and/or as debug aids.
More precisely, the control class has static member functions to

1. trace which rules are attempted to match where, and whether they succeed or fail,
2. customise which exceptions are thrown in case of errors,
3. customise how an action's `apply()` or `apply0()` is called,
4. customise how a rule's `match()` is called.

## Contents

* [Normal Control](#normal-control)
* [Control Functions](#control-functions)
* [Exception Throwing](#exception-throwing)
* [Advanced Control](#advanced-control)
* [Changing Control](#changing-control)

## Normal Control

The `normal` control class template included with the PEGTL is used by default and shows which hook functions there are.

```c++
template< typename Rule >
struct normal
{
   static constexpr bool enable;  // See Meta-Data-and-Visit.md

   template< typename ParseInput,
             typename... States >
   static void start( const ParseInput&, States&&... );

   template< typename ParseInput,
             typename... States >
   static void success( const ParseInput&, States&&... );

   template< typename ParseInput,
             typename... States >
   static void failure( const ParseInput&, States&&... );

   template< typename ParseInput,
             typename... States >
   static void raise( const ParseInput& in, States&&... );

   template< template< typename... > class Action,
             typename Iterator,
             typename ParseInput,
             typename... States >
   static auto apply( const Iterator& begin, const ParseInput& in, States&&... st )
      -> decltype( ... );

   template< template< typename... > class Action,
             typename ParseInput,
             typename... States >
   static auto apply0( const ParseInput&, States&&... st )
      -> decltype( ... );

   template< apply_mode A,
             rewind_mode M,
             template< typename... > class Action,
             template< typename... > class Control,
             typename ParseInput,
             typename... States >
   static bool match( ParseInput& in, States&&... st );
};
```

The static member functions `start()`, `success()` and `failure()` can be used to debug a grammar by using them to provide insight into what exactly is going on during a parsing run, or to construct a parse tree, etc.

There is one more, *optional* hook function: `unwind()`.
It is called when a rule throws an exception, e.g. on global error.
It's signature is identical to `start()`/`success()`/`failure()`.
It is not included in the default control template `normal`, as the existence of an `unwind()` method requires an additional `try`/`catch` block.
This might potentially have an impact on the binary size and therefore, one should only add an `unwind()` method if necessary.
Several other control classes utilize the `unwind()` method to track the execution even in the presence of global errors.

The static member function `raise()` is used to create a global error, and any replacement should again throw an exception, or abort the application.

The static member functions `apply()` and `apply0()` can customise how actions with, and without, receiving the matched input are called, respectively.
Note that these functions should only exist or be visible when an appropriate `apply()` or `apply0` exists in the action class template.
This can be achieved via SFINAE, e.g. with a trailing return type as shown above.

The static member function `match()` by default checks if there exists a suitable `match()` in the action class template for the current rule. If so, it is called, otherwise it calls the main `tao::pegtl::match()` function.

## Control Functions

For debugging a grammar and tracing exactly what happens during a parsing run, the control class' `start()`, `success()` and `failure()` can be used.
In addition, `apply()` and `apply0()` can be used to see which actions are invoked.

Before attempting to match a rule `R`, the PEGTL calls `C< R >::start()` where `C` is the current control class template.

Depending on what happens during the attempt to match `R`, one of the other three functions might be called.

- If `R` succeeds, then `C< R >::success()` is called; compared to the call to `C< R >::start()`, the input will have consumed whatever the successful match of `R` consumed.
- If `R` finishes with a local failure, i.e. a return value of `false` from `match()`, then `C< R >::failure()` is called; a failed match **must not** consume input.
- If `R` finishes with a global failure, i.e. an exception being thrown from `match()`, then, if present, `C< R >::unwind()` is called.
- If `R` is wrapped in `must< R >`, a global failure is generated by calling `C< R >::raise()` to throw some exception as is expected by the PEGTL in this case.

Additionally, if matching `R` was successful and actions are enabled:

- If `C< R >::apply()` exists, then `C< R >::apply()` is called with the matched input and the current state arguments.
- If `C< R >::apply0()` exists, then `C< R >::apply0()` is called with the current state arguments.

It is an error when both `C< R >::apply()` and `C< R >::apply0()` exist.

Note that the default `C< R >::apply()` is SFINAE-enabled if `A< R >::apply()` exists (where `A` is the current action class template) and calls the latter. A control class might modify state, etc.

In case of actions that return `bool`, i.e. actions where `apply()` or `apply0()` return `bool`, `C< R >::success()` is only called when both the rule *and* the action succeed.
If either produce a (local) failure then `C< R >::failure()` is called.

In all cases where an action is called, the success or failure hooks are invoked after the action returns.

The included `<tao/pegtl/contrib/trace.hpp>` gives a practical example that shows how the control class can be used to debug grammars.

## Exception Throwing

The control-hook, the `raise()` static member function, **must** throw an exception.
For most parts of the PEGTL the exception class is irrelevant and any user-defined data type can be thrown by a user-defined control hook.

The `try_catch` rule only catches exceptions of type `tao::pegtl::parse_error`!

When custom exception types are used then `try_catch_type` must be used with the custom exception class that they are supposed to catch as first template argument.

## Advanced Control

The control's `match()` is the first, outer-most function in the call-chain that eventually calls the rule's `match()`.

For advanced use cases, it is possible to create a custom control class with a custom `match()` that can change "everything" before calling the rule's `match()`.

Similarly, the control's `apply()` and `apply0()` can customise action invocation; in particular `apply()` can change how the matched portion of the input is passed to the action.

## Changing Control

Just like the action class template, a custom control class template can be used (or changed) by either

1. supplying it as explicit template argument to the `parse()` functions, or
2. setting it as control class with the [`tao::pegtl::control`](Rule-Reference.md#control-c-r-) combinator, or
3. setting it as control class with the `change_control` action.

The latter requires the use of a [custom action](Actions-and-States.md).
Deriving the specialisation of the custom action for `my_rule` from `tao::pegtl::change_control< my_control >` will switch the current control to `my_control` before attempting to match `my_rule`.

Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
