# Errors and Exceptions

A parsing run, a call to one of the `parse()` functions as explained in [Inputs and Parsing](Inputs-and-Parsing.md), can have the same results as calling `match()` on a grammar rule.

* A return value of `true` indicates a *successful* match.
* A return value of `false` is called a *local failure* (even when propagated to the top).
* An exception indicating a *global failure* is thrown.

The PEGTL parsing rules throw exceptions of type `tao::pegtl::parse_error`, some of the inputs throw additional exceptions like `std::system_error` or `std::filesystem_error`.
Other exception classes can be used freely from actions and custom parsing rules.

## Contents

* [Global Failure](#global-failure)
* [Local to Global Failure](#local-to-global-failure)
  * [Intrusive Local to Global Failure](#intrusive-local-to-global-failure)
  * [Non-Intrusive Local to Global Failure](#non-intrusive-local-to-global-failure)
* [Global to Local Failure](#global-to-local-failure)
* [Examples for Must Rules](#examples-for-must-rules)
* [Custom Exception Messages](#custom-exception-messages)

## Global Failure

By default, global failure means that an exception of type `tao::pegtl::parse_error` is thrown.

Synposis:

```c++
namespace tao::pegtl
{
   class parse_error
      : public std::runtime_error
   {
      parse_error( const char* msg, position p );

      parse_error( const std::string& msg, position p )
         : parse_error( msg.c_str(), std::move( p ) )
      {}

      template< typename ParseInput >
      parse_error( const char* msg, const ParseInput& in )
         : parse_error( msg, in.position() )
      {}

      template< typename ParseInput >
      parse_error( const std::string& msg, const ParseInput& in )
         : parse_error( msg, in.position() )
      {}

      const char* what() const noexcept override;

      std::string_view message() const noexcept;
      const std::vector< position >& positions() const noexcept;

      void add_position( position&& p );
   };
}
```

The `what()` message will contain all positions as well as the original `msg`.
This allows retrieval of all information if the exception is handled as a `std::runtime_error` in a generic way.

The `message()` function will return the original `msg`, while `positions()` allows access to the stored positions.
This is useful to decompose the exception and provide more helpful errors to the user.

The constructors can be used by custom rules to signal global failure, while `add_position()` is often used when you are parsing nested data, so you can append the position in the original file which includes the nested file.

## Local to Global Failure

### Intrusive Local to Global Failure

A local failure returned by a parsing rule is not necessarily propagated to the top, for example when the rule is

* in a rule like `not_at<>`, `opt<>` or `star<>`, or
* not the last rule inside an `sor<>` combinator.

To convert local failures to global failures, the `must<>` combinator rule can be used (together with related rules like `if_must<>`, `if_must_else<>` and `star_must<>`).
The `must<>` rule is equivalent to `seq<>` in that it attempts to match all sub-rules in sequence, but converts all local failures of the (direct) sub-rules to global failures.

Global failures can also be unconditionally provoked with the `raise<>` grammar rule, which is more flexible since the template argument can be any type, not just a parsing rule.
It should be mentioned that `must< R >` is semantically equivalent to `sor< R, raise< R > >`, but more efficient.

In any case, the task of actually throwing an exception is delegated to the [control class'](Control-and-Debug.md) `raise()`.

Note that rules and actions can throw exceptions directly, meaning those are not generated from the [control class'](Control-and-Debug.md) `raise()`.

### Non-Intrusive Local to Global Failure

If a grammar does not contain any `must<>` rule(s) (or `raise<>`, `if_must<>`, ...), one can still convert a local failure for a rule into a global failure via `must_if<>`.
This helper allows one to create a [control class'](Control-and-Debug.md) and provide custom error messages for global failures.
If an error message is provided for a rule that would normally return a local failure, it is automatically converted to a global failure.
See [Custom Exception Messages](#custom-exception-messages) for more information.

## Global to Local Failure

To convert global failure to local failure, the grammar rules [`try_catch`](Rule-Reference.md#try_catch-r-) and [`try_catch_type`](Rule-Reference.md#try_catch_type-e-r-) can be used.
Since these rules are not very commonplace they are ignored in this document, in other words we assume that global failure always propagages to the top.

## Examples for Must Rules

One basic use case of the `must<>` rule is as top-level grammar rule.
Then a parsing run can only either be successful, or throw an exception, it is not necessary to check the return value of the `parse()` function.

For another use case consider the following parsing rules for a simplified C-string literal that only allows `\n`, `\r` and `\t` as escape sequences.
The rule `escaped` is for a single escaped character, the rule `content` is for the complete content of such a literal.

```c++
using namespace tao::pegtl;
struct escaped : seq< one< '\\' >, one< 'n', 'r', 't' > > {};
struct content : star< sor< escaped, not_one< '\\', '"' > > > {};
struct literal : seq< one< '"' >, content, one< '"' > > {};
```

The `escaped` rule first matches a backslash, and then one of the allowed subsequent characters.
When either of the two `one<>` rules returns a local failure, then so will `escaped` itself.
In that case backtracking is performed in the `sor<>` and it will attempt to match the `not_one< '\\', '"' >` at the same input position.

This backtracking is appropriate if the `escaped` rule failed to match for lack of a backslash in the input.
It is however *not* appropriate when the backslash was not followed by one of the allowed characters since we know that there is no other possibility that will lead to a successful match.

We can therefore re-write the `escaped` rule as follows so that once the backslash has matched we need one of the following allowed characters to match, otherwise a global failure is thrown.

```c++
using namespace tao::pegtl;
struct escaped : seq< one< '\\' >, must< one< 'n', 'r', 't' > > > {};
```

A `seq<>` where all but the first sub-rule is inside a `must<>` occurs frequently enough to merit a convenience rule.
The following rule is equivalent to the above.

```c++
using namespace tao::pegtl;
struct escaped : if_must< one< '\\' >, one< 'n', 'r', 't' > > {};
```

Now the `escaped` rule can only return local failure when the next input byte is not a backslash.
This knowledge can be used to simplify the `content` rule by not needing to exclude the backslash in the following rule.

```c++
using namespace tao::pegtl;
struct content : star< sor< escaped, not_one< '"' > > > {};
```

Finally we apply our "best practice" and give the `one< 'n', 'r', 't' >` rule a dedicated name.
This will improve the built-in error message when the global failure is thrown, and also prevents actions or custom error messages (as explained below) from accidentally attaching to the same rule used in multiple places in a grammar.
The resulting example is as follows.

```c++
using namespace tao::pegtl;
struct escchar : one< 'n', 'r', 't' > {};
struct escaped : if_must< one< '\\' >, escchar > {};
struct content : star< sor< escaped, not_one< '"' > > > {};
struct literal : seq< one< '"' >, content, one< '"' > > {};
```

The same use of `if_must<>` can be applied to the `literal` rule assuming that it occurs in some `sor<>` where it is the only rule whose matched input can begin with a quotation mark...

## Custom Exception Messages

By default, when using any `must<>` error points, the exceptions generated by the PEGTL use the demangled name of the failed parsing rule as descriptive part of the error message.
This is often insufficient and one would like to provide more meaningful error messages.

A practical technique to provide customised error messages for all `must<>` error points uses the `must_if<>` helper.

For an example of this method see `src/example/pegtl/json_errors.hpp`, where all errors that might occur in the supplied JSON grammar are customised like this:

```c++
template< typename > inline constexpr const char* error_message = nullptr;

template<> inline constexpr auto error_message< tao::pegtl::json::text > = "no valid JSON";

template<> inline constexpr auto error_message< tao::pegtl::json::end_array > = "incomplete array, expected ']'";
template<> inline constexpr auto error_message< tao::pegtl::json::end_object > = "incomplete object, expected '}'";
template<> inline constexpr auto error_message< tao::pegtl::json::member > = "expected member";
template<> inline constexpr auto error_message< tao::pegtl::json::name_separator > = "expected ':'";
template<> inline constexpr auto error_message< tao::pegtl::json::array_element > = "expected value";
template<> inline constexpr auto error_message< tao::pegtl::json::value > = "expected value";

template<> inline constexpr auto error_message< tao::pegtl::json::digits > = "expected at least one digit";
template<> inline constexpr auto error_message< tao::pegtl::json::xdigit > = "incomplete universal character name";
template<> inline constexpr auto error_message< tao::pegtl::json::escaped > = "unknown escape sequence";
template<> inline constexpr auto error_message< tao::pegtl::json::char_ > = "invalid character in string";
template<> inline constexpr auto error_message< tao::pegtl::json::string::content > = "unterminated string";
template<> inline constexpr auto error_message< tao::pegtl::json::key::content > = "unterminated key";

template<> inline constexpr auto error_message< tao::pegtl::eof > = "unexpected character after JSON value";

// As must_if can not take error_message as a template parameter directly, we need to wrap it:
struct error { template< typename Rule > static constexpr auto message = error_message< Rule >; };

template< typename Rule > using control = tao::pegtl::must_if< error >::control< Rule >;
```

`must_if<>` expects a wrapper for the error message as its first template parameter.
There is a second parameter for the base control class, which defaults to `tao::pegtl::normal`, and which can combine `must_if`'s control class with other control classes.

Since `raise()` is only instantiated for those rules for which `must<>` could trigger an exception, it is sufficient to provide specialisations of the error message string for those rules.
Furthermore, there will be a compile-time error (i.e. a `static_assert`) for all rules for which the specialisation was forgotten although `raise()` could be called.

The [control class](Control-and-Debug.md) provided by `must_if<>` also turns, by default, local failures into global failure if an error message is provided, i.e. if the error message is not `nullptr`.
This means that one can provide additional points in the grammar where a global failure is triggered, even when the grammar contains no `must<>` error points.

The above feature also means that a rule which is used both with and without `must<>`, one would not only provide a custom error message for the location where the rule is failing within a `must<>`-context, but local errors in other contexts are implicitly turned into global error.
If this behaviour is not intended, one can disable the "turn local to global failure" feature by setting `raise_on_failure` to `false` in the wrapper class:

```c++
struct error
{
   template< typename Rule > static constexpr bool raise_on_failure = false;
   template< typename Rule > static constexpr auto message = error_message< Rule >;
};
```

It is advisable to choose the error points in the grammar with prudence.
This choice becoming particularly cumbersome and/or resulting in a large number of error points might be an indication of the grammar needing some kind of simplification or restructuring.

Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
