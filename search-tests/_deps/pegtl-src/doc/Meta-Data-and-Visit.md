# Meta Data and Visit

Each rule has several type aliases that allow for automatic inspection of a grammar and all of its rules for multiple purposes.
Note that true custom rules, i.e. rules that implement custom `match()` functions, do **not** need to define these type aliases for parsing.
They are only required to support functions based on `visit()` and the [grammar analysis](Grammar-Analysis.md).

* [Internals](#internals)
* [Rule Type](#rule-type)
* [Sub Rules](#sub-rules)
* [Grammar Visit](#grammar-visit)
* [Grammar Print](#grammar-print)
* [Rule Coverage](#rule-coverage)

## Internals

While accessible in the namespace `TAO_PEGTL_NAMESPACE`, which defaults to `tao::pegtl`, the [rules and combinators](Rule-Reference.md) included with the PEGTL all have their actual implementation in the sub-namespace `internal`.
For example the header `include/tao/pegtl/rules.hpp` shows how the user-facing rules are nothing more than forwarders to their `internal` implementation.

The original motivation for this additional level of indirection was to prevent uninteded invocation of user-defined actions due to some PEGTL rules being built from exisiting rules instead of having a dedicated implementation.
For example consider `rep_min` from `include/tao/pegtl/internal/rep_min.hpp`.

```c++
template< unsigned Min, typename Rule, typename... Rules >
using rep_min = seq< rep< Min, Rule, Rules... >, star< Rule, Rules... > >;
```

The expansion of `rep_min` uses the sub-rule `star< Rule, Rules... >`.
If a grammar were to contain both `rep_min` and `star` with the same sub-rules, and an action were provided for `star` with these exact sub-rules, then the action would not only be called for the explicit occurrences of `star` in the grammar but *also* for the corresponding sub-rule of `rep_min`.

The action invocation for the sub-rule of `rep_min` is considered surprising and undesirable because it exposes implementation details to the user, forces her to deal with them, and breaks her grammar if the implementation were to change.

Therefore it is possible to selectively disable most of the [control](Control-and-Debug.md) functions, including the `apply`-functions that perform action invocation, on a rule-by-rule basis.
More precisely, the action and debug control functions are only invoked for a rule `R` when `control< R >::enable` is `true`.

```c++
template< typename Rule >
struct normal
{
   static constexpr bool enable = internal::enable_control< Rule >;

// ...
};
```

The default control class template `normal` defines `normal< R >::enable` in terms of `internal::enable_control< R >` which is `true` by default, but `false` for all rules in sub-namespace `internal`, thereby hiding all invocations of `internal` rules from all actions and most of the control class functions.

The facilities documented on this page however do **not** hide the implementation details since, while debugging a grammar or a parsing run, it is essential to have a complete picture.

## Rule Type

For each rule `R`, the type alias `R::rule_t` is defined as the type of the class that actually implements the `match()` function.
This is usually the root of the inheritance hierarchy.

Note that `R::rule_t` can be completely different from `R`.
For example while `seq< R >::rule_t` is `internal::seq< R >`, due to `seq<>` being not only equivalent to `success`, but also implemented in terms of it, `seq<>::rule_t` is actually `internal::success`.

In each rule's implementation mapping section the [rule reference](Rule-Reference.md) shows how `rule_t` is defined depending on the template parameters.

## Sub Rules

For each rule `R`, the type alias `R::subs_t` is an instantiation of `type_list` with all the direct sub-rules of `R` as template parameters.

For example `seq< R, S >::subs_t` is `type_list< R, S >` and `alpha::subs_t` is `type_list<>`.

Note that for many rules with sub-rules the corresponding `subs_t` is not as might be expected.
For example `enable< R, S >::subs_t` is `type_list< internal::seq< R, S > >` instead of the probably expected `type_list< R, S >`.

Please again consult the [Rule Reference](Rule-Reference.md) (or the source) for how `subs_t` is defined for all included rules.

## Grammar Visit

The `visit()` function uses `subs_t` to recursively find all sub-rules of a grammar and call a function for each of them.

1. The first, explicit, template parameter of `visit()` is the starting rule of the grammar that is to be inspected.

2. The second, explicit, template parameter of `visit()` is a class template `F` where, for every sub-rule `R`, `visit()` will call `F< R >::visit()`.

All arguments given to `visit()` will be passed on to all `F< R >::visit()` calls.

The header `include/tao/pegtl/contrib/visit_rt.hpp` contains a drop-in replacement for `visit()` called `visit_rt()` that uses a run-time data structure, rather than compile-time type lists, to make sure the visitor is called only once for every grammar rule.
This can be a advantageous when working with large grammars since it reduces the template instantiation depth by shifting some of the work from compile time to run time.
Unlike `visit()`, `visit_rt()` returns the number of rules visited.

## Grammar Print

The functions `print_rules()` and `print_sub_rules()` from `include/tao/pegtl/contrib/print.hpp` combine the `visit()` function with visitors that print some information about all (sub-)rules of the supplied grammar to the supplied `std::ostream`.

See `src/example/pegtl/json_print_rules.cpp` and `src/example/pegtl/json_print_sub_rules.cpp` for how to use these functions, and what the output looks like.
As expected, the `internal` sub-rules are printed, too.

## Rule Coverage

The function `coverage()` from `include/tao/pegtl/contrib/coverage.hpp` is very similar to the `parse()` function.
It is called like `parse()`, with the some of the same template parameters and all of the same function arguments, however it returns an object of type `coverage_result` instead of a `bool`.

```c++
template< typename Rule,
          template< typename... > class Action = nothing,
          template< typename... > class Control = normal,
          typename ParseInput,
          typename... States >
coverage_result coverage( ParseInput& in,
                          States&&... st );
```

After a parsing run, the `coverage_result` indicates whether the run was a success or a failure, and contains "rule coverage" and "branch coverage" information.

The "rule coverage" shows how often each rule was attempted to match, and how many of these attempts were a success or a -- local or global -- failure.

The "branch coverage" consists in the matching information also being recorded for each immediate sub-rule of every rule; in the case of an `sor<>` this shows how often each sub-rule was taken, hence the name.

The coverage information in the `coverage_result` can either be inspected and processed or printed manually, or the `ostream` output `operator<<` from `include/tao/pegtl/contrib/print_coverage.hpp` can be used.
The operator formats the output as JSON.

```c++
std::ostream operator<<( std::ostream&, const coverage_result& );
```

The coverage information in the `coverage_result` is defined as follows.
The `coverage_info` is used in two places, as part of the `coverage_entry` for each rule, and as value in the `branches` map for each immediate sub-rule.

```c++
struct coverage_info
{
   std::size_t start = 0;  // How often a rule was attempted to match.
   std::size_t success = 0;  // How many attempts were a success (true).
   std::size_t failure = 0;  // How many attempts were a local failure (false).
   std::size_t unwind = 0;  // How many attempts were aborted due to an exception (thrown here or elsewhere).
   std::size_t raise = 0;  // How many attempts were a global failure (exception thrown at this rule).
};

struct coverage_entry
   : coverage_info  // The coverage_info for each rule.
{
   std::map< std::string_view, coverage_info > branches;  // The coverage_info for each immediate sub-rule.
};

struct coverage_result
{
   std::string_view grammar;  // Name of the top-level grammar rule.
   std::string source;  // From the input.

   std::map< std::string_view, coverage_entry > map;  // The coverage_entry for each rule.
   bool result;  // Whether the parsing run was a success.
};
```

As usual, unless otherwise indicated, all functions and data structure are in the namespace `tao::pegtl`.

Copyright (c) 2020-2022 Dr. Colin Hirsch and Daniel Frey
