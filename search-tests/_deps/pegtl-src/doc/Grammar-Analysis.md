# Grammar Analysis

The PEGTL contains an `analyze()` function that checks a grammar for rules that can enter an infinite loop without consuming input.

## Content

* [Running](#running)
* [Example](#example)
* [Requirements](#requirements)
* [Limitations](#limitations)

## Running

In order to run an analysis on a grammar it is necessary to explicitly include `<tao/pegtl/contrib/analyze.hpp>`.
Then call `tao::pegtl::analyze()` with the top-level grammar rule as template argument.

```c++
#include <tao/pegtl/contrib/analyze.hpp>

const std::size_t issues = tao::pegtl::analyze< my_grammar >();
```

The `analyze()` function prints some information about the found issues to `std::cout` and returns the total number of issues found.
The output can be suppressed by passing `-1` as sole function argument, or be extended to give some information about the issues when called with `1`.

Analysing a grammar is usually only done while developing and debugging a grammar, or after changing it.

## Example

Regarding the kinds of issues that are detected, consider the following example rules.

```c++
struct bar;

struct foo
   : sor< digit, bar > {};

struct bar
   : plus< foo > {};
```

When attempting to match `bar` against an input where the next character is not a digit the parser immediately goes into an infinite loop between `bar` calling `foo` and then `foo` calling `bar` again.

As shown by the example program `src/example/pegtl/analyze.cpp`, the grammar analysis will correctly detect a cycle without progress in this grammar.

Due to the differences regarding back-tracking and non-deterministic behaviour, this kind of infinite loop is a frequent issue when translating a CFG into a PEG.

## Requirements

The `analyze()` function operates on an abstract form of the grammar that is mostly equivalent to the original grammar regarding the possibility of infinite cycles without progress.

This abstract form is obtained via specialisations of the `analyze_traits<>` class template which each must have exactly one of `analyze_any_traits`, `analyze_opt_traits`, `analyze_seq_traits` and `analyze_sor_traits` as public (direct or indirect) base class.

Specialisations of the `analyze_traits<>` class template are appropriately implemented for all grammar rule classes included with the PEGTL.
This support automatically extends to all custom rules built "the usual way" via public inheritance of (combinations and specialisations of) rules included with the PEGTL.

For true custom rules, i.e. rules that implement their own `match()` function, the following steps need to be taken for them to work with the grammar analysis.

1. The rule needs a [`rule_t`](Meta-Data-and-Visit.md#rule-type) that, usually for true custom rules, is a type alias for the grammar rule itself.
2. There needs to be a specialisation of the `analyze_traits<>` for the custom rule, with an additional first template parameter:

Assuming a custom rule like the following

```c++
struct my_rule
{
   using rule_t = my_rule;

   template< typename Input >
   bool match( Input& in )
   {
      return /* Something that always consumes on success... */ ;
   }
};
```

the analyze traits need to be set up as

```c++
// In namespace TAO_PEGTL_NAMESPACE

template< typename Name >
struct analyze_traits< Name, my_rule >
   : analyze_any_traits<>
{};
```

where the base class is chosen as follows.

1. `analyze_any_traits<>` is used for rules that always consume when they succeed.
2. `analyze_opt_traits<>` is used for rules that (can also) succeed without consuming.
3. `analyze_seq_traits<>` is used for rules that, regarding their match behaviour, are equivalent to `seq<>`.
4. `analyze_sor_traits<>` is used for rules that, regarding their match behaviour, are equivalent to `sor<>`.

If `my_rule` has rules, that it calls upon as sub-rules, as template parameters, these need to be passed as template parameters to the chosen base class.

Note that the first template parameter `Name` is required by the analyse traits of some rules in order to facilitate their transcription in terms of the basic combinators `seq`, `sor` and `opt`.

For example `R = plus< T >` is equivalent to `seq< T, opt< R > >`, and the corresponding specialisation of the analyse traits is as follows.

```c++
   template< typename Name, typename... Rules >
   struct analyze_traits< Name, internal::plus< Rules... > >
      : analyze_traits< Name, typename seq< Rules..., opt< Name > >::rule_t >
   {};
```

Note how the specialisation is for `internal::plus` rather than `plus`.
The convention is that only the classes that actually implement `match()` define `rule_t`.
This greatly reduces both the number of classes that need to define `rule_t` as well as the number of required `analyze_traits` specialisations.

Note further how `Name` is required to transform the implicitly iterative rule `plus` into an explicitly recursive form that only uses `seq` and `opt`.
The analyse traits have the task of simplifying the grammar in order to keep the core analysis algorithm as simple as possible.

Please consult `include/tao/pegtl/contrib/analyze_traits.hpp` for many examples of how to correctly set up analyse traits for more complex rules, in particular for rules that do not directly fall into one of the aforementioned four categories.

For any further reaching questions regarding how to set up the traits for custom rules please contact the authors at **taocpp(at)icemx.net**.

## Limitations

It has been conjectured, but, given the expressive power of PEGs, not proven, that *all* potential infinite loops are correctly detected.

In practice it appears to catch all cases of left-recursion that are typical for grammars converted from CFGs or other formalisms that gracefully handle left-recursion.

False positives are a theoretical problem in that, while relatively easy to trigger, they are not usually encountered when dealing with real world grammars.

Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
