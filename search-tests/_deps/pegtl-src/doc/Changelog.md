# Changelog

## 3.2.8

Released 2024-09-14

* Fixed build with `-Wshorten-64-to-32`.

## 3.2.7

Released 2022-07-14

* Removed superfluous deprecated include.

## 3.2.6

Released 2022-06-29

* Made `unwind()` optional for parse tree nodes.
* Fixed `demangle()` for MSVC, again.
* Fixed `demangle()` for GCC 12.

## 3.2.5

Released 2022-02-05

* Added missing include for fallback `demangle()` implementations.

## 3.2.4

Released 2022-02-03

* Fixed `version.hpp`.

## 3.2.3

Released 2022-02-03

* Fixed `static_assert` in `demangle()` with recent MSVC.

## 3.2.2

Released 2021-10-22

* Added rule [`odigit`](Rule-Reference.md#odigit) for octal digits.
* Enabled default-constructed state in `state<>`, `change_state<>`, and `change_action_and_state<>`.
* Changed rules in [`tao/pegtl/contrib/integer.hpp`](Contrib-and-Examples.md#taopegtlcontribintegerhpp) to not throw by default.
* Added [`tao/pegtl/contrib/separated_seq.hpp`](Contrib-and-Examples.md#taopegtlcontribseparated_seqhpp).
* Added `tao/pegtl/contrib/iri.hpp` grammar for IRIs.
* Added `tao/pegtl/contrib/proto3.hpp` grammar for protocol buffer v3.

## 3.2.1

Released 2021-07-31

* Added an optional limiter to guard against infinite recursion.
* Fixed CMake export error.
* Improved compile time efficiency.

## 3.2.0

Released 2021-01-15

* Added support for disabling exceptions with [`-fno-exceptions`](Installing-and-Using.md#disabling-exceptions).
* Improved efficiency of parse tree nodes.
* Fixed namespace issue with `tao::pegtl::demangle<T>()` (was: `tao::demangle<T>()`).

## 3.1.0

Released 2020-12-17

* Made `analyze()` more verbose by default to aid finding the rule cycles.
* Added `parse_nested()` overload that accepts a `position` as first argument.
* Added some experimental and undocumented `contrib` features and their infrastructure.
* Improved CMake support for [`<filesystem>`](Installing-and-Using.md#filesystem) fallbacks and alternatives.
  * Re-enabled support for GCC 7.
  * Automatically link with `libstdc++fs` or `libc++fs` as needed.
  * Added automatic fallback from `std::filesystem` to `std::experimental::filesystem`.
  * Added manual fallback from `std::filesystem` to `boost::filesystem`.
  * Thank you [Beman Dawes](https://isocpp.org/blog/2020/12/remembering-beman-dawes)!
* Converted continuous integration infrastructure to GitHub Actions.

## 3.0.0

Released 2020-11-28

* Use the [**migration guide**](Migration-Guide.md#version-300) when updating.
* Infrastructure
  * Updated required C++ standard to C++17.
  * Updated required [CMake](https://cmake.org/) version to 3.8.
  * The macro `TAO_PEGTL_NAMESPACE` now contains the fully qualified namespace, e.g. `tao::pegtl`.
  * Added `[[nodiscard]]` or `[[noreturn]]` to most non-void functions.
* Meta-Data Layer
  * Replaced `analysis_t` with more general and complete `rule_t` and `subs_t`.
  * Added functions to visit all rules of a grammar.
  * Added functions to measure rule coverage of a parsing run.
  * Moved the analysis function and header to contrib.
* Error Handling
  * Replaced `tao::pegtl::input_error` with `std::system_error` and `std::filesystem::filesystem_error`.
  * Added [`must_if<>`](Errors-and-Exceptions.md#custom-exception-messages)
    * Allows to define custom error messages for global errors.
    * Adds a non-intrusive way to define global parse errors for a grammar retroactively.
* Demangling
  * Removed the need for RTTI.
    * Some broken/unknown compilers will use RTTI as a fallback, without demangling.
  * Moved `tao::pegtl::internal::demangle<T>()` to `tao::demangle<T>()`.
  * Improved generated code to be shorter and more efficient.
* Parse Tree
  * Removed the need for RTTI.
* Other
  * Changed `std::string` to `std::filesystem::path` for filename parameters.
  * Renamed `byte_in_line` to `column` and use 1-based counting.
  * Moved rule `eolf` from inline namespace `tao::pegtl::ascii` to `tao::pegtl`.
  * Changed rules in `tao/pegtl/contrib/integer.hpp` to not accept redundant leading zeros.
  * Added rules to `tao/pegtl/contrib/integer.hpp` that test unsigned values against a maximum.
  * Demoted UTF-16 and UTF-32 support to contrib.
  * Demoted UINT-8, UINT-16, UINT-32 and UINT-64 support to contrib.
  * Folded `contrib/counter.hpp` into `json_count.cpp`, count is superceded by coverage.
  * Removed right padding from `contrib/json.hpp`'s `value`.
* Cleanup
  * Removed option of [state](Rule-Reference.md#state-s-r-)'s `S::success()` to have an extended signature to get access to the current `apply_mode`, `rewind_mode`, *action*- and *control* class (template).
  * Removed compatibility macros starting with `TAOCPP_PEGTL_`.
  * Removed compatibility uppercase enumerators.
  * Removed compatibility `peek_byte()` member functions.
  * Removed compatibility header `changes.hpp` from contrib.

## 2.8.3

Released 2020-04-22

* Fixed excessive read-ahead with incremental inputs.
* Added state manipulators `remove_first_state`, `remove_last_states`, `rotate_states_right`, `rotate_states_left`, and `reverse_states` to contrib.
* Reduced the number of intermediate parse tree nodes.

## 2.8.2

Released 2020-04-05

* Fixed parse tree node generation to correctly remove intermediate nodes.

## 2.8.1

Released 2019-08-06

* Added fallback symbol demangling if RTTI is disabled.
* Fixed missing `string_input<>` in amalgamated header.
* Fixed `discard_input*` actions to properly forward the apply mode.
* Fixed contrib HTTP grammar for chunked data.

## 2.8.0

Released 2019-04-09

* Use the [**migration guide**](Migration-Guide.md#version-280) when updating.
* Changed enumerators to lowercase.
  * Renamed `tracking_mode::IMMEDIATE` to `tracking_mode::eager`.
  * Compatibility enumerators with uppercase names are still included.
    * Will be removed in version 3.0.0.
* Renamed `peek_byte()` to `peek_uint8()`.
  * Compatibility member functions with previous names are still included.
    * Will be removed in version 3.0.0.
* Allowed actions to implement `match`.
* Made deriving action class templates from `nothing` optional.
* Added debug tools `require_apply` and `require_apply0`.
* Added combinator class [`rematch`](Rule-Reference.md#rematch-r-s-).
* Improved the [Parse Tree / AST interface](Parse-Tree.md) to mostly hide its internal state.
* Added new action-based helpers `change_*.hpp`.
  * The control-based helpers in `contrib/changes.hpp` are still included.
    * Will be removed in version 3.0.0.
* Added new action-based helpers `disable_action.hpp` and `enable_action.hpp`.
* Added new action-based helpers `discard_input.hpp`, `discard_input_on_success.hpp`, and `discard_input_on_failure.hpp`.
* Added [Clang Static Analyzer](https://clang-analyzer.llvm.org/) to the CI build.
* Added new Makefile target `amalgamate` to generate a single-header version of the PEGTL.
* Added support for [Universal Windows Platform (UWP)](https://en.wikipedia.org/wiki/Universal_Windows_Platform).

## 2.7.1

Released 2018-09-29

* Added new ASCII convenience rule [`forty_two`](Rule-Reference.md#forty_two-c-).
* Added experimental `if_then` rule.
* Simplified how parse tree nodes can be selected.
* Reduced the number of intermediate parse tree nodes.
* Allowed an action class template to be used with the parse tree.

## 2.7.0

Released 2018-07-31

* Added [`mmap_file<>`](Inputs-and-Parsing.md#file-input) support for Windows.
* Added [deduction guides](https://en.cppreference.com/w/cpp/language/class_template_argument_deduction) for the input classes when compiling with C++17.

## 2.6.1

Released 2018-07-22

* Fixed endianness detection in test program.

## 2.6.0

Released 2018-06-22

* Added [Conan](https://conan.io/) [packages](https://bintray.com/taocpp/public-conan/pegtl%3Ataocpp/).
* Fixed the UTF-8 decoder to no longer accept UTF-16 surrogates.
* Fixed the UTF-16 decoder to no longer accept UTF-16 unmatched surrogates.
* Fixed the UTF-32 "decoder" to no longer accept UTF-16 surrogates.
* Fixed `pegtl/contrib/unescape.hh` to no longer accept unmatched surrogates.
* Optimised convenience rule [`two`](Rule-Reference.md#two-c-).
* Added new convenience rule [`three`](Rule-Reference.md#three-c-).

## 2.5.2

Released 2018-05-31

* Fixed [`opt`](Rule-Reference.md#opt-r-) and [`until`](Rule-Reference.md#until-r-s-) to work as documented in some rare edge cases.
* Used [`opt_must`](Rule-Reference.md#opt_must-r-s-) and [`star_must`](Rule-Reference.md#star_must-r-s-) to optimise some included grammars.

## 2.5.1

Released 2018-05-14

* Added new convenience rule [`opt_must`](Rule-Reference.md#opt_must-r-s-).
* Optimised convenience rule [`if_must`](Rule-Reference.md#if_must-r-s-).
* Fixed examples to compile with Visual Studio and MinGW.
* Added [automated testing](https://travis-ci.org/taocpp/PEGTL) with GCC 8.

## 2.5.0

Released 2018-05-01

* Added rules to match Unicode properties via [ICU](http://site.icu-project.org) to contrib.
* Improved the [Parse Tree / AST interface](Parse-Tree.md).
* Fixed parse tree node generation to correctly remove intermediate nodes.
* Added big- and little-endian support to the UTF-16 and UTF-32 rules.
* Added rules for UINT-8 and big- and little-endian UINT-16, UINT-32 and UINT-64.
* Added member functions to `memory_input<>` to obtain the line around a position.
* Added member functions to `memory_input<>` to start again from the beginning.
* Added example for Python-style indentation-aware grammars.
* Added examples for regular, context-free, and context-sensitive grammars.
* Added example for how to parse with a symbol table.
* Added [automated testing](https://travis-ci.org/taocpp/PEGTL) with Clang 6.
* Added [automated testing](https://travis-ci.org/taocpp/PEGTL) with Clang's `-fms-extensions`.
* Fixed build with Clang when `-fms-extensions` is used (`clang-cl`).

## 2.4.0

Released 2018-02-17

* Use the [**migration guide**](Migration-Guide.md#version-240) when updating.
* Improved and documented the [Parse Tree / AST support](Parse-Tree.md).
* Changed prefix of all macros from `TAOCPP_PEGTL_` to `TAO_PEGTL_`.
  * Compatibility macros with the old names are provided.
  * They will be removed in version 3.0.0.
* Added a deleted overload to prevent creating a `memory_input<>` from a temporary `std::string`.

## 2.3.4

Released 2018-02-08

* Fixed build on older systems where `O_CLOEXEC` is not available.
* Added [automated testing](https://travis-ci.org/taocpp/PEGTL) with Android 6.0 and 7.0.

## 2.3.3

Released 2018-01-01

* Added more `noexcept`-specifications.
* Fixed most `clang-tidy`-issues.

## 2.3.2

Released 2017-12-16

* Worked around a Visual Studio 15.5 bug.

## 2.3.1

Released 2017-12-14

* Fixed linkage of `tao::pegtl::internal::file_open`.
* Improved error message for missing `source` parameter of `string_input<>`.

## 2.3.0

Released 2017-12-11

* Added constructor to `read_input<>` that accepts a `FILE*`, see issue [#78](https://github.com/taocpp/PEGTL/issues/78).
* Enhanced [`apply`](Rule-Reference.md#apply-a-), [`apply0`](Rule-Reference.md#apply0-a-) and [`if_apply`](Rule-Reference.md#if_apply-r-a-) to support `apply()`/`apply0()` returning boolean values.
* Simplified implementation of [`raw_string`](Contrib-and-Examples.md#taopegtlcontribraw_stringhpp), the optional `Contents...` rules' `apply()`/`apply0()` are now called with the original states.
* Fixed the tracer to work with `apply()`/`apply0()` returning boolean values.
* Fixed, simplified and improved [`examples/parse_tree.cpp`](Contrib-and-Examples.md#srcexamplepegtlparse_treecpp).

## 2.2.2

Released 2017-11-22

* Bumped version.

## 2.2.1

Released 2017-11-22

* Celebrating the PEGTL's 10th anniversary!
* Fixed missing call to the [control class'](Control-and-Debug.md#control-functions) `failure()` when a rule with `apply()` with a boolean return type fails.
* Fixed string handling in [`examples/abnf2pegtl.cc`](Contrib-and-Examples.md#srcexamplepegtlabnf2pegtlcpp).
* Simplified/improved Android build.

## 2.2.0

Released 2017-09-24

* Added possibility for an action's `apply()` or `apply0()` to return `bool` which is then used to determine overall success or failure of the rule to which such an action was attached.
* Added [`<tao/pegtl/contrib/parse_tree.hpp>`](Contrib-and-Examples.md#taopegtlcontribparse_treehpp) and the [`examples/parse_tree.cpp`](Contrib-and-Examples.md#srcexamplepegtlparse_treecpp) application that shows how to build a [parse tree](https://en.wikipedia.org/wiki/Parse_tree). The example goes beyond a traditional parse tree and demonstrates how to select which nodes to include in the parse tree and how to transform the nodes into an [AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree)-like structure.
* Added `bom` rules for UTF-8, UTF-16 and UTF-32.
* Added some missing includes for `config.hpp`.
* Added [automated testing](https://travis-ci.org/taocpp/PEGTL) with Clang 5.
* Added [automated testing](https://travis-ci.org/taocpp/PEGTL) with Xcode 9.

## 2.1.4

Released 2017-06-27

* Fixed shadow warning.

## 2.1.3

Released 2017-06-27

* Fixed [`raw_string`](Contrib-and-Examples.md#taopegtlcontribraw_stringhpp) with optional parameters.

## 2.1.2

Released 2017-06-25

* Bumped version.

## 2.1.1

Released 2017-06-25

* Fixed build with MinGW on Windows.
* Added [automated testing](https://ci.appveyor.com/project/taocpp/PEGTL) with MinGW.

## 2.1.0

Released 2017-06-23

* Added optional template parameters to [`raw_string`](Contrib-and-Examples.md#taopegtlcontribraw_stringhpp) for rules that the content must match.
* Added new contrib rules [`rep_one_min_max`](Contrib-and-Examples.md#taopegtlcontribrep_one_min_maxhpp) and `ellipsis`.
* Fixed broken [`TAOCPP_PEGTL_KEYWORD`](Rule-Reference.md#tao_pegtl_keyword--) macro.
* Fixed a bug in the contrib HTTP grammar which prevented it from parsing status lines in some cases.
* Fixed build with MinGW-w64 on Windows.
* Added [automated testing](https://ci.appveyor.com/project/taocpp/PEGTL) with MinGW-w64.
* Added [automated testing](https://travis-ci.org/taocpp/PEGTL) with GCC 7.

## 2.0.0

Released 2017-05-18

* Project

  * Migrated to ["The Art of C++"](https://github.com/taocpp).
  * Use the [**migration guide**](Migration-Guide.md#version-200) when updating.
  * Version 2.z can be installed and used in parallel to version 1.y of the PEGTL.
  * The semantics of all parsing rules and grammars is the same as for versions 1.y.

* Input Layer

  * Added support for custom [incremental input](Inputs-and-Parsing.md#incremental-input) readers.
  * Added support for parsing [C streams](Inputs-and-Parsing.md#stream-inputs), i.e. `std::FILE*`.
  * Added support for parsing [C++ streams](Inputs-and-Parsing.md#stream-inputs), i.e. `std::istream`.
  * Added support for different [EOL-styles](Inputs-and-Parsing.md#line-ending).
  * Renamed class `position_info` to `position`.
  * Added the byte position to input classes and `position`.
  * Added [fast parsing without line counting](Inputs-and-Parsing.md#tracking-mode) (except in errors).
  * Refactored the `input` class into multiple input classes.
  * Refactored the file parser classes into [input classes](Inputs-and-Parsing.md#file-input).
  * Refactored the handling of [nested parsing](Inputs-and-Parsing.md#nested-parsing).
  * Removed the `begin()` member from class `position`.
  * Removed most [parsing front-end functions](Inputs-and-Parsing.md#parse-function).

* Parsing Rules

  * Added combinator class [`minus`](Rule-Reference.md#minus-m-s-).
  * Added ASCII rule class [`keyword`](Rule-Reference.md#keyword-c--).
  * Added [`string`](Rule-Reference.md#string-c--1) rules for UTF-8, UTF-16 and UTF-32.
  * Added [`apply`](Rule-Reference.md#apply-a-), [`apply0`](Rule-Reference.md#apply0-a-) and [`if_apply`](Rule-Reference.md#if_apply-r-a-) rules for intrusive actions.
  * Added incremental input support rules [`discard`](Rule-Reference.md#discard) and [`require`](Rule-Reference.md#require-num-).

* String Macros

  * Renamed to [`TAOCPP_PEGTL_(I)STRING`](Rule-Reference.md#tao_pegtl_istring--).
  * Increased allowed string length to 512.
  * Allowed embedded null bytes.
  * Reduced template instantiation depth.

* Other Changes

  * Added `apply()` and `apply0()` to the [control class](Control-and-Debug.md#control-functions).
  * Optimised superfluous input markers.
  * Allowed optimisation of [actions that do not need the input](Actions-and-States.md#apply0).
  * Replaced layered matching with superior Duseltronik™.
  * Reduced template instantiation depth.
  * Added support for [CMake](https://cmake.org/).
  * Added [automated testing](https://ci.appveyor.com/project/taocpp/PEGTL) with Visual Studio 2015 and 2017.
  * Added automated testing with Android 5.1, NDK r10e.

## 1.3.1

Released 2016-04-06

* Fixed unit test to use `eol` instead of hard-coded line ending.

## 1.3.0

Released 2016-04-06

* Tentative Android compatibility.
* Fixed build with MinGW on Windows.
* Changed file reader to open files in binary mode.
* Changed `eol` and `eolf` to accept both Unix and MS-DOS line endings.
* Optimised bumping the input forward and removed little used bump function.
* Simplified grammar analysis algorithm (and more `analyze()` tests).

## 1.2.2

Released 2015-11-12

* Improved the JSON grammar and JSON string escaping.
* Added JSON test suite from http://json.org/JSON_checker/.
* Optimised bumping the input forward and string unescaping.
* Promoted `examples/json_changes.hh` to `pegtl/contrib/changes.hh`.

## 1.2.1

Released 2015-09-21

* Added `file_parser` as alias for `mmap_parser` or `read_parser` depending on availability of the former.
* Added Clang 3.7 to the automated tests.
* Added Mac OS X with Xcode 6 and Xcode 7 to the automated tests.
* Added coverage test and improved test coverage to 100%.
* Fixed state changing bug in `json_build_one` example.

## 1.2.0

Released 2015-08-23

* Added [`pegtl_string_t`](Rule-Reference.md#tao_pegtl_string--) and [`pegtl_istring_t`](Rule-Reference.md#tao_pegtl_istring--) to simplify string definitions as follows:
```c++
   pegtl::string< 'h', 'e', 'l', 'l', 'o' >  // Normal
   pegtl_string_t( "hello" )                 // New shortcut
```
* Added [`examples/abnf2pegtl.cc`](Contrib-and-Examples.md#srcexamplepegtlabnf2pegtlcpp) application that converts grammars based on [ABNF (RFC 5234)](https://tools.ietf.org/html/rfc5234) into a PEGTL C++ grammar.
* Added [`contrib/alphabet.hh`](Contrib-and-Examples.md#taopegtlcontribalphabethpp) with integer constants for alphabetic ASCII letters.

## 1.1.0

Released 2015-07-31

* Renamed namespace `pegtl::ucs4` to `pegtl::utf32` and generally adopted UTF-32 in all naming.
* Added experimental support for UTF-16 similar to the previously existing UTF-32 parsing rules.
* Added support for merging escaped UTF-16 surrogate pairs to `pegtl/contrib/unescape.hh`.
* Fixed incorrect handling of escaped UTF-16 surrogate pairs in the JSON examples.
* A [state](Rule-Reference.md#state-s-r-)'s `S::success()` can now have an extended signature to get access to the current `apply_mode`, *action*- and *control* class (template).
* The `contrib/raw_string` class template now calls `Action<raw_string<...>::content>::apply()` with the user's state(s).

## 1.0.0

Released 2015-03-29

Version 1.0.0 was a very large refactoring based on the previous years of experience.
The core design and approach were kept, but nearly all details of the implementation were changed, and some parts were added to, or removed from, the library.
Semantic versioning was introduced with version 1.0.0.

* Deprecated old site on Google code and published new version on GitHub.
* Removed the semi-automatic pretty-printing of grammar rules; now the class names are used, when possible demangled.
* Renamed rule classes with multiple words in their names to use underscores, e.g. `ifmust<>` is now `if_must<>`.
* Removed support for incremental/stream parsing to allow for some simplifications and optimisations (*reintroduced in 2.0.0*).
* Removed the rules `apply<>` and `if_apply<>` that were used to directly call actions from within the grammar (*reintroduced in 2.0.0*), and:
* Where the other method of attaching actions to rules in PEGTL 0.x required specialisation of a given class template `action<>`, in PEGTL 1.y the action class template can be chosen by the user and changed at any point in the grammar.
* As a side-effect there is a much cleaner way of enabling and disabling actions in a portion of the grammar.
* Actions now have access to the current position in the input, i.e. to the filename, and line and column number.
* Actions now receive a pointer to, and the size of, the matched portion of the input (previously a `std::string` with a copy of the matched data), therefore:
* ~~There is no distinction between actions that require access to the matched data and those that don't, furthermore~~:
* The object via which actions gain access to the matched data is similar to that which rules receive ~~so actions can easily invoke another grammar on the matched data.~~
* The `at<>` and `not_at<>` rules now call their subordinate rules with actions disabled.
* The variadic `states...` arguments that are passed through all rule invocations for use by the actions are *not* forwarded with `std::forward<>` anymore since it (usually) doesn't make much sense to move them, and accidentially moving multiple times was a possible error scenario.
* There are now five different `rep` rules for repeating a sequence of rules with more control over the acceptable or required number of repetitions.
* There are new rules `try_catch<>` and `try_catch_type<>` that convert global errors, i.e. exceptions, into local errors, i.e. a return value of `false`.
* Unified concept for actions and debug hooks, i.e. just like the actions are called from a class template that is passed into the top-level `parse()` function, there is another class template that is called for debug/trace and error throwing purposes; both can be changed at any point within the grammar.
* A large under-the-hood reorganisation has the benefit of preventing actions from being invoked on rules that are implementation details of other rules, e.g. the `pad< Rule, Padding >` rule contains `star< Padding >` in its implementation, so a specialisation of the action-class-template for `star< Padding >` would be called within `pad<>`, even though the `star< Pad >` was not explicitly written by the user; in PEGTL 1.y these unintended action invocations no longer occur.
* Partial support for Unicode has been added in the form of some basic rules like `one<>` and `range<>` also being supplied in a UTF-8 (and experimental UTF-16 and UTF-32) aware version(s) that can correctly process arbitrary code points from `0` to `0x10ffff`.
* The supplied input classes work together with the supplied exception throwing to support better error locations when performing nested file parsing, i.e. a `parse_error` contains a vector of parse positions.
* Added a function to analyse a grammar for the presence of infinite loops, i.e. cycles in the rules that do not (necessarily) consume any input like left recursion.
* As actions are applied to a grammar in a non-invasive way, several common grammars were added to the PEGTL as documented in [Contrib and Examples](Contrib-and-Examples.md).
* The `list<>`-rule was replaced by a set of new list rules with different padding semantics.
* The `at_one<>` and other rules `foo` that are merely shortcuts for `at< foo >` were removed.
* The `if_then<>` rule was removed.
* The `error_mode` flag was removed.
* The semantics of the `must<>` rules was changed to convert local failure to global failure only for the immediate sub-rules of a `must<>` rule.
* The `parse()` functions now return a `bool` and can also produce local failures. To obtain the previous behaviour of success-or-global-failure, the top-level grammar rule has to be wrapped in a `must<>`.

## 0.32

Released 2012-12

* Removed superfluous includes (issue 5 from Google code hosting).
* Fixed bug in `not_at` rule regarding wrong propagation of errors (issue 3 from Google code hosting).

## 0.31

Released 2011-02

* Fixed bug in `not_at` rule regarding wrong propagation of errors (issue 3 from Google code hosting).

## 0.30

* Fixed missing template arguments in the implementation of `smart_parse_string()`.

## 0.29

* Fixed broken convenience rules `space_until_eof` and `blank_until_eol`.
* Extended the included examples that show how to build parse trees etc.

## 0.28

* Optimised object file footprint of class `printer` and some related functions.
* Renamed class `rule_helper` to `rule_base` and `action_helper` to `action_base`.

## 0.27

* Changed the type of exceptions thrown by the library to `pegtl::parse_error`.
* Changed class `basic_debug` to only generate a grammar back-trace when a `pegtl::parse_error` is flying.
* Changed logging to use a virtual member function on the debug class inherited from common debug base class.
* Removed all `*_parse_*_nothrow()` parse functions.
* Removed the `_throws` substring from all remaining parse functions and changed the return type to `void`.
* Added convenience classes `file_input`, `ascii_file_input` and `dummy_file_input` for custom parse functions.

## 0.26

* Changed pretty-printing of the `until` and `if...` rules (consistency).
* Changed pretty-printing of rules to use ":=" instead of "===" (conciseness).
* Renamed rule `action` to `ifapply` and removed rule `action_nth` (orthogonality).
* Renamed action `apply_nth` to `nth`, and renamed some other actions (consistency).
* Extended pretty-printing to the `apply` and `ifapply` rules (completeness).

The last of these changes effectively requires custom action classes to derive either from a valid rule class, or from the new class `pegtl::action_helper<>`, passing itself as template argument.

## 0.25

* Fixed and cleaned up the rule pretty-printer in many places (readability).
* Added new convenience rule `enclose`, useful for quoted strings (convenience).
* Added new rule `apply` to unconditionally apply an action with empty matched string (convenience).
* Added action argument to `list` rule and added action `nop` for use as default action (convenience).

## 0.24

* Fixed some bugs in the pretty-printer; still in the experimental phase (usability).

## 0.23

* Added new rules `padl` and `padr` (convenience).
* Added example for quoted strings with arbitrary unicode characters (documentation).
* Changed rule `pad` to not suppress the padding in diagnostic messages (consistency).

## 0.22

* Cleaned up the source to compile with `-std=c++0x -pedantic` (compliance).
* Cleaned out some superfluous compiler flags from the Makefile (minimalism).
* Changed the default compiler to `g++`, which can be overriden by `$CXX` (consistency).
* Cleaned up unittests for where `char` is signed but `-fno-strict-overflow` is not given (compliance).
* Removed `list/not_list/at_list/at_not_list`, but `one/not_one/at_one/at_not_one` are now variadic (orthogonality).
* Removed the redundant rules `space_star`, `space_plus`, `blank_star`, and `blank_plus` (minimalism).
* Added new rule class `list` (not to be confused with the old, very different, rule `list`) (convenience).
* Changed class `seq` to invoke the `marker` with a modified `Must` flag for single-rule sequences (performance).
* Changed rule class `until1` to be a specialisation of `until`, rather than have a different name (consistency).
* Changed around the order of the template arguments of the `until` rule (consistency and flexibility).
* Changed around the order of the template arguments of the `rep` rule and reduced to strict repeat (minimalism).
* Changed many rule classes from one template argument to variadic sequence of arguments (flexibility).

## 0.21

* Changed the pretty-printing of rules, this is work in progress (aesthetics).
* Fixed the exception that occurred when `mmap()`ing an empty file (correctness).

## 0.20

* Added the missing `pegtl.hh` header file to the release archive...

## 0.19

* Cleanly layered implementation of `action_nth` (flexibility).
* Renamed class `action_all` back to `action` (was better that way).
* Moved main `pegtl.hh` include file out of `pegtl` directory (simplicity).
* Renamed the rule method from `s_match` to `match` (readability).
* Renamed the action method from `matched` to `apply` (readability).
* Renamed the rule method from `s_insert` to `prepare` (consistency).
* Changed the input iterator classes to report byte offsets (consistency).
* Added rule and action class to match captured sub-expressions (experiment).
* Changed class `action` to invoke arbitrary many actions (succinctness).
* Changed classes `ifmust` and `ifthen` to accept arbitrary many 'then' rules (succinctness).
* Fixed potential dangling reference in helper class `names` (correctness).

## 0.18

* Added parser functions `parse_forward` for forward iterators (completeness).
* Renamed parser functions for input iterators to `parse_input` (consistency).
* Added parser functions `parse_file` for files, implemented with `mmap(2)` (necessity).
* Added initial support for customised logging of error messages (flexibility).

## 0.17

* Added support for ranges of input iterators with automatic minimal buffering (flexibility).

## 0.16

* Added class `action_nth` (flexibility).
* Renamed class `action` to `action_all` (consistency).
* Changed class `marker` to a nop when "must" is true (performance).
* Changed `dummy_debug` to interpret "must" tracking (consistency).
* Fixed typo in name of `PEGTL_IMPURE_OPTIMISATIONS` macro (correctness).
* Made the marker class a sub-class of the input class (simplicity).
* Renamed some of classes named `white`, `space`, or `blank` (consistency).
* Fixed some issues in the R6RS example (CFG to PEG mismatch, only first datum).
* Added missing template arguments to `smart_parse`-functions (correctness).

## 0.15

* Removed some small superfluous functions (less is more).
* Changed the "must" tracking from run-time to compile-time (better?).

## 0.14

* Optimised behaviour of `seq<>` and `string<>` (performance).
* Added detection of division-by-zero to calculator example.
* Removed data source debug tracking from the library (simplicity).
* Removed run-time limits on rule applications and nesting (simplicity).
* Disentangled a couple of header files (maintainability).
* Renamed class `iterator_input` to forward_input (consistency).
* Added class `string_input` to initialise forward_input from a string (convenience).
* Removed template argument Rule to action functor's `matched()` method (simplicity).

## 0.13

* Added more wrapper functions for parsing (convenience).
* Renamed existing wrapper functions for parsing (consistency).
* Added `rewind()` method to class `iterator_input` (indirect).

## 0.12

* Added more directory structure.
* Fixed compile-error in `sexpression.cc` (correctness).

## 0.11

* Fixed back-tracking in class `string` (correctness).
* Fixed order of operands in calculator example (correctness).

## 0.10

* Added Scheme R6RS grammar (example).
* Fixed behaviour at end-of-input (aesthetics).
* Fixed behaviour and use of class `position` (correctness).
* Changed to lazy initialisation of pretty-printer (performance).
* Changed the design of the input and parser classes (flexibility).
* Changed how expression rules provide their printer key (simplicity).

## 0.9

Released 2008

* First public release.

## History

Development of the PEGTL started in November 2007 as an experiment in C++0x.
It is based on ideas from the YARD library by Christopher Diggins.

Copyright (c) 2007-2022 Dr. Colin Hirsch and Daniel Frey
