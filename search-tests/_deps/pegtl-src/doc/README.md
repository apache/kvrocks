# PEGTL Documentation

* [Project](https://github.com/taocpp/PEGTL)
* [Getting Started](Getting-Started.md)
* [Installing and Using](Installing-and-Using.md)
  * [Requirements](Installing-and-Using.md#requirements)
  * [Filesystem](Installing-and-Using.md#filesystem)
  * [Disabling Exceptions](Installing-and-Using.md#disabling-exceptions)
  * [Disabling RTTI](Installing-and-Using.md#disabling-rtti)
  * [Installation Packages](Installing-and-Using.md#installation-packages)
  * [Using Vcpkg](Installing-and-Using.md#using-vcpkg)
  * [Using Conan](Installing-and-Using.md#using-conan)
  * [Using CMake](Installing-and-Using.md#using-cmake)
    * [CMake Installation](Installing-and-Using.md#cmake-installation)
    * [`find_package`](Installing-and-Using.md#find_package)
    * [`add_subdirectory`](Installing-and-Using.md#add_subdirectory)
    * [Mixing `find_package` and `add_subdirectory`](Installing-and-Using.md#mixing-find_package-and-add_subdirectory)
  * [Manual Installation](Installing-and-Using.md#manual-installation)
  * [Embedding the PEGTL](Installing-and-Using.md#embedding-the-pegtl)
    * [Embedding in Binaries](Installing-and-Using.md#embedding-in-binaries)
    * [Embedding in Libraries](Installing-and-Using.md#embedding-in-libraries)
    * [Embedding in Library Interfaces](Installing-and-Using.md#embedding-in-library-interfaces)
  * [Single Header Version](Installing-and-Using.md#single-header-version)
* [Rules and Grammars](Rules-and-Grammars.md)
  * [Combining Existing Rules](Rules-and-Grammars.md#combining-existing-rules)
  * [Toy S-Expression Grammar](Rules-and-Grammars.md#toy-s-expression-grammar)
  * [Creating New Rules](Rules-and-Grammars.md#creating-new-rules)
    * [Simple Rules](Rules-and-Grammars.md#simple-rules)
    * [Complex Rules](Rules-and-Grammars.md#complex-rules)
* [Actions and States](Actions-and-States.md)
  * [Overview](Actions-and-States.md#overview)
  * [Example](Actions-and-States.md#example)
  * [States](Actions-and-States.md#states)
  * [Apply](Actions-and-States.md#apply)
  * [Apply0](Actions-and-States.md#apply0)
  * [Inheriting](Actions-and-States.md#inheriting)
  * [Specialising](Actions-and-States.md#specialising)
  * [Changing Actions](Actions-and-States.md#changing-actions)
    * [Via Rules](Actions-and-States.md#via-rules)
    * [Via Actions](Actions-and-States.md#via-actions)
  * [Changing States](Actions-and-States.md#changing-states)
    * [Via Rules](Actions-and-States.md#via-rules-1)
    * [Via Actions](Actions-and-States.md#via-actions-1)
  * [Changing Actions and States](Actions-and-States.md#changing-actions-and-states)
  * [Match](Actions-and-States.md#match)
  * [Nothing](Actions-and-States.md#nothing)
  * [Backtracking](Actions-and-States.md#backtracking)
  * [Troubleshooting](Actions-and-States.md#troubleshooting)
    * [Boolean Return](Actions-and-States.md#boolean-return)
    * [State Mismatch](Actions-and-States.md#state-mismatch)
  * [Legacy Actions](Actions-and-States.md#legacy-actions)
* [Errors and Exceptions](Errors-and-Exceptions.md)
  * [Global Failure](Errors-and-Exceptions.md#global-failure)
  * [Local to Global Failure](Errors-and-Exceptions.md#local-to-global-failure)
    * [Intrusive Local to Global Failure](Errors-and-Exceptions.md#intrusive-local-to-global-failure)
    * [Non-Intrusive Local to Global Failure](Errors-and-Exceptions.md#non-intrusive-local-to-global-failure)
  * [Global to Local Failure](Errors-and-Exceptions.md#global-to-local-failure)
  * [Examples for Must Rules](Errors-and-Exceptions.md#examples-for-must-rules)
  * [Custom Exception Messages](Errors-and-Exceptions.md#custom-exception-messages)
* [Rule Reference](Rule-Reference.md)
  * [Meta Rules](Rule-Reference.md#meta-rules)
  * [Combinators](Rule-Reference.md#combinators)
  * [Convenience](Rule-Reference.md#convenience)
  * [Action Rules](Rule-Reference.md#action-rules)
  * [Atomic Rules](Rule-Reference.md#atomic-rules)
  * [ASCII Rules](Rule-Reference.md#ascii-rules)
  * [Unicode Rules](Rule-Reference.md#unicode-rules)
    * [ICU Support](Rule-Reference.md#icu-support)
    * [Basic ICU Rules](Rule-Reference.md#basic-icu-rules)
    * [ICU Rules for Binary Properties](Rule-Reference.md#icu-rules-for-binary-properties)
    * [ICU Rules for Enumerated Properties](Rule-Reference.md#icu-rules-for-enumerated-properties)
    * [ICU Rules for Value Properties](Rule-Reference.md#icu-rules-for-value-properties)
  * [Binary Rules](Rule-Reference.md#binary-rules)
  * [Full Index](Rule-Reference.md#full-index)
* [Inputs and Parsing](Inputs-and-Parsing.md)
  * [Tracking Mode](Inputs-and-Parsing.md#tracking-mode)
  * [Line Ending](Inputs-and-Parsing.md#line-ending)
  * [Source](Inputs-and-Parsing.md#source)
  * [File Input](Inputs-and-Parsing.md#file-input)
  * [Memory Input](Inputs-and-Parsing.md#memory-input)
  * [String Input](Inputs-and-Parsing.md#string-input)
  * [Stream Inputs](Inputs-and-Parsing.md#stream-inputs)
  * [Argument Input](Inputs-and-Parsing.md#argument-input)
  * [Parse Function](Inputs-and-Parsing.md#parse-function)
  * [Nested Parsing](Inputs-and-Parsing.md#nested-parsing)
  * [Incremental Input](Inputs-and-Parsing.md#incremental-input)
    * [Buffer Size](Inputs-and-Parsing.md#buffer-size)
    * [Discard Buffer](Inputs-and-Parsing.md#discard-buffer)
    * [Custom Rules](Inputs-and-Parsing.md#custom-rules)
    * [Custom Readers](Inputs-and-Parsing.md#custom-readers)
    * [Buffer Details](Inputs-and-Parsing.md#buffer-details)
  * [Error Reporting](Inputs-and-Parsing.md#error-reporting)
  * [Deduction Guides](Inputs-and-Parsing.md#deduction-guides)
* [Control and Debug](Control-and-Debug.md)
  * [Normal Control](Control-and-Debug.md#normal-control)
  * [Control Functions](Control-and-Debug.md#control-functions)
  * [Exception Throwing](Control-and-Debug.md#exception-throwing)
  * [Advanced Control](Control-and-Debug.md#advanced-control)
  * [Changing Control](Control-and-Debug.md#changing-control)
* [Parse Tree](Parse-Tree.md)
  * [Full Parse Tree](Parse-Tree.md#full-parse-tree)
  * [Partial Parse Tree](Parse-Tree.md#partial-parse-tree)
  * [Transforming Nodes](Parse-Tree.md#transforming-nodes)
  * [Transformer](Parse-Tree.md#transformer)
  * [`tao::pegtl::parse_tree::node`](Parse-Tree.md#taopegtlparse_treenode)
  * [Custom Node Class](Parse-Tree.md#custom-node-class)
  * [Requirements](Parse-Tree.md#requirements)
* [Meta Data and Visit](Meta-Data-and-Visit.md)
  * [Internals](Meta-Data-and-Visit.md#internals)
  * [Rule Type](Meta-Data-and-Visit.md#rule-type)
  * [Sub Rules](Meta-Data-and-Visit.md#sub-rules)
  * [Grammar Visit](Meta-Data-and-Visit.md#grammar-visit)
  * [Grammar Print](Meta-Data-and-Visit.md#grammar-print)
  * [Rule Coverage](Meta-Data-and-Visit.md#rule-coverage)
* [Contrib and Examples](Contrib-and-Examples.md)
  * [Contrib](Contrib-and-Examples.md#contrib)
  * [Examples](Contrib-and-Examples.md#examples)
* [Grammar Analysis](Grammar-Analysis.md)
  * [Running](Grammar-Analysis.md#running)
  * [Example](Grammar-Analysis.md#example)
  * [Requirements](Grammar-Analysis.md#requirements)
  * [Limitations](Grammar-Analysis.md#limitations)
* [Changelog](Changelog.md)
* [Migration Guide](Migration-Guide.md)

# Rule Reference Index

* [`action< A, R... >`](Rule-Reference.md#action-a-r-) <sup>[(meta rules)](Rule-Reference.md#meta-rules)</sup>
* [`alnum`](Rule-Reference.md#alnum) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`alpha`](Rule-Reference.md#alpha) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`alphabetic`](Rule-Reference.md#alphabetic) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`any`](Rule-Reference.md#any) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`any`](Rule-Reference.md#any-1) <sup>[(unicode rules)](Rule-Reference.md#unicode-rules)</sup>
* [`any`](Rule-Reference.md#any-2) <sup>[(binary rules)](Rule-Reference.md#binary-rules)</sup>
* [`apply< A... >`](Rule-Reference.md#apply-a-) <sup>[(action rules)](Rule-Reference.md#action-rules)</sup>
* [`apply0< A... >`](Rule-Reference.md#apply0-a-) <sup>[(action rules)](Rule-Reference.md#action-rules)</sup>
* [`ascii_hex_digit`](Rule-Reference.md#ascii_hex_digit) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`at< R... >`](Rule-Reference.md#at-r-) <sup>[(combinators)](Rule-Reference.md#combinators)</sup>
* [`bidi_class< V >`](Rule-Reference.md#bidi_class-v-) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-enumerated-properties)</sup>
* [`bidi_control`](Rule-Reference.md#bidi_control) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`bidi_mirrored`](Rule-Reference.md#bidi_mirrored) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`binary_property< P >`](Rule-Reference.md#binary_property-p-) <sup>[(icu rules)](Rule-Reference.md#basic-icu-rules)</sup>
* [`binary_property< P, V >`](Rule-Reference.md#binary_property-p-v-) <sup>[(icu rules)](Rule-Reference.md#basic-icu-rules)</sup>
* [`blank`](Rule-Reference.md#blank) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`block< V >`](Rule-Reference.md#block-v-) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-enumerated-properties)</sup>
* [`bof`](Rule-Reference.md#bof) <sup>[(atomic rules)](Rule-Reference.md#atomic-rules)</sup>
* [`bol`](Rule-Reference.md#bol) <sup>[(atomic rules)](Rule-Reference.md#atomic-rules)</sup>
* [`bom`](Rule-Reference.md#bom) <sup>[(unicode rules)](Rule-Reference.md#unicode-rules)</sup>
* [`bytes< Num >`](Rule-Reference.md#bytes-num-) <sup>[(atomic rules)](Rule-Reference.md#atomic-rules)</sup>
* [`canonical_combining_class< V >`](Rule-Reference.md#canonical_combining_class-v-) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-value-properties)</sup>
* [`case_sensitive`](Rule-Reference.md#case_sensitive) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`control< C, R... >`](Rule-Reference.md#control-c-r-) <sup>[(meta rules)](Rule-Reference.md#meta-rules)</sup>
* [`dash`](Rule-Reference.md#dash) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`decomposition_type< V >`](Rule-Reference.md#decomposition_type-v-) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-enumerated-properties)</sup>
* [`default_ignorable_code_point`](Rule-Reference.md#default_ignorable_code_point) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`deprecated`](Rule-Reference.md#deprecated) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`diacritic`](Rule-Reference.md#diacritic) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`digit`](Rule-Reference.md#digit) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`disable< R... >`](Rule-Reference.md#disable-r-) <sup>[(meta rules)](Rule-Reference.md#meta-rules)</sup>
* [`discard`](Rule-Reference.md#discard) <sup>[(meta rules)](Rule-Reference.md#meta-rules)</sup>
* [`east_asian_width< V >`](Rule-Reference.md#east_asian_width-v-) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-enumerated-properties)</sup>
* [`enable< R... >`](Rule-Reference.md#enable-r-) <sup>[(meta-rules)](Rule-Reference.md#meta-rules)</sup>
* [`eof`](Rule-Reference.md#eof) <sup>[(atomic rules)](Rule-Reference.md#atomic-rules)</sup>
* [`eol`](Rule-Reference.md#eol) <sup>[(atomic rules)](Rule-Reference.md#atomic-rules)</sup>
* [`eolf`](Rule-Reference.md#eolf) <sup>[(atomic rules)](Rule-Reference.md#atomic-rules)</sup>
* [`extender`](Rule-Reference.md#extender) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`failure`](Rule-Reference.md#failure) <sup>[(atomic rules)](Rule-Reference.md#atomic-rules)</sup>
* [`forty_two< C... >`](Rule-Reference.md#forty_two-c-) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`full_composition_exclusion`](Rule-Reference.md#full_composition_exclusion) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`general_category< V >`](Rule-Reference.md#general_category-v-) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-enumerated-properties)</sup>
* [`grapheme_base`](Rule-Reference.md#grapheme_base) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`grapheme_cluster_break< V >`](Rule-Reference.md#grapheme_cluster_break-v-) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-enumerated-properties)</sup>
* [`grapheme_extend`](Rule-Reference.md#grapheme_extend) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`grapheme_link`](Rule-Reference.md#grapheme_link) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`hangul_syllable_type< V >`](Rule-Reference.md#hangul_syllable_type-v-) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-enumerated-properties)</sup>
* [`hex_digit`](Rule-Reference.md#hex_digit) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`hyphen`](Rule-Reference.md#hyphen) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`id_continue`](Rule-Reference.md#id_continue) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`id_start`](Rule-Reference.md#id_start) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`identifier_first`](Rule-Reference.md#identifier_first) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`identifier_other`](Rule-Reference.md#identifier_other) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`identifier`](Rule-Reference.md#identifier) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`ideographic`](Rule-Reference.md#ideographic) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`ids_binary_operator`](Rule-Reference.md#ids_binary_operator) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`ids_trinary_operator`](Rule-Reference.md#ids_trinary_operator) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`if_apply< R, A... >`](Rule-Reference.md#if_apply-r-a-) <sup>[(action rules)](Rule-Reference.md#action-rules)</sup>
* [`if_must< R, S... >`](Rule-Reference.md#if_must-r-s-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`if_must_else< R, S, T >`](Rule-Reference.md#if_must_else-r-s-t-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`if_then_else< R, S, T >`](Rule-Reference.md#if_then_else-r-s-t-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`istring< C... >`](Rule-Reference.md#istring-c-) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`join_control`](Rule-Reference.md#join_control) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`joining_group< V >`](Rule-Reference.md#joining_group-v-) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-enumerated-properties)</sup>
* [`joining_type< V >`](Rule-Reference.md#joining_type-v-) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-enumerated-properties)</sup>
* [`keyword< C... >`](Rule-Reference.md#keyword-c-) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`lead_canonical_combining_class< V >`](Rule-Reference.md#lead_canonical_combining_class-v-) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-value-properties)</sup>
* [`line_break< V >`](Rule-Reference.md#line_break-v-) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-enumerated-properties)</sup>
* [`list< R, S >`](Rule-Reference.md#list-r-s-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`list< R, S, P >`](Rule-Reference.md#list-r-s-p-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`list_must< R, S >`](Rule-Reference.md#list_must-r-s-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`list_must< R, S, P >`](Rule-Reference.md#list_must-r-s-p-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`list_tail< R, S >`](Rule-Reference.md#list_tail-r-s-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`list_tail< R, S, P >`](Rule-Reference.md#list_tail-r-s-p-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`logical_order_exception`](Rule-Reference.md#logical_order_exception) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`lower`](Rule-Reference.md#lower) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`lowercase`](Rule-Reference.md#lowercase) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`mask_not_one< M, C... >`](Rule-Reference.md#mask_not_one-m-c-) <sup>[(binary rules)](Rule-Reference.md#binary-rules)</sup>
* [`mask_not_range< M, C, D >`](Rule-Reference.md#mask_not_range-m-c-d-) <sup>[(binary rules)](Rule-Reference.md#binary-rules)</sup>
* [`mask_one< M, C... >`](Rule-Reference.md#mask_one-m-c-) <sup>[(binary rules)](Rule-Reference.md#binary-rules)</sup>
* [`mask_range< M, C, D >`](Rule-Reference.md#mask_range-m-c-d-) <sup>[(binary rules)](Rule-Reference.md#binary-rules)</sup>
* [`mask_ranges< M, C1, D1, C2, D2, ... >`](Rule-Reference.md#mask_ranges-m-c1-d1-c2-d2--) <sup>[(binary rules)](Rule-Reference.md#binary-rules)</sup>
* [`mask_ranges< M, C1, D1, C2, D2, ..., E >`](Rule-Reference.md#mask_ranges-m-c1-d1-c2-d2--e-) <sup>[(binary rules)](Rule-Reference.md#binary-rules)</sup>
* [`mask_string< M, C... >`](Rule-Reference.md#mask_string-m-c-) <sup>[(binary rules)](Rule-Reference.md#binary-rules)</sup>
* [`math`](Rule-Reference.md#math) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`minus< M, S >`](Rule-Reference.md#minus-m-s-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`must< R... >`](Rule-Reference.md#must-r-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`nfc_inert`](Rule-Reference.md#nfc_inert) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`nfd_inert`](Rule-Reference.md#nfd_inert) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`nfkc_inert`](Rule-Reference.md#nfkc_inert) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`nfkd_inert`](Rule-Reference.md#nfkd_inert) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`noncharacter_code_point`](Rule-Reference.md#noncharacter_code_point) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`not_at< R... >`](Rule-Reference.md#not_at-r-) <sup>[(combinators)](Rule-Reference.md#combinators)</sup>
* [`not_one< C... >`](Rule-Reference.md#not_one-c-) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`not_one< C... >`](Rule-Reference.md#not_one-c--1) <sup>[(unicode rules)](Rule-Reference.md#unicode-rules)</sup>
* [`not_one< C... >`](Rule-Reference.md#not_one-c--2) <sup>[(binary rules)](Rule-Reference.md#binary-rules)</sup>
* [`not_range< C, D >`](Rule-Reference.md#not_range-c-d-) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`not_range< C, D >`](Rule-Reference.md#not_range-c-d--1) <sup>[(unicode rules)](Rule-Reference.md#unicode-rules)</sup>
* [`not_range< C, D >`](Rule-Reference.md#not_range-c-d--2) <sup>[(binary rules)](Rule-Reference.md#binary-rules)</sup>
* [`nul`](Rule-Reference.md#nul) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`numeric_type< V >`](Rule-Reference.md#numeric_type-v-) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-enumerated-properties)</sup>
* [`odigit`](Rule-Reference.md#odigit) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`one< C... >`](Rule-Reference.md#one-c-) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`one< C... >`](Rule-Reference.md#one-c--1) <sup>[(unicode rules)](Rule-Reference.md#unicode-rules)</sup>
* [`one< C... >`](Rule-Reference.md#one-c--2) <sup>[(binary rules)](Rule-Reference.md#binary-rules)</sup>
* [`opt< R... >`](Rule-Reference.md#opt-r-) <sup>[(combinators)](Rule-Reference.md#combinators)</sup>
* [`opt_must< R, S...>`](Rule-Reference.md#opt_must-r-s-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`pad< R, S, T = S >`](Rule-Reference.md#pad-r-s-t--s-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`pad_opt< R, P >`](Rule-Reference.md#pad_opt-r-p-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`pattern_syntax`](Rule-Reference.md#pattern_syntax) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`pattern_white_space`](Rule-Reference.md#pattern_white_space) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`plus< R... >`](Rule-Reference.md#plus-r-) <sup>[(combinators)](Rule-Reference.md#combinators)</sup>
* [`posix_alnum`](Rule-Reference.md#posix_alnum) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`posix_blank`](Rule-Reference.md#posix_blank) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`posix_graph`](Rule-Reference.md#posix_graph) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`posix_print`](Rule-Reference.md#posix_print) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`posix_xdigit`](Rule-Reference.md#posix_xdigit) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`print`](Rule-Reference.md#print) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`property_value< P, V >`](Rule-Reference.md#property_value-p-v-) <sup>[(icu rules)](Rule-Reference.md#basic-icu-rules)</sup>
* [`quotation_mark`](Rule-Reference.md#quotation_mark) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`radical`](Rule-Reference.md#radical) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`raise< T >`](Rule-Reference.md#raise-t-) <sup>[(atomic rules)](Rule-Reference.md#atomic-rules)</sup>
* [`range< C, D >`](Rule-Reference.md#range-c-d-) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`range< C, D >`](Rule-Reference.md#range-c-d--1) <sup>[(unicode rules)](Rule-Reference.md#unicode-rules)</sup>
* [`range< C, D >`](Rule-Reference.md#range-c-d--2) <sup>[(binary rules)](Rule-Reference.md#binary-rules)</sup>
* [`ranges< C1, D1, C2, D2, ... >`](Rule-Reference.md#ranges-c1-d1-c2-d2--) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`ranges< C1, D1, C2, D2, ... >`](Rule-Reference.md#ranges-c1-d1-c2-d2---1) <sup>[(unicode rules)](Rule-Reference.md#unicode-rules)</sup>
* [`ranges< C1, D1, C2, D2, ... >`](Rule-Reference.md#ranges-c1-d1-c2-d2---2) <sup>[(binary rules)](Rule-Reference.md#binary-rules)</sup>
* [`ranges< C1, D1, C2, D2, ..., E >`](Rule-Reference.md#ranges-c1-d1-c2-d2--e-) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`ranges< C1, D1, C2, D2, ..., E >`](Rule-Reference.md#ranges-c1-d1-c2-d2--e--1) <sup>[(unicode rules)](Rule-Reference.md#unicode-rules)</sup>
* [`ranges< C1, D1, C2, D2, ..., E >`](Rule-Reference.md#ranges-c1-d1-c2-d2--e--2) <sup>[(binary rules)](Rule-Reference.md#binary-rules)</sup>
* [`rematch< R, S... >`](Rule-Reference.md#rematch-r-s-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`rep< Num, R... >`](Rule-Reference.md#rep-num-r-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`rep_max< Max, R... >`](Rule-Reference.md#rep_max-max-r-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`rep_min< Min, R... >`](Rule-Reference.md#rep_min-min-r-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`rep_min_max< Min, Max, R... >`](Rule-Reference.md#rep_min_max-min-max-r-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`rep_opt< Num, R... >`](Rule-Reference.md#rep_opt-num-r-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`require< Num >`](Rule-Reference.md#require-num-) <sup>[(meta-rules)](Rule-Reference.md#meta-rules)</sup>
* [`s_term`](Rule-Reference.md#s_term) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`segment_starter`](Rule-Reference.md#segment_starter) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`sentence_break< V >`](Rule-Reference.md#sentence_break-v-) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-enumerated-properties)</sup>
* [`seq< R... >`](Rule-Reference.md#seq-r-) <sup>[(combinators)](Rule-Reference.md#combinators)</sup>
* [`seven`](Rule-Reference.md#seven) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`shebang`](Rule-Reference.md#shebang) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`soft_dotted`](Rule-Reference.md#soft_dotted) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`sor< R... >`](Rule-Reference.md#sor-r-) <sup>[(combinators)](Rule-Reference.md#combinators)</sup>
* [`space`](Rule-Reference.md#space) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`star< R... >`](Rule-Reference.md#star-r-) <sup>[(combinators)](Rule-Reference.md#combinators)</sup>
* [`star_must< R, S... >`](Rule-Reference.md#star_must-r-s-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`state< S, R... >`](Rule-Reference.md#state-s-r-) <sup>[(meta rules)](Rule-Reference.md#meta-rules)</sup>
* [`string< C... >`](Rule-Reference.md#string-c-) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`string< C... >`](Rule-Reference.md#string-c--1) <sup>[(unicode rules)](Rule-Reference.md#unicode-rules)</sup>
* [`string< C... >`](Rule-Reference.md#string-c--2) <sup>[(binary rules)](Rule-Reference.md#binary-rules)</sup>
* [`success`](Rule-Reference.md#success) <sup>[(atomic rules)](Rule-Reference.md#atomic-rules)</sup>
* [`TAO_PEGTL_ISTRING( "..." )`](Rule-Reference.md#tao_pegtl_istring--) <sup>[(ascii rules)](Rule-Reference.md#ascii_rules)</sup>
* [`TAO_PEGTL_KEYWORD( "..." )`](Rule-Reference.md#tao_pegtl_keyword--) <sup>[(ascii rules)](Rule-Reference.md#ascii_rules)</sup>
* [`TAO_PEGTL_STRING( "..." )`](Rule-Reference.md#tao_pegtl_string--) <sup>[(ascii rules)](Rule-Reference.md#ascii_rules)</sup>
* [`terminal_punctuation`](Rule-Reference.md#terminal_punctuation) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`three< C >`](Rule-Reference.md#three-c-) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`trail_canonical_combining_class< V >`](Rule-Reference.md#trail_canonical_combining_class-v-) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-value-properties)</sup>
* [`try_catch< R... >`](Rule-Reference.md#try_catch-r-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`try_catch_type< E, R... >`](Rule-Reference.md#try_catch_type-e-r-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`two< C >`](Rule-Reference.md#two-c-) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`unified_ideograph`](Rule-Reference.md#unified_ideograph) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`until< R >`](Rule-Reference.md#until-r-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`until< R, S... >`](Rule-Reference.md#until-r-s-) <sup>[(convenience)](Rule-Reference.md#convenience)</sup>
* [`upper`](Rule-Reference.md#upper) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`uppercase`](Rule-Reference.md#uppercase) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`variation_selector`](Rule-Reference.md#variation_selector) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`white_space`](Rule-Reference.md#white_space) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`word_break< V >`](Rule-Reference.md#word_break-v-) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-enumerated-properties)</sup>
* [`xdigit`](Rule-Reference.md#xdigit) <sup>[(ascii rules)](Rule-Reference.md#ascii-rules)</sup>
* [`xid_continue`](Rule-Reference.md#xid_continue) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>
* [`xid_start`](Rule-Reference.md#xid_start) <sup>[(icu rules)](Rule-Reference.md#icu-rules-for-binary-properties)</sup>

Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
