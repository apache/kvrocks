# Migration Guide

## Version 3.0.0

* The build system needs to be configured for C++17 or later.
* The macro `TAO_PEGTL_NAMESPACE` now defines the fully qualified namespace and was changed from `pegtl` to `tao::pegtl`. When setting `TAO_PEGTL_NAMESPACE`, adapt as needed; in case of doubt, add `tao::` as prefix.
* The 0-based `byte_in_line` was replaced with the 1-based `column`.
* The control class template's `apply()` and `apply0()` must only be visible (e.g. via SFINAE) when an action class template function with the appropriate signature exists. See `tao::pegtl::normal` as example.
* If you have a use-case for [state](Rule-Reference.md#state-s-r-)'s `S::success()` to have an extended signature to get access to the current `apply_mode`, `rewind_mode`, *action*- and *control* class (template), please let us know and we'll re-add this feature.
* The compatibility macros starting with `TAOCPP_PEGTL_` were removed, the corresponding `TAO_PEGTL_`-prefixed macros must be used.
* The compatibility uppercase enumerators were removed, the corresponding lowercase enumerators must be used.
* The compatibility `peek_byte()` member functions were removed, the `peek_uint8()` member functions must be used.
* The compatibility header `changes.hpp` was removed, use the action-based `change_*.hpp` headers.
* The parse tree nodes provide `node->type` (a `std::string_view`) instead of `node->name()`.
* The parse tree nodes provide `node->string_view()` and `node->string()` instead of `node->content()`.

## Version 2.8.0

* All enumerators were changed from uppercase to lowercase.
* The enumerator `tracking_mode::IMMEDIATE` was renamed to `tracking_mode::eager`.
* The input-classes' `peek_byte()` member function was renamed to `peek_uint8()`.

Compatibility enumerators and functions are provided, they will be removed in version 3.0.0.

## Version 2.4.0

* The common prefix for all macros was changed from `TAOCPP_PEGTL_` to `TAO_PEGTL_`.

Compatibility macros are provided, they will be removed in version 3.0.0.

## Version 2.0.0

Some of the [changes](Changelog.md#200) for version 2.0.0 require modifications to any source using the PEGTL.
The good news is that the semantics of all parsing rules and grammars is the same as for versions 1.x.
Existing grammars will continue to work as before once the following list of mostly naming and namespace related changes is taken into account.

* Everything in the PEGTL is now in namespace `tao::pegtl`.
* The file name extensions were changed from `.hh` to `.hpp`.
* The main include file is now `<tao/pegtl.hpp>`, all other include files are `<tao/pegtl/*.hpp>`.
* The distribution of tasks between [the parse functions and input classes](Inputs-and-Parsing.md) was changed.
* The string macros have been renamed from `pegtl_(i)string_t` to `TAOCPP_PEGTL_(I)STRING`.
* The `begin()` member function of the input-classes is now called `current()`.
* The first argument to actions' `apply()` is now of type `tao::pegtl::internal::action_input< ... >`.

For flexibility and future compatibility it is recommended to "template over" the first argument to `apply()` as shown in [Actions and States](Actions-and-States.md#actions).

The `pegtl` part of the namespace can be controlled with the `TAOCPP_PEGTL_NAMESPACE` macro which is set in `include/tao/pegtl/config.hpp` if not defined previously.

Most of the other changes for version 2.0.0 extend or optimise the PEGTL without breaking compatibility.
Applications integrated more tightly with the PEGTL might require additional changes.

Please contact the authors at `taocpp(at)icemx.net` for any further questions when updating the PEGTL.

## Version 1.0.0

There were [many important changes](Changelog.md#100) leading up to version 1.0.0.
Please contact the authors at `taocpp(at)icemx.net` for any further questions when updating the PEGTL.

Copyright (c) 2017-2022 Dr. Colin Hirsch and Daniel Frey
