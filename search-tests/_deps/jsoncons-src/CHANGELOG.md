1.3.0
-----

- Fixed bugs:

    - Git Issue #600: Added "-Wnull-dereference" to CI and worked around some false positives.

    - Git Issue #597: Invalid json schema compiled successfully

    - Git Issue #595: SIGABRT when serialising unmapped enum value

    - Fixed a jmespath issue with parenthesized expressions involving projections (wildcard expressions, 
      the flatten operator, slices and filter expressions) where the right parenthesis did not stop the projection.
      For example, given JSON `{"foo" : [[0, 1], [2, 3]]}`, the JMESPath query `(foo[*])[0]` 
      returned `[0,2]` rather than the correct `[0,1]`.

    - Fixed a `json_encoder` formatting issue when `array_object_line_splits` option set to `line_split_kind::same_line`.

- Implemented new features:

    - JMESPath Lexical Scoping using the new [let expression](https://github.com/jmespath/jmespath.jep/blob/main/proposals/0018-lexical-scope.md)

    - JMESPath evaluation now supports late binding of variables to an initial (global) scope
      via parameters.

    - New `json_options` members `allow_comments` and `allow_trailing_comma`. These options should
      be preferred over using an error handler.


1.2.0 
----- 

- Fixed bugs:

    - Git Issue #453: jsonpath length function with recursive select argument gives wrong result

- Implemented new features:

    - Git issue #556: Support nested JSON to CSV. Add `flat`, `column_mapping`, and `max_nesting_depth` options to `basic_csv_options`

    - Git issue #585: Add `raw_tag()` accessor to `basic_cbor_cursor`. Add functions `begin_object_with_tag`,
    `begin_array_with_tag`, `uint64_value_with_tag` etc. to `basic_cbor_encoder` to support encoding values with
    raw CBOR tags.

    - Git issue #574: Support custom JSON Schema error messages with `errorMessage` keyword. Add 
    `enable_custom_error_message` option to `evaluation_options`.

1.1.0 
-----

- API Changes

    - Reverted changes to `basic_json_parser` API introduced in 1.0.0, cf Git issue #576

- Fixed bugs:

    - Git Issue #554: Made headers self-contained

1.0.0
-----

- API Changes

    - Non-const `basic_json::operator[const string_view_type& key]` no longer 
    returns a proxy type.  The rationale for this change is given in Git Issue 
    #315.  The new behavior for the non-const overload of `operator[](const 
    string_view_type& key)` is to return a reference to the value that is 
    associated with `key`, inserting a default constructed value with the key 
    if no such key already exists, which is consistent with the standard 
    library `std::map` behavior.  The new behavior for the const overload of 
    `operator[](const string_view_type& key)` is to return a const reference 
    to the value that is associated with `key`, returning a const reference to 
    a default constructed value with static storage duration if no such key 
    already exists.  

    - Until 1.0.0, a buffer of text is supplied to `basic_json_parser` with a 
    call to `update()` followed by a call to `parse_some()`.  Once the parser 
    reaches the end of the buffer, additional JSON text can be supplied to the 
    parser with another call to `update()`, followed by another call to 
    `parse_some()`.  See [Incremental parsing (until 
    1.0.0)](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/corelib/basic_json_parser.md#incremental-parsing-until-01790).  
    Since 0.179, an initial buffer of text is supplied to the parse with a 
    call to `set_buffer`, and parsing commences with a call to `parse_some`.  
    The parser can be constructed with a user provided chunk reader to obtain 
    additional JSON text as needed.  See [Incremental parsing (since 
    1.0.0)](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/corelib/basic_json_parser.md#incremental-parsing-since-01790).  

    - enum `bigint_chars_format` is deprecated and replaced by 
    `bignum_format_kind`.  Added `bignum_format` getter and setter functions 
    to `basic_json_options`, and deprecated `bigint_format` getter and setter 
    functions.  Changed default `bignum_format` from 
    `bigint_chars_format::base10` to `bignum_format_kind::raw`.  Rationale: 
    `bigint_chars_format` was misnamed, as it applied to `bigdec` as well as 
    `bigint` numbers, and defaulting to `bigint_chars_format::base10` produced 
    surprising results for users of our lossless number option.  

    - The URI argument passed to the jsonschema ResolveURI function object now 
    included the fragment part of the URI.  

- Fixed bugs:

    - Git Issue #554: [jsonpath] evaluation throws on json containing json_const_pointer

    - Git PR #560: [jmespath] When there are elements and the sum is indeed zero, avg function should return average value returned instead of a null value.

    - Git Issue #561: json_string_reader does not work correctly for empty string or string with all blanks

    - Git Issue #564: Fixed basic_json compare of double and non-numeric string

    - Git Issue #570: Fixed writing fixed number of map value/value pairs using cbor_encoder and msgpack_encoder

    - Fixed a number of issues in `uri::resolve`, used in jsonschema, related to abnormal references,
    particulay ones containing dots in path segments. 

- Removed deprecated classes and functions:

    - The jsonschema function `make_schema`, classes `json_validator` and `validation_output`,
    header file `json_validator.hpp` and example `legacy_jsonschema_examples.cpp`, 
    deprecated in 0.174.0, have been removed.

- Enhancements:

    - Added stream output operator (`<<`) to uri class.

    - Added `basic_json(json_pointer_arg_t, basic_json* j)` constructor to 
    allow a `basic_json` value to contain a non-owning view of another `basic_json`
    value.

    - Added constant `null_arg` so that a null json value can be 
    constructed with
    ```
        json j{jsoncons::null_arg};
    ```

    - Custom jmespath functions are now supported thanks to PR #560

    - jsonschema now understands the 'uri' and 'uri-reference' formats

0.178.0
-------

Defect fixes:

- Fixed issue with `jmespath::join function` through PR #546
 
- Fixed issue with the path for cmake config files through PR #547

- Related to #539, made the basic_json constructor `basic_json(const Allocator&)`  
consistent with `basic_json(json_object_arg_t, const Allocator& alloc = Allocator())`.

- Related to #539, `basic_json` copy construction now applies allocator traits `select_on_container_copy_construction`
to the allocator obtained from `other`. For pmr allocators, this gives a default constructed pmr allocator rather
than a copy of the allocator in `other`.
 
Enhancements:
 
- Improved the implementation of `basic_json` swap. Previously in
some cases it would allocate. 
 
- Improved the implementation of `basic_json` copy assignment. 
Reduced allocations when assigning from array to array and object to
object.

- Documented the rules for `basic_json` allocators
[here](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/corelib/json/allocators.md),
and added numerous tests for conformance to the rules.  

- Added missing `basic_json` constructor

```
basic_json(json_array_arg_t, 
    std::size_t count, const basic_json& value, semantic_tag tag = semantic_tag::none, 
    const Allocator& alloc = Allocator());   
```

0.177.0
-------

Changes

- Removed deprecated functions and type names identified in #487

- Reduced the size of some initial `json_parser` allocations to help with #531

Defect fixes

- Fixed #530 by making `jmespath_expression::evaluate` const

- Fixed #488 related to some standard libraries that don't support strings with stateful allocators.

- Fixed issue identified in PR #532 with `basic_json::compare` function

0.176.0 
-------

Compiler support

- Update to [Supported compilers](https://github.com/danielaparker/jsoncons?tab=readme-ov-file#supported-compilers)
documentation to reflect the compilers that are currently in continuous integration testing. 

- Support for some ancient compilers, in particular g++ 4.8 and 4.9, has been dropped. 

- Accepted pr #519 to support build with with llvm-toolset-7 on CentOS 7. 

- We (and users) have seen some compilation errors with tests of `std::scoped_allocator_adaptor` using our sample stateful allocator `FreeListAllocator`
in versions of clang predating version 11, and versions of g++ predating version 10. We've therefore excluded these tests when testing with
the older compilers.

Enhancements

- `basic_json` now supports using C++ 17 structured binding, so you can write

```
for (const auto& [key, value] : j.object_range())
{
    std::cout << key << " => " << value << std::endl;
}
```

-  Addressed issue #509 of over allocating memory due to alignment constraints through pr #510

jsonschema library defect fixes:

- Fixed issue #520 where enabling format validation resulted in a premature abort to validation

- Addressed issue #521 so that jsonschema now supports big integers

Other defect fixes:

- Resolved #518 about CUDA and int128 and float 128

0.175.0
-------

This release contains two breaking changes to recently added
features.

Change to jsonpath::get function

- The return value for `jsonpath::get` has been changed from a
pointer to the selected JSON value, or null if not found, to a
`std::pair<Json*,bool>`, where the bool component indicates 
whether the get operation succeeded.

Change to new jsonschema classes and functions introduced in 0.174.0:

- The overload of `json_schema<Json>::validate` that takes a callback
must now be passed a lambda that returns `walk_result::advance` or 
`walk_result::abort`. This supports early exit from validation.  

Note that this change does not affect the legacy pre-0.174.0 jsonschema
classes and functions (`make_schema`, `json_validator`). 

Enhancement to jsonschema library:

- New `json_schema` member function `walk` for walking through the schema.

0.174.0 
-------

Defect fixes:

- Fixed issue #499 with `nan_to_str`, `inf_to_str` and `neginf_to_str`

Core library enhancements

- New `json_options` `line_splits` option that addresses issue #490

jsonpath library enhancements

- New function `jsonpath::replace` analagous to `jsonpointer::replace`,
  but for normalized paths. 

jsonschema library enhancements

- The jsonschema extension now supports Drafts 4, 6, 2019-09 and 2020-12
in addition to Draft 07. 

    - New function `make_json_schema` that returns a representation of 
      a compiled JSON Schema document.

    - New class `validation_message`

    - The legacy function `make_schema` and classes `json_validator` and
      `validation_output` remain for backward compatibility, but have been 
      deprecated.

0.173.4
-------

Defect fixes

- Fixed issue #485 where `basic_json` member names did not use `polymorphic_allocator`

- Addressed issue #482 by replacing `static_assert` with runtime exception

0.173.3
-------

Defect fixes:

- Made all member functions of [jsoncons::range](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/corelib/json/range.md) const


0.173.2
-------

- Removed use of deduced return types in jsonpath extension (C++ 14 feature) 

0.173.1
-------

- Fixed issue #473 about `bigint` and `-Werror=stringop-overflow` 

0.173.0
-------

Defect fixes:

- Fixed issue #473 about `bigint` and `-Werror=stringop-overflow` 

- Fixed jmespath issue #471 about `jmespath::search` and `ojson` with `AddressSanitizer` in macos environment
 
- Fixed jsonpointer issue with `json_pointer::parse` for empty string keys (which had implications
for jsonschema `definitions` keyword with empty keys.)

- Fixed jsonschema issue with `definitions` keyword with empty keys

- Fixed jsonschema issue #474 about JSON Schema maximum keyword error message

- Fixed jsonschema issue with `multipleOf` keyword and type integer and `multipleOf` a floating point number

- Fixed jsonschema issue with the behavior of `$id` for rebasing in some keywords, particularly `if`, `then`, and `else`.

Enhancements:

- The `jsonschema::make_schema` functions now support providing a retrieval URI, to initialize the base URI.

0.172.1
-------

Defect fixes:

- Fixed issue #470 concerning jsonpath::make_expression with ec broken 

0.172.0
-------

Defect fixes:

- Fixed issue #469 affecting JSMESPath expressions with terminating CR

Enhancements to jsonpath

- Added `result_option` `sort_descending`.

- Added new classes `basic_json_location`, `basic_path_element`, and `basic_path_node`.

- Added a new function `get` for selecting a single JSON value from a JSON document at
a location represented by a `json_location`.

- Added a new function `remove` for removing a single JSON value from a JSON document at a
location represented by a `json_location`.

- Added member function `select` to `jsonpath_expression`.

- Added member function `select_paths` to `jsonpath_expression`.

- Added member function `update` to `jsonpath_expression`, to support
update-in-place for compiled JSONPath expressions.

Enhancements to jsonpointer:

- Added free functions `to_string` and `to_wstring`.

- Added `append` function to `basic_json_pointer`.

0.171.1
-------

Defect fixes:

- Fixed issue #441 concerning misaligned allocation for string data. 

- Fixed issue #436 concerning overflow warning. 

- Changed internal variable names to avoid shadow warnings

- Fixed signature of `dump_pretty` #438

- Fixed `basic_json_decode_options` constructor #437

Enhancements:

- Improved error messages (with field names) for invalid type mappings when using the
`json_type_traits` convenience macros.

- Support char8_t and u8string for C++ 20

- Added compare for bigint/bigdec/bigfloat as double or exact text match. This makes 
JSON Query work if you use `options.lossless_number(true)` for parsing JSON.

0.171.0
-------

Enhancements:

- `basic_json` supports allocators that automatically propagate,  

    - `std::pmr::polymorphic_allocator`

    - `std::scoped_allocator_adaptor`

- Defines aliases and alias templates for `basic_json` using polymorphic allocators
in the `jsoncons::pmr` namespace.

```
namespace jsoncons { namespace pmr {
    template<class CharT, class Policy>
    using basic_json = jsoncons::basic_json<CharT, Policy, std::pmr::polymorphic_allocator<char>>;

    using json = basic_json<char,sorted_policy>;
    using wjson = basic_json<wchar_t,sorted_policy>;
    using ojson = basic_json<char, order_preserving_policy>;
    using wojson = basic_json<wchar_t, order_preserving_policy>;
}}

```

- Added a class `allocator_set` for holding an allocator for persistent data and an
allocator for temporary allocations.

- Added the ability to pass an `allocator_set` object to functions `basic_json::parse`, 
`decode_json`, `decode_csv`, `decode_bson`, `decode_cbor`, `decode_msgpack`, `decode_ubjson`, 
`encode_json`, `encode_csv`, `encode_bson`, `encode_cbor`, `encode_msgpack`, `encode_ubjson`.

- Added the ability to set a JSON parse error handler through options (rather than
as a separate parameter.)

- Added an error handler `allow_trailing_commas`

Changes:

- For users creating a custom `basic_json` with a user provided `Policy`, the name
`string` in `Policy` must be changed to `member_key`.

- Non-propagating stateful allocators are no longer supported.
Attempting to use a regular stateful allocator will produce a compile error.
Regular stateful allocators must be wrapped with [std::scoped_allocator_adaptor](https://en.cppreference.com/w/cpp/memory/scoped_allocator_adaptor)

- Until 0.171.0, the `basic_json` functions `try_emplace`, `emplace`, and `emplace_back` allowed an allocator argument,
and one was required when using stateful allocators. Since 0.171.0, `try_emplace`, `emplace`, and `emplace_back`
must not be passed an allocator argument, because both `std::pmr::polymorphic_allocator` and `std::scoped_allocator_adaptor`
use uses-allocator construction.   

Note: Non-stateful custom allocators are supported as before.

- The tag type `result_allocator_arg_t` and constant `result_allocator_arg` have been deprecated.

- The `jsonpath::json_replace` function no longer supports an optional `result_options` parameter.

- The order preserving versions of `basic_json`, `ojson` and `wojson`, no longer use indexes to
support search, but rely on sequential search instead. The feedback from users was that the additional 
allocations and overhead wasn't worth any gains, particularly as the number of members in JSON 
objects is typically not large.

Defect fixes

- Fixed issue danielaparker/jsoncons/#430 with `jsonpath::json_replace` 

0.170.2
-------

Defect fixes

- Fixed issue with jsonschema default values (introduced in 0.170.0)

0.170.1
-------

Defect fixes:

- Fixed issue danielaparker/jsoncons/#418 where use of `std::aligned_storage` 
produced a diagnostic that it is deprecated in C++2023.

- Fixed issue danielaparker/jsoncons/#420 where `to_integer_base16` produced
warnings when passed a wide character string.

- Fixed issue danielaparker/jsoncons/#391 where parsing JSON resulted
in temporary allocations from `std::stable_sort`

- Fixed issue danielaparker/jsoncons/#421 where PVS-Studio found vulnerabilities in code.

- Fixed issue danielaparker/jsoncons/#425 where `basic_byte_string::assign` and `basic_byte_string::append` failed to compile

- Fixed issue danielaparker/jsoncons/#426 where enum keyed maps didn't serialize correctly.

0.170.0 
-------

Changed:

- Removed static functions `jsonpath_expression::compile`. These have long been
superceded with function `make_expression`.

Defect fixes:

- Fixed issue danielaparker/jsoncons/#416 where `length` operator failed
for array of length zero.
 
- Fixed issue danielaparker/jsoncons/#411 where an overeager g++ 12.2.0 compiler reported a
spurious `stringop-overflow` warning.

- Fixed issue danielaparker/jsoncons/#410 where `jsoncons::jsonpath::json_location::to_string`
escaped only single quotes.

- Merged PR danielaparker/jsoncons/#406 that fixed multiple float parsing and boolean pretty print 
issues with wjson. This included reverting a change to use `std::from_chars`
in 0.169.0.

Enhancements:

- The jsonpath library now works with stateful allocators. In particular, the `make_expression`,
`json_query` and `json_replace` functions now accept an allocator argument for use in allocating  
memory during expression compilation and evaluation.

- Merged PR danielaparker/jsoncons/#395 that added a `ser_context::end_position()` function.

0.169.0
-------

Defect fixes:

- Fixed issue with boost_interprocess examples not working with gcc,
contributed by [raplonu](https://github.com/raplonu)

- Fixed and enhance basic_json_diagnostics_visitor,
contributed by [ecorm](https://github.com/ecorm)

- Fixed [Visual Studio 2022 Preview 17.4.0 compilation errors](https://github.com/danielaparker/jsoncons/issues/387)

Performance Enhancement:

- Use `std::from_chars` for chars to double conversion when 
supported in GCC and VC.

Enhancements:

- Added a `size()` accessor function to `basic_staj_event`.
If the event type is a `key` or a `string_value` or a `byte_string_value`, 
returns the size of the key or string or byte string value.
If the event type is a `begin_object` or a `begin_array`, returns the size of the object
or array if known, otherwise 0.
For all other event types, returns 0.

- Extended the parsers/cursors/encoder for JSON, CBOR, Msgpack,
BSON, CSV, UBJSON so that they are resettable to new sources/sinks,
contributed by [ecorm](https://github.com/ecorm)

Changes:

- For consistency with library naming conventions, the directory 
`include/jsoncons/json_merge_patch` has been renamed to
`include/jsoncons/mergepatch`, the namespace `json_merge_patch`
to `mergepatch`, and the include file `json_merge_patch.hpp` to 
`mergepatch.hpp`.

0.168.7
-------

Defect fixes:

- In release 0.167.0, the `csv_options::mapping()` accessor
was renamed to `csv_options::mapping_kind()`, and the old name was
deprecated but still useable. That release neglected to 
similarily rename the corresponding mutator, which is now
addressed. 

- Fixed csv parsing issue [Exponential formatted numbers with leading zeros in exponent](https://github.com/danielaparker/jsoncons/issues/365)

- Fixed [Issue with assignment of an object_iterator to const_object_iterator using empty object](https://github.com/danielaparker/jsoncons/issues/372). This issue affects some xcode osx users.

0.168.6
-------

Bug Fix:

- Fixed an issue with the order preserving `ojson` erase function 
that takes two iterator arguments.

Enhancement:

- The `basic_json::erase` function return value, previously void, is now
an iterator following the last removed element,

```
array_iterator erase(const_array_iterator pos);
object_iterator erase(const_object_iterator pos);

array_iterator erase(const_array_iterator first, const_array_iterator last);
object_iterator erase(const_object_iterator first, const_object_iterator last);
```

See [Issue \#363](https://github.com/danielaparker/jsoncons/issues/363)

0.168.5
-------

Issues fixed:

- Fixed issue #355, "Array move constructor of basic_json copies provided array".

0.168.4
-------

Issues fixed:

- Fixed [issue \#352](https://github.com/danielaparker/jsoncons/issues/352)
regarding `json_type_traits` macro failure when number of parameters 
reached 47.

Enhancement:

Increased maximum number of parameters in `json_type_traits` macros
from 50 to 70.

0.168.3
-------

Issues fixed:

- Preseve original error messages when decoding into C++ data structures
(related to [issue \#345](https://github.com/danielaparker/jsoncons/issues/345))

- Fixed [issue \#348](https://github.com/danielaparker/jsoncons/issues/348))
concerning compilation issue on OSx with C++11 

0.168.2
-------

Issues fixed:

- Fixed [issue \#343](https://github.com/danielaparker/jsoncons/issues/343)
concerning segfault using JMESPath with Apple clang version 12.0.0 and
x86_64-apple-darwin19.6.0.

- Fixed [issue \#344](https://github.com/danielaparker/jsoncons/issues/344)
concerning compile error with gcc on archlinux with `-Werror=nonnull`.

0.168.1
-------

Bugs fixed:

- Fixed jsonpath issue of normalized path component not being computed
for expression, [issue \#338](https://github.com/danielaparker/jsoncons/issues/338).

0.168.0
-------

Bugs fixed:

- Fixed [issue \#335](https://github.com/danielaparker/jsoncons/issues/335).
The 0b... notation used in a cbor header file has been replaced with 0x... notation.
The 0b... notation used is only standard compliant with C++14 and later, although
supported in some C++ 11 compilers. 

- Fixed issue with csv automatic number detection discovered while investigating
[issue \#333](https://github.com/danielaparker/jsoncons/issues/333).

Enhancements to jsonpointer extension:

- Support construction of a `json_pointer` from a URI fragment representation of a JSON Pointer.
 
- Support stringifying a `json_pointer` to a URI fragment representation with the `to_uri_fragment` function. 

0.167.1
-------

Bugs fixed:

- Fixed compilation error with clang version 8.0.1, [issue \#328](https://github.com/danielaparker/jsoncons/issues/328).

0.167.0
-------

Bugs fixed:

- Fixed issue with a json_cursor hanging if opened with an empty file 
or string, detected by google/OSS-fuzz. 

- Fixed issue with the unary minus in a JSONPath filter expression,
where an expression such as `$[?-@.key > -42]` would fail to parse.

- Fixed issue with private typedef and Intel C++ Compiler
via [PR \#327](https://github.com/danielaparker/jsoncons/pull/327)

Changes: 

- In the csv extension, the enum name `mapping_kind` has been 
renamed to `csv_mapping_kind`, and the `csv_options.mapping` function
has been renamed to `csv_options.mapping_kind`. The old names have
been deprecated but are still usable.

Enhancements:

- Added support for JSON Merge Patch.

- Added support in JSONPath expressions for '%' (modulus) operator

0.166.0
-------

jsonpath bugs fixed:
 
- Fixed issue with normalized paths produced by JSONPath expressions with filters
 
jsonpath enhancements:
 
- Added support for a parent selector, using the `^' symbol, 
following [jsonpath-plus](https://www.npmjs.com/package/jsonpath-plus). 

- Implemented a number of performance optimizations.

jsonschema enhancements:

- Improved error messages reported when compiling a schema document

- Added a check that the "$schema" keyword, if present, is Draft 7.

0.165.0
-------

Enhancements for bson extension:

- Added semantic tags float128, id, regex, and code to support
bson decode and encode for decimal128, ObjectId, regex,
and Javascript code.

- Support has been added for ObjectId, regex, decimal128 and
the other non-deprecated bson types, as requested in issue
[\#321](https://github.com/danielaparker/jsoncons/issues/321).

- Parsing now checks that the the number of bytes read for a
document, embedded document and array matches the expected total.

0.164.0
-------

Changes to jsonpath:

- The values in the `result_options` bitmask have been changed from

    enum class result_options {value=1, path=2, nodups=4|path, sort=8|path};  (until 0.164.0)

to

    enum class result_options {value=0, nodups=1, sort=2, path=4};            (since 0.164.0)


In practice this means that any combination of these values that includes
`result_options::value` has the same meaning as before, except that
`result_options::value` can now be omitted. And any combination that includes
`result_options::path` but not `result_options::value` has the
same meaning as before.

Enhancements to jsonpath:

- Functions now allow expressions to be passed as arguments, e.g.

    $.books[?(ceil(@.price*100) == 2272)]

- User provided custom functions are now supported

Changes to json_reader and csv_reader:

- The typedefs `json_reader` and `wjson_reader` have been deprecated,
but for backwards compatibility they are still supported.  They 
have been replaced by `json_string_reader` and `wjson_string_reader`
for string sources, and `json_stream_reader` and `wjson_stream_reader`,
for stream sources. 

- The typedefs `csv_reader` and `wcsv_reader` have been deprecated,
but for backwards compatibility they are still supported.  They 
have been replaced by `csv_string_reader` and `wcsv_string_reader`
for string sources, and `csv_stream_reader` and `wcsv_stream_reader`,
for stream sources. 

0.163.3
--------

Bugs fixed:

- Fixed a jsonpath issue where the two overloads of json_query and json_replace
that took a callback function argument used a deprecated typedef in an
SFINAE condition. 

0.163.2
--------

Bugs fixed:

- Fixed a jmespath issue with reusing compiled expressions, see [\#317](https://github.com/danielaparker/jsoncons/issues/317)

0.163.1
--------

Bugs fixed:

- Reversed change made in 0.163.0 to removal of duplicates with the `result_options::nodups`,
reverting to previous behaviour (only consider as duplicates nodes with the exact same path
starting from the root.) That seems to be the consensus.

- Fixed a memory leak in jsoncons::jsonpath::node_set, see [\#314](https://github.com/danielaparker/jsoncons/pull/314),
also related, [\#316](https://github.com/danielaparker/jsoncons/issues/316)

- Fixed some gcc warnings about non-virtual destructors in the jsonpath, jmespath, and
jsonschema extensions by making the destructors protected, see [\#313](https://github.com/danielaparker/jsoncons/pull/313). 
Added the gcc compiler flag `-Wnon-virtual-dtor` for gcc in the `tests/CMakeLists.txt` file.


0.163.0
--------

Bugs fixed:

- Fixed a jsonpath issue with removal of duplicates with the `result_options::nodups`
flag in the case of a union with different paths

0.162.3
--------

- Fixed a sign-compare warning in android builds, [\#309](https://github.com/danielaparker/jsoncons/issues/309) 

0.162.2
--------

- Fixed a sign-compare warning 

0.162.1
--------

- Fixed a [gcc warning with -Wsign-compare](https://github.com/danielaparker/jsoncons/issues/307) 
- `-Wsign-compare` enabled for gcc test builds
- Fixed some PVS-Studio warnings

0.162.0
--------

Enhancements to jsonpointer

- The jsonpointer functions `get`, `add`, `add_if_absent`, and `replace`
have an additonal overload with a `create_if_missing` parameter. If
passed `true`, creates key-object pairs when object keys are missing.

Enhancements to jsonpath

- Improved syntax checking for JSONPath unions
- Simplified function signatures for `make_expression`, `json_query`, and `json_replace`.

Changes:

- The jsonpointer function `insert` has been deprecated and renamed to `add_if_absent`,
for consistency with the other names.

0.161.0
--------

The `jsoncons::jsonpath` extension has been rewritten, see [JSONPath extension revisited](https://github.com/danielaparker/jsoncons/issues/306).

Enhancements to JSONPath extension

- Added a new function `make_expression` for creating a compiled JSONPath expression for later evaluation. 
- The `json_query` and `json_replace` functions now take an optional `result_options` parameter that allows duplicate values (i.e. values with
the same node paths) to be excluded from results, and for results to be sorted in path order.

Changes to `json_query`

- The parameter `result_type` has been replaced by a bitmask type `result_options`.
For backwards compatability, `result_type` has been typedefed to `result_options`,
and the `value` and `path` enumerators are still there. In addition, `result_options`
provides options for excluding duplicates from results, and for results to be sorted in
path order.

- Until 0.161.0, `json_query` was limited to returning an array of results, a copy. 
With 0.161, `json_query` allows the user to provide a binary callback 
that is passed two arguments - the path of the item and a const reference to the 
original item.

- Until 0.161.0, `json_replace` allowed the user to provide a unary callback to replace 
an item in the original JSON with a returned value. This overload is still there, but has
been deprecated. With 0.161, `json_replace` allows the user to provide a binary callback 
that is passed two arguments - the path of the item and a mutable reference to the 
original item.

Changes to supported JSONPath syntax

- Previous versions allowed optionally omitting the '$' representing the root of the 
JSON instance in path selectors. This is no longer allowed. In 0.161.0, all path 
selectors must start with either '$', if relative to the root of the JSON instance, 
or '@', if relative to the current node. E.g. `store.book.0` is not allowed, 
rather, `$store.book.0`.
- Previous versions supported union of completely separate paths, e.g. 
`$..[name.first,address.city]`. 0.161.0 does too, but requires that the 
relative paths `name.first` and `address.city` start with a '@', so the 
example becomes `$..[@.name.first,@.address.city]` .
- Previous versions supported unquoted names with the square bracket notation, 
this is no longer allowed. E.g. `$[books]` is not allowed, rather `$['books']` 
or `$["books"]`.
- Previous versions allowed an empty string to be passed as a path argument 
to `json_query`. This is no longer allowed, a syntax error will be raised.
- In 0.161.0, unquoted names in the dot notation are restricted to digits `0-9`, 
letters `A-Z` and `a-z`, the underscore character `_`, and unicode coded characters 
that are non-ascii.  All others names must be enclosed with single or double quotes. 
In particular, names with hypens (`-`) must be enclosed with single or double quotes. 

Enhancements to JMESPath extension

- Function arity errors are now raised during compilation of the JMESPath expression
rather than during evaluation.

0.160.0
--------

Bugs fixed:

- A C++20 change caused a `basic_json` overloaded operator '==' to be ambiguous 
despite there being a unique best viable function. Fixed.

- When parsing MessagePack buffers, lengths were being incorrectly 
parsed as signed integers. Fixed.

Enhancements:

- Added jsonschema extension that implements the JSON Schema [Draft 7](https://json-schema.org/specification-links.html#draft-7) 
specification for validating input JSON, [\#280](https://github.com/danielaparker/jsoncons/issues/280)

Changes:

- Until 0.160.0, `jsonpointer::flatten`, when applied to an
an empty array or empty object, would produce a flattened value 
of `null` rather than `[]` or `{}`. Since 0.160.0, it will
produce `[]` or `{}`. For example, given `{"bar":{},"foo":[]}`,
the flattened output was {"/bar":null,"/foo":null}, but is now 
`{"/bar":{},"/foo":[]}`. `jsonpointer::unflatten` will now return 
the original object.  

Deprecated function removed:

- The long deprecated `basic_json` function 
`to_string(const basic_json_encode_options<char_type>&, char_allocator_type&) const`
has been removed (replacement is `dump`).

0.159.0
--------

Bugs fixed:

- Fixed clang 11 compile issues [\#284](https://github.com/danielaparker/jsoncons/issues/284)
and  [\#285](https://github.com/danielaparker/jsoncons/issues/285).

Changes:

- In the jsonpointer extension, the type names `json_ptr` and `wjson_ptr` have been deprecated and
renamed to `json_pointer` and `wjson_pointer`. 

Enhancements:

- The json_pointer operators `/=` and `/` now support integers.

- New override for `jsonpath::json_replace` that searches for all values that match a JSONPath expression 
and replaces them with the result of a given function, see [\#279](https://github.com/danielaparker/jsoncons/pull/279)

- New factory function `jmespath::make_expression` to create compiled JMESPath expressions.

0.158.0 
--------

Bugs fixed:

- Fixed compilation error with gcc 11, [\#276](https://github.com/danielaparker/jsoncons/pull/276)
(thanks to Laurent Stacul)

Changes:

- In 0.157.0, the `_NAME_` convenience macros were augmented to allow an optional `mode` parameter 
(`JSONCONS_RDWR` or `JSONCONS_RDONLY`) and three optional function object parameters, `match` (value matches expected), 
`from` (convert from type known to jsoncons) and `into` (convert into type known to jsoncons).
In this release - 0.158.0 - the order of the `from` and `into` function object parameters has been reversed.
If you've provided `from` and `into` function objects as arguments in your json traits convenience macros, you'll
need to reverse their order, or if you've provided just a `from` function argument, you'll need to precede it
with `jsoncons::identity()` (or `std::identity()` if C++20). For the rationale for this change, see
[\#277](https://github.com/danielaparker/jsoncons/issues/277)

- Conversion errors during decode are now reported more consistently as `jsoncons::convert_error`, 
parsing errors remain `jsoncons::ser_error` (`or std::error_code`) as before.

0.157.2
--------

Warnings fixed:

- Fixed C20 deprecated move_iterator access with arrow operator.
- Fixed PVS-Studio warnings

OSS-Fuzz issues fixed:

- Fixed OSS-Fuzz failed throw issue 25891 affecting `decode_ubjson`
and potentially other decode functions. This means that decode 
functions will throw a `ser_error` instead of an `assertion_error`
in the presence of certain kinds of bad data. 

0.157.1
--------

Bugs fixed:

- The macros `JSONCONS_ALL_MEMBER_NAME_TRAITS` and
`JSONCONS_N_MEMBER_NAME_TRAITS` failed at compile time 
when provided with exactly two optional member arguments, 
`JSONCONS_RDWR` followed by a `Match` function object 
(other cases were fine.) This has been fixed.

Change reverted:

- The name change `ser_error` to `codec_error` introduced in
0.157.0 has been reverted back to `ser_error`. Just in case
anybody used it, the name `codec_error` has been typedefed 
to `ser_error`.

0.157.0 
--------

Changes:

- The name `ser_error` has been deprecated and renamed to `codec_error`.

Enhancements:

- The `_NAME_` convenience macros now allow an optional `mode` parameter 
(`JSONCONS_RDWR` or `JSONCONS_RDONLY`) and three function objects, 
`match` (value matches expected), `from` (convert from type known to jsoncons) 
and `into` (convert into type known to jsoncons), [\#267](https://github.com/danielaparker/jsoncons/issues/267)

 0.156.1
---------

Bugs fixed:

- Fixed issue with jsonpath exception raised when querying empty string, [\#270](https://github.com/danielaparker/jsoncons/issues/270)

- Included pull request [\#273](https://github.com/danielaparker/jsoncons/pull/273) that fixes an 
issue with a misnamed macro (`BOOST_HAS_FLOAT128` instead of `JSONCONS_HAS_FLOAT128`) introduced in 0.156.0.

 0.156.0
---------

Bugs Fixed:

- Fixed issue with JSONCONS_N_MEMBER_NAME_TRAITS macro, [\#263](https://github.com/danielaparker/jsoncons/issues/263)

Enhancements:

- New `basic_json(json_const_pointer_arg_t, const basic_json*)` constructor to 
allow `basic_json` values to contain non-owning views of other `basic_json`
values.

- New `deep_copy` function to make a deep copy of a `basic_json` value that
contains non-owning views on other `basic_json` values.

- Reduced memory allocations in the jmespath extension using the new
`basic_json(json_const_pointer_arg_t, const basic_json*)` constructor.

- Support for encoding `std::bitset` into `base16` encoded strings (JSON) and
byte strings (binary formats), and decoding `std::bitset` from integer values,
byte strings and `base16` encoded strings.

- Support 128 bit integer types `__int128` and `unsigned __int128`, if supported 
on the platform.

0.155.1
--------

Enhancements:

- Improved support for `bson_parser` to switch to array parsing 
when the BSON root object is a document but `decode_bson` needs
to convert to an `std::array`, `std::tuple` or `std::pair`. 

0.155.0
--------

Changes:

- The `semantic_tag` enum value `timestamp` has been deprecated. 
It has been replaced by `epoch_second`, `epoch_milli` and `epoch_nano`.
The deprecated `timestamp` value has been aliased to `epoch_second`. 

Enhancements:

- Allow `bson_parser` to switch to array parsing when the root
object is a document but `decode_bson` expects an array. 

- Added `json_type_traits` support for `std::nullptr_t`

- Added `json_type_traits` support for `std::chrono::duration`

- Improved memory efficiency of jmespath extension

- Added function `json_encode_pretty` as the preferred alternative   
to the `json_encode` overload with `indenting::indent` argument.

- Added `basic_json` member function `dump_pretty` as the preferred alternative   
to the `dump` overload with `indenting::indent` argument.

- Generalized the `basic_json` member function `dump` and the functions `encode_json` 
and `encode_csv` to write to any back insertable character container.

- Generalized the `basic_json` function `parse` and the  functions `decode_json` 
and `decode_csv` to read from any contiguous character sequence.

0.154.3
--------

Bugs fixed:

- Fixed g++ compile issue with -Wnoexcept compiler flag, [\#260](https://github.com/danielaparker/jsoncons/issues/260)

- Fixed issue with creating a patch to remove array elements using `json_patch::from_diff`, 
[\#261](https://github.com/danielaparker/jsoncons/issues/261)

- Fixed memory leak issue introduced in 0.154.2

0.154.2
--------

Withdrawn

0.154.1
--------

Bugs fixed:

- Fixed issue with encode_cbor overload for user type input and output stream, 
[\#259](https://github.com/danielaparker/jsoncons/issues/259)

0.154.0
--------

Bugs fixed:

- Fixed issue with escaping special characters in the `jsonpath::flatten` function [\#255](https://github.com/danielaparker/jsoncons/issues/255)
- Added workaround for clang xcode 10 bug in `std::optional` implementation
- Fixed bug in `basic_json` less operator with left hand side `uint64_value` and right hand side `int64_value`

Changes:

- The function name `jsonpointer::insert_or_assign` has been deprecated and renamed to `jsonpointer::add`.
Rationale: consistency with JSON Patch names.
 
- Until 0.154.0, the `position()` member function of `ser_context` was defined for JSON 
name and string events only, and indicated the position of the first character of the 
name or string in the input. Since 0.154.0, the `position()` member function of `ser_context` 
is defined for all JSON parse events, and indicates the position of the character at the beginning
of the event, e.g. '[' for an array, '{' for an object, and '"' for a string. 
[\#256](https://github.com/danielaparker/jsoncons/issues/256)

Enhancements

- Added jmespath extension for [JMESPath](https://jmespath.org/) support, [\#204](https://github.com/danielaparker/jsoncons/issues/204)
- Added `json_type_traits` support for `std::variant`, [\#257](https://github.com/danielaparker/jsoncons/issues/257)

0.153.3
--------

Bug fixes:

- Fixed a bug in jsonpath array slice when the step component is negative
and the start and stop components are omitted, [\#252](https://github.com/danielaparker/jsoncons/issues/252).
jsoncons jsonpath slices now have the same semantics as Python slices
including for negative steps.

- Fixed a bug in jsonpath line/column error reporting when using functions.

0.153.2
--------

Bug fixes:

- Fixed a bug in jsonpath array slice when the end argument is negative, [\#250](https://github.com/danielaparker/jsoncons/issues/250)

Platform:

- Support for QNX Neutrino (thanks to Oleh Derevenko for [\#244](https://github.com/danielaparker/jsoncons/issues/244) 
and [\#245](https://github.com/danielaparker/jsoncons/pull/248)) 
  

0.153.1
--------

Bug fixes for BSON:

- Fixed int32 encoding error in the BSON encoder [\#243](https://github.com/danielaparker/jsoncons/issues/243)
- Fixed issue with default binary subtype when not specified, was 0, now 0x80 (user defined.)

0.153.0
--------

Bug fixes:

- Fixed decode issue with `json_type_traits` defined for `set`, `unordered_set`, `multiset`, 
`unordered_multiset` and `forward_list` [\#242](https://github.com/danielaparker/jsoncons/issues/242)

- Fixed issue with preserving original CBOR semantic tag for CBOR byte strings  
associated with an unknown (to jsoncons) tag.

Enhancements:

- `basic_json::parse`, `decode_json`, `decode_csv`, `decode_bson`, `decode_cbor`,
`decode_msgpack`, and `decode_ubjson` now support reading data from a pair of 
LegacyInputIterators that specify a character or byte sequence.

- `byte_string_view` now has an explicit constructor that allows any contiguous
byte sequence container. 

0.152.0
--------

Bug fixes:

- Fixed compile error when building with Android SDK level less than 21 [\#240](https://github.com/danielaparker/jsoncons/pull/240)

- Fixed bson encode/decode of binary type (wasn't reading/writing subtype.)

Changes:

- `basic_json_compressed_encoder` has been deprecated and renamed to
`basic_compact_json_encoder`. Rationale: consistency with other names.
The typedefs `json_compressed_stream_encoder`, `wjson_compressed_stream_encoder`,
`json_compressed_string_encoder`, and `wjson_compressed_string_encoder` have
been deprecated and renamed to `compact_json_stream_encoder`, 
`compact_wjson_stream_encoder`, `compact_json_string_encoder`, and
`compact_wjson_string_encoder`. 

- The factory function `make_array_iterator()` has been replaced by `staj_array()`.

- The factory function `make_object_iterator()` has been replaced by `staj_object()`.

- The constructors for `json_cursor`, `csv_cursor`, `bson_cursor`, `cbor_cursor`, `msgpack_cursor`, and `ubjson_cursor`
that take a filter argument have been deprecated. Instead filters may be applied to a cursor using the pipe syntax, e.g.

    json_cursor cursor(is);
    auto filtered_c = cursor | filter1 | filter2;

Enhancements:

- Generalized `basic_json(byte_string_arg_t, ...` constructor to accomodate any contiguous byte sequence container,
which is a contiguous container that has member functions `data()` and `size()`, and member type `value_type` with size exactly 8 bits.
Any of the values types `int8_t`, `uint8_t`, `char`, `unsigned char` and `std::byte` (since C++17) are allowed.

- Generalized the functions `decode_bson`, `decode_cbor`, `decode_msgpack` and `decode_ubjson`
to read from any contiguous byte sequence.

- Generalized the `json_visitor` member function `byte_string_value`
to accept any contiguous byte sequence argument. In particular this means that `byte_string_value`
can be called on an encoder with any bytes sequence argument.

- Generalized the functions `encode_bson`, `encode_cbor`, `encode_msgpack` and `encode_ubjson`
to write to any back insertable byte container.
Any of the values types `int8_t`, `uint8_t`, `char`, `unsigned char` and `std::byte` (since C++17) are allowed.

- Generalized the `json_type_traits` for maps to accomodate all key types 
that themselves have json_type_traits defined [\#241](https://github.com/danielaparker/jsoncons/issues/241)

- Unknown CBOR tags preceding a byte string (unknown to jsoncons), 
MessagePack types associated with the MessagePack ext family,
and bson binary subtypes associated with a binary value are now captured. 

- If in `basic_json` `tag()` == `semantic_tag::ext`, the function `ext_tag()` will return a format 
specific tag associated with a byte string value. 

- The `basic_json` constructor with parameter `byte_string_arg_t` now allows constructing a byte string
associated with a format specific tag.  

0.151.1
--------

Bug fixes:

- Fixed `jsoncons::semantic_tag::uri`, `jsoncons::semantic_tag::base64` and `jsoncons::semantic_tag::base64url`
applied to text strings incorrectly encoded into CBOR [\238](https://github.com/danielaparker/jsoncons/issues/238)

0.151.0
--------

Bug fixes:

- Fixed eternal loop in csv parser [\#220](https://github.com/danielaparker/jsoncons/issues/220)

- Fixed JSONPath issue with filter expressions containing regular expressions [\#233](https://github.com/danielaparker/jsoncons/issues/233)

- Fixed OSS-Fuzz failed throw issue in CSV parser [\#232](https://github.com/danielaparker/jsoncons/issues/232)

- Fixed OSS-Fuzz integer-overflow issue in CSV parser [\#231](https://github.com/danielaparker/jsoncons/issues/231)

- Fixed OSS-Fuzz timeout issues [\#230](https://github.com/danielaparker/jsoncons/issues/230)

- Fixed UBJSON issue parsing arrays with end markers [\#229](https://github.com/danielaparker/jsoncons/issues/229)

- Fixed OSS-Fuzz memory allocation issues [\#228](https://github.com/danielaparker/jsoncons/issues/228)

- Fixed OSS-Fuzz stack overflow issues [\#225](https://github.com/danielaparker/jsoncons/issues/225)

- OSS-Fuzz failed throw issue in CBOR parser [\#235](https://github.com/danielaparker/jsoncons/issues/235)

- Msg pack bin8 wrong format [\#237](https://github.com/danielaparker/jsoncons/issues/237)

Changes:

- The cbor_option name `enable_typed_arrays` has been deprecated and
renamed to `use_typed_arrays`. 

- `jsonpointer::unflatten_method` has been deprecated and replaced with `jsonpointer::unflatten_options`.

- The cursor functions named `read` have been deprecated and renamed to `read_to`.

Enhancements:

- Added classes bson_options, msgpack_options, and ubjson_options

- Until this release, only JSON parsing supported a `max_nesting_depth` option. Since this release,
JSON, BSON, CBOR, MessagePack and UBJSON all support a `max_nesting_depth` option for both
parsing and serializing. The default is 1024.

- UBJSON supports a `max_items` option for parsing and serializing. The default is 16,777,216.

0.150.0
--------

Defects fixed:

- Fixed jsonpath issue [Union with spaces](https://cburgmer.github.io/json-path-comparison/results/union_with_spaces.html)

- Fixed jsonpath issue [Bracket notation with empty string](https://cburgmer.github.io/json-path-comparison/results/bracket_notation_with_empty_string.html)

- Fixed jsonpath issue [Bracket notation with empty string doubled quoted](https://cburgmer.github.io/json-path-comparison/results/bracket_notation_with_empty_string_doubled_quoted.html)

Changes:

- The names `basic_json_content_handler` and `basic_default_json_content_handler`
have been deprecated and renamed to `basic_json_visitor` and `basic_default_json_visitor`.
The private visitor functions `do_xxx` have been renamed to `visit_xxx`.
This change should be transparent to most users. 

- The name `staj_event_type::name` has been deprecated and renamed to
`staj_event_type::key`. Rationale: consistency with other names. The old
name is still supported.

- The class `null_ser_context` has been deprecated. For defaults, `ser_context()` 
is now used in place of `null_ser_context()`.

Enhancements:

- Added jsonpointer `unflatten` function

0.149.0
--------

Defects fixed:

- Fixed vs issue (since 0.148.0) with basic_json constructor disambiguation with 
a combination of type tag and `std::initializer_list` arguments.

Non-breaking change:

- For consistency with naming conventions across `json_type_traits` convenience macros,
macro names containing `GETTER_CTOR` now contain `CTOR_GETTER`, e.g. 
`JSONCONS_ALL_GETTER_CTOR_NAME_TRAITS` is now `JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS`.
The old names are still supported.

Enhancements:

- Added jsonpath functions `flatten` and `unflatten` that flatten a json object or array 
to a single depth object of key-value pairs, and unflatten that object back to the original json.

0.148.0
--------

Changes:

- The `json_type_traits` convenience macro names ending in `NAMED_TRAITS` 
  have been deprecated and now end in `NAME_TRAITS`, e.g. the old name 
  `JSONCONS_ALL_GETTER_SETTER_NAMED_TRAITS` is now `JSONCONS_ALL_GETTER_SETTER_NAME_TRAITS`. 
  Rationale: name consistency.

- Fixed some deprecated `json_type_traits` convenience macro names. All
  of the convenience macro names that have ever been deprecated should work.

Enhancements:

- Added overload with leading `temp_allocator_arg_t` parameter to `encode_json`, `encode_bson`, `encode_csv`,
`encode_cbor`, `encode_msgpack` and `encode_ubjson`, which allows the user to supply a custom allocator 
that serialization will use for temporary work areas.

0.147.0
--------

Fixed bugs:

- Fixed an issue with the `jsonpatch_error` class implementation of `what()`, likely related to
  [issue #212 ](https://github.com/danielaparker/jsoncons/issues/212)

Enhancements:

- Added support to the convenience macros `JSONCONS_N_GETTER_CTOR_TRAITS` and `JSONCONS_ALL_GETTER_CTOR_NAMED_TRAITS` 
  for-non mandatory members to be omitted altogether from the serialized JSON. These macros had been overlooked
  when this feature was added to the `_N_` macros in 0.146.0.

- Added jsonpointer function `flatten` to flatten a json object or array into a single depth object of JSONPointer-value pairs.

- Added overload with leading `temp_allocator_arg_t` parameter to `decode_json`, `decode_bson`, `decode_csv`,
`decode_cbor`, `decode_msgpack` and `decode_ubjson`, which allows the user to supply a custom allocator 
that deserialization will use for temporary work areas.

0.146.1
--------

Fixed issue with `json_type_traits` specializations of `std::shared_ptr<T>` and 
`std::unique_ptr<T>` when converting from JSON null.

0.146.0
--------

Changes:

- The name `rename_object_member_filter` has been deprecated and renamed to 
`rename_object_key_filter`

- The `json_content_handler` public function `name` has been deprecated and renamed to `key`.
Rationale: in the future we'll likely support overloads for non-string keys for binary formats

- The `json_content_handler` private virtual function `do_name` has been removed and replaced with `do_key`.
Rationale: in the future we'll likely support overloads for non-string keys for binary formats
  
Enhancements:
 
-  New json_type_traits specialization for `std::shared_ptr<T>` for `T` that is not a polymorphic class,
i.e., does not have any virtual functions 

-  New json_type_traits specialization for `std::unique_ptr<T>` for `T` that is not a polymorphic class  

- For the `_N_` convenience macros that allow some non-mandatory members, the generated 
traits `to_json` function will exclude altogether empty values for `std::shared_ptr` 
and `std::unique_ptr` non-mandatory members, as they do currently for `std::optional`.

0.145.2
--------

Defect fixes:

- Fixed issue with `json_type_traits` specialization for optional 

0.145.1
--------

Bug fixes:

- Fixed issue with jsoncons::optional

0.145.0
--------

Bug fixes:

- Fixed [compiler warning with clang](https://github.com/danielaparker/jsoncons/issues/214)
 
- Fixed [Missing return in typed_array](https://github.com/danielaparker/jsoncons/issues/213) 

Name changes:

- The `json_type_traits` convenience macro names ending in `_DECL` have been shortened 
  by dropping the `_DECL` suffix, e.g. the old name `JSONCONS_N_MEMBER_TRAITS_DECL` is now
  `JSONCONS_N_MEMBER_TRAITS`. The old names are still supported.

Enhancements:

- Includes `json_type_traits` specialization for `std::optional` if C++ 17. `nullopt` values
are mapped to JSON null values. 

- When encoding to JSON, the `json_type_traits` convenience macros will exlude altogether a non-mandatory 
`std::optional` `nullopt` value from the JSON output.

0.144.0
--------

Bug fixes:

Fixed issue [json with preserve_order_policy does not preserve order of elements #210](https://github.com/danielaparker/jsoncons/issues/210)

Implemented feature requests:

- [208](https://github.com/danielaparker/jsoncons/issues/208) Added member function to `basic_json`
```
template <class T>
T as(byte_string_arg_t, semantic_tag hint) const; 
```

0.143.2
--------

Bug fix:

- Fixed bug in destructor of `typed_array` class

Enhancement:

- Improved performance of decoding CBOR typed arrays

0.143.1
--------

Bug fix:

- 0.143.1 fixs a bug in the jsonpath filter < comparison operator that was introduced in 0.143.0 

Enhancements:

- `j.as<int>()`, `j.as<uint64_t>()` etc. supported for binary, octal and hex string values,
in addition to decimal string values.
 
- Includes `json_type_traits` specialization for integer keyed maps, with conversion 
to/from string keys in a `basic_json`. 

- The `json_type_traits` specializations for type `T` generated by the convenience macros now include a specialization of
`is_json_type_traits_declared<T>` with member constant `value` equal `true`.

- New `basic_json` member function `json_type type()`

- New `basic_json` member function `get_value_or` that gets a value as type `T` if available, 
or a default value if not: 

    template <class T,class U>
    T get_value_or(const string_view_type& name, U&& default_value) const; 

`get_value_or` is the preferred alternative to

    template<class T>
    T get_with_default(const string_view_type& name, const T& default_value) const

Note that `get_value_or` requires that the first template parameter `T` be specified explicitly (unlike `get_with_default`)

Changes:

- The basic_json `get_with_default(name)` function, which returned a const reference 
to the value if available and to a json null constant if not,
has been deprecated and renamed to `at_or_null(name)`. Rationale: `get_with_default(name)`
is more like `at(name)` (both return a const reference) than 
`get_with_default(name,default_value)` (which returns a value.) 

- The tag type `bstr_arg_t` has been renamed to `byte_string_arg_t`, and the constant 
`bstr_arg` to `byte_string_arg`.

- The `cbor_content_handler` public member functions `typed_array()` and private virtual functions 
`do_typed_array()` have been moved to `basic_json_content_handler`. The `do_typed_array()`
private virtual functions have been given default implementations. `cbor_content_handler`
has been deprecated.
 
- Replaced Martin Moene's span-lite with simpler implementation until `std::span` is available
(primarily for typed array interface.)  

0.142.0
--------

Defect fixes:

- Fixed csv documentation showing the wrong header file includes 
- Fixed jsonpath issue [json_query() is reordering arrays](https://github.com/danielaparker/jsoncons/issues/200)
- Fixed jsonpath issue with recursive descent reported in 
[Alignment on JSONPath implementations across languages](https://github.com/danielaparker/jsoncons/issues/199)

0.141.0
--------

Name change:

- The names `JSONCONS_ALL_PROPERTY_TRAITS_DECL`, `JSONCONS_N_PROPERTY_TRAITS_DECL`,
`JSONCONS_TPL_ALL_PROPERTY_TRAITS_DECL` and `JSONCONS_N_PROPERTY_TRAITS_DECL`  
have been deprecated and renamed to `JSONCONS_ALL_GETTER_SETTER_TRAITS_DECL`, 
`JSONCONS_N_GETTER_SETTER_TRAITS_DECL`,`JSONCONS_TPL_ALL_GETTER_SETTER_TRAITS_DECL` 
and `JSONCONS_TPL_N_GETTER_SETTER_TRAITS_DECL`. Rationale: naming consistency.  

Enhancements:

- New tag types `json_object_arg_t`, `json_array_arg_t` and `bstr_arg_t`
have been introduced that make it simpler to construct `basic_json` values
of objects, arrays and byte strings.

0.140.0
--------

Changes to the `json_type_traits` convenience macros:

- In [Issue 190](https://github.com/danielaparker/jsoncons/issues/190), Twan Springeling 
raised an issue with the macro `JSONCONS_GETTER_CTOR_TRAITS_DECL`, that it did not come
with "strict" and "non-strict" versions. In this release we introduce new macros, where
`_N_` is mnemonic for the number of mandatory properties provided (possibly zero), and _ALL_ is 
mnemonic for all properties are mandatory:
   
```c++
JSONCONS_N_GETTER_CTOR_TRAITS_DECL(class_name,num_mandatory_params,
                                   getter_name0,
                                   getter_name1,...) // (11)

JSONCONS_ALL_GETTER_CTOR_TRAITS_DECL(class_name,
                                     getter_name0,getter_name1,...) // (12)

JSONCONS_TPL_N_GETTER_CTOR_TRAITS_DECL(num_template_params,
                                       class_name,num_mandatory_params,
                                       getter_name0,getter_name1,...) // (13)

JSONCONS_TPL_ALL_GETTER_CTOR_TRAITS_DECL(num_template_params,
                                         class_name,
                                         getter_name0,getter_name1,...) // (14)

JSONCONS_N_GETTER_CTOR_NAMED_TRAITS_DECL(class_name,num_mandatory_params,
                                         (getter_name0,"name0"),
                                         (getter_name1,"name1")...) // (15)

JSONCONS_ALL_GETTER_CTOR_NAMED_TRAITS_DECL(class_name,
                                          (getter_name0,"name0"),
                                          (getter_name1,"name1")...) // (16)

JSONCONS_TPL_N_GETTER_CTOR_NAMED_TRAITS_DECL(num_template_params,
                                             class_name,num_mandatory_params,
                                             (getter_name0,"name0"),
                                             (getter_name1,"name1")...) // (17)

JSONCONS_TPL_ALL_GETTER_CTOR_NAMED_TRAITS_DECL(num_template_params,
                                               class_name,
                                               (getter_name0,"name0"),
                                               (getter_name1,"name1")...) // (18)
```   
- When the traits generated by e.g. `JSONCONS_N_GETTER_CTOR_TRAITS_DECL` need to pass an argument to a
constructor that is not present in the JSON, they pass a default constructed parameter value.
The legacy macros e.g. `JSONCONS_GETTER_CTOR_TRAITS_DECL` remain, and have the same meaning as before, but are deprecated. 

- With the introduction of the `JSONCONS_POLYMORPHIC_TRAITS_DECL` macro,
and the support for polymorhic types, there comes a problem with the "non-strict" macros
JSONCONS_MEMBER_TRAITS_DECL, JSONCONS_PROPERTY_TRAITS_DECL, JSONCONS_SETTER_GETTER_TRAITS_DECL, etc.,
when a class has non-mandatory properties. Type selection is based on the presence of properties,
but the `json_type_traits` generated by the non-strict macros don't know which properties are mandatory 
and which non-mandatory, hence type selection becomes impossible when members are absent in
the JSON. For this reason, twelve new `_N_` macros have been introduced to allow you to specify which properties
are mandatory (`_N_` is mnemonic for number of mandatory properties):
 
```c++
JSONCONS_N_MEMBER_TRAITS_DECL(class_name,num_mandatory_params,
                              member_name0,member_name1,...)

JSONCONS_TPL_N_MEMBER_TRAITS_DECL(num_template_params,
                                  class_name,num_mandatory_params,
                                  member_name0,member_name1,...)

JSONCONS_N_MEMBER_NAMED_TRAITS_DECL(class_name,num_mandatory_params,
                                    (member_name0,"name0"),
                                    (member_name1,"name1")...)

JSONCONS_TPL_N_MEMBER_NAMED_TRAITS_DECL(num_template_params,
                                        class_name,num_mandatory_params,
                                        (member_name0,"name0"),
                                        (member_name1,"name1")...)

JSONCONS_N_GETTER_CTOR_TRAITS_DECL(class_name,num_mandatory_params,
                                   getter_name0,
                                   getter_name1,...)

JSONCONS_TPL_N_GETTER_CTOR_TRAITS_DECL(num_template_params,
                                       class_name,num_mandatory_params,
                                       getter_name0,getter_name1,...)

JSONCONS_N_GETTER_CTOR_NAMED_TRAITS_DECL(class_name,num_mandatory_params,
                                         (getter_name0,"name0"),
                                         (getter_name1,"name1")...)

JSONCONS_TPL_N_GETTER_CTOR_NAMED_TRAITS_DECL(num_template_params,
                                             class_name,num_mandatory_params,
                                             (getter_name0,"name0"),
                                             (getter_name1,"name1")...)

JSONCONS_N_PROPERTY_TRAITS_DECL(class_name,get_prefix,set_prefix,num_mandatory_params,
                                property_name0,property_name1,...)

JSONCONS_TPL_N_PROPERTY_TRAITS_DECL(num_template_params,
                                    class_name,get_prefix,set_prefix,num_mandatory_params,
                                    property_name0,property_name1,...)

JSONCONS_N_GETTER_SETTER_NAMED_TRAITS_DECL(class_name,num_mandatory_params,
                                          (getter_name0,setter_name0,"name0"),
                                          (getter_name1,setter_name1,"name1")...)

JSONCONS_TPL_N_GETTER_SETTER_NAMED_TRAITS_DECL(num_template_params,
                                               class_name,num_mandatory_params,
                                               (getter_name0,setter_name0,"name0"),
                                               (getter_name1,setter_name1,"name1")...)
``` 
- The legacy non-strict macros remain, and have the same meaning as before, but are deprecated. 
 
- For consistency with the new naming, and to allow the legacy `JSONCONS_GETTER_CTOR_TRAITS_DECL` to
keep the same meaning for backwards compatibility, the `_STRICT_` names are deprecated 
and renamed substituting `_ALL_` for `STRICT`.

See [json_type_traits](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/json_type_traits.md)
for complete documentation of the convenience macros. 

0.139.0
--------

Enhancements:

- New convenience macro for generating `json_type_traits` from getter and setter functions
that will serialize to the stringified property names,
```c++
JSONCONS_PROPERTY_TRAITS_DECL(class_name,get_prefix,set_prefix,
                              property_name0,property_name1,...) 

JSONCONS_STRICT_PROPERTY_TRAITS_DECL(class_name,get_prefix,set_prefix,
                                     property_name0,property_name1,...) 

JSONCONS_TPL_PROPERTY_TRAITS_DECL(num_template_params,
                                  class_name,get_prefix,set_prefix,
                                  property_name0,property_name1,...)   

JSONCONS_TPL_STRICT_PROPERTY_TRAITS_DECL(num_template_params,
                                         class_name,get_prefix,set_prefix,
                                         property_name0,property_name1,...) 
```

- `JSONCONS_POLYMORPHIC_TRAITS_DECL` now specializes `json_type_traits`
for `std::unique_ptr<base_class>` in addition to `std::shared_ptr<base_class>`. 

0.138.0
--------

Defect fix:

- Fixes issue in `csv_parser` parsing a CSV file with `mapping_kind::m_columns`,
  when parsing a quoted string containing a numeric value, processing it as a
  number rather than as a string.  

Changes:

- It is no longer neccessay to place a semicolon after `JSONCONS_TYPE_TRAITS_FRIEND` 

Enhancements:

- New convenience macro for generating `json_type_traits` for polymorphic types,
based on the presence of properties,
```c++
JSONCONS_POLYMORPHIC_TRAITS_DECL(base_class_name,derived_class_name0,derived_class_name1,...) 
```

- The convenience macros `JSONCONS_MEMBER_TRAITS_DECL`, `JSONCONS_STRICT_MEMBER_TRAITS_DECL`,
`JSONCONS_TPL_MEMBER_TRAITS_DECL`, `JSONCONS_TPL_STRICT_MEMBER_TRAITS_DECL`,
`JSONCONS_MEMBER_TRAITS_NAMED_DECL`, `JSONCONS_STRICT_MEMBER_TRAITS_NAMED_DECL`,
`JSONCONS_TPL_MEMBER_TRAITS_NAMED_DECL`, and `JSONCONS_TPL_STRICT_MEMBER_TRAITS_NAMED_DECL`
now allow you to have `const` or `static const` data members that are serialized.

- `basic_csv_encoder` now supports json values that map to multi-valued fields and
json objects where each member is a name-array pair.  

- `basic_csv_parser` and `basic_csv_encoder` now support nan, infinity,
and minus infinity substitution

Deprecated `basic_csv_options` functions removed:

- basic_csv_options& column_names(const std::vector<string_type>&)
- basic_csv_options& column_defaults(const std::vector<string_type>& value)
- column_types(const std::vector<string_type>& value)
(Instead, use the versions that take comma-delimited strings)

0.137.0
--------

Defect fixes:

- Fixes GCC 9.2 warning: �class jsoncons::json_exception� 
  has virtual functions and accessible non-virtual destructor,
  contributed by KonstantinPlotnikov.
    
Enhancements:

- Includes Martin Moene's span-lite to support a C++20-like span in the jsoncons namespace.  

- Includes enhancements to the CBOR encode and decode classes and functions to support the 
  CBOR extension [Tags for Typed Arrays](https://tools.ietf.org/html/draft-ietf-cbor-array-tags-08).
  The implementation uses the span class.

Changes:

- The `json_options` parameter to `precision()` has been changed from `int` to `int8_t`

- The `json_options` parameter to `indent_size()` has been changed from `size_t` to `uint8_t`

- The `csv_options` parameter to `precision()` has been changed from `int` to `int8_t`

- The CSV extension enum name `quote_style_type` has been deprecated and renamed to `quote_style_kind`.

- The CSV extension enum name `mapping_type` has been deprecated and renamed to `mapping_kind`.

- The `do_` virtual functions in `basic_json_content_handler` have been augmented with a `std::error_code`
output parameter, e.g.

    virtual bool do_begin_object(semantic_tag tag, const ser_context& context, std::error_code& ec) = 0;

0.136.1
--------

Defect fixes:

- This version fixes a defect in the `erase` functions for
  the order preserving `basic_json` specializations, in
  particular, for `ojson` (issue 188.) 

0.136.0
--------

For consistency with other names, the names of the convenience macros below
for classes with templates and JSON with given names have been deprecated.

    JSONCONS_MEMBER_TRAITS_NAMED_DECL
    JSONCONS_STRICT_MEMBER_TRAITS_NAMED_DECL
    JSONCONS_TEMPLATE_MEMBER_TRAITS_DECL
    JSONCONS_STRICT_TEMPLATE_MEMBER_TRAITS_DECL
    JSONCONS_TEMPLATE_MEMBER_TRAITS_NAMED_DECL
    JSONCONS_STRICT_TEMPLATE_MEMBER_TRAITS_NAMED_DECL
    JSONCONS_ENUM_TRAITS_NAMED_DECL
    JSONCONS_GETTER_CTOR_TRAITS_NAMED_DECL
    JSONCONS_TEMPLATE_GETTER_CTOR_TRAITS_DECL
    JSONCONS_TEMPLATE_GETTER_CTOR_TRAITS_NAMED_DECL
    JSONCONS_GETTER_SETTER_TRAITS_NAMED_DECL
    JSONCONS_STRICT_GETTER_SETTER_TRAITS_NAMED_DECL
    JSONCONS_TEMPLATE_GETTER_SETTER_TRAITS_NAMED_DECL
    JSONCONS_STRICT_TEMPLATE_GETTER_SETTER_TRAITS_NAMED_DECL

They have been renamed to

    JSONCONS_MEMBER_NAMED_TRAITS_DECL
    JSONCONS_STRICT_MEMBER_NAMED_TRAITS_DECL
    JSONCONS_TPL_MEMBER_TRAITS_DECL
    JSONCONS_STRICT_TPL_MEMBER_TRAITS_DECL
    JSONCONS_TPL_MEMBER_NAMED_TRAITS_DECL
    JSONCONS_STRICT_TPL_MEMBER_NAMED_TRAITS_DECL
    JSONCONS_ENUM_NAMED_TRAITS_DECL
    JSONCONS_GETTER_CTOR_NAMED_TRAITS_DECL
    JSONCONS_TPL_GETTER_CTOR_TRAITS_DECL
    JSONCONS_TPL_GETTER_CTOR_NAMED_TRAITS_DECL
    JSONCONS_GETTER_SETTER_NAMED_TRAITS_DECL
    JSONCONS_STRICT_GETTER_SETTER_NAMED_TRAITS_DECL
    JSONCONS_TPL_GETTER_SETTER_NAMED_TRAITS_DECL
    JSONCONS_STRICT_TPL_GETTER_SETTER_NAMED_TRAITS_DECL

See [json_type_traits](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/json_type_traits.md)

0.135.0
--------

New macros that generate the code to specialize `json_type_traits` from the getter and setter functions
and serialize to given names:

- `JSONCONS_GETTER_SETTER_TRAITS_NAMED_DECL(class_name,(getter_name0,setter_name0,"name0"),(getter_name1,setter_name1,"name1")...)`
- `JSONCONS_STRICT_GETTER_SETTER_TRAITS_NAMED_DECL(class_name,(getter_name0,setter_name0,"name0"),(getter_name1,setter_name1,"name1")...) `

- `JSONCONS_TEMPLATE_GETTER_SETTER_TRAITS_NAMED_DECL(num_template_params,class_name,(getter_name0,setter_name0,"name0"),(getter_name1,setter_name1,"name1")...) `
- `JSONCONS_STRICT_TEMPLATE_GETTER_SETTER_TRAITS_NAMED_DECL(num_template_params,class_name,(getter_name0,setter_name0,"name0"),(getter_name1,setter_name1,"name1")...) `

Support for disabling exceptions

- Support compilation with exceptions disabled

0.134.0
--------

- Fixed [Compilation issue #178](https://github.com/danielaparker/jsoncons/issues/178).

- The name `jsonpointer::address` has been deprecated and renamed
  to `jsonpointer::json_ptr`

- `basic_json::contains` declared `noexcept` 

- Support stateful result and work allocators in `json_decoder`

- Support stateful work allocators in `basic_json_reader`, `basic_json_parser`,
`basic_csv_reader` and `basic_csv_parser`.

0.133.0
--------

- Deprecation warnings should now show for all supported versions of GCC and clang

- New single argument `basic_json::get_value_with_default`, which has default value `basic_json::null()`,
  to replace deprecated `basic_json::get`. 

- `basic_json::get_value_with_default` now returns the default value if a `basic_json` has a null value,
  previously threw.

0.132.1
--------

- Fixed vs2019 warnings

0.132.0
--------

Changes

- The name `JSONCONS_TEMPLATE_STRICT_MEMBER_TRAITS_DECL` has been deprecated and renamed to 
`JSONCONS_STRICT_TEMPLATE_MEMBER_TRAITS_DECL`

- The enum name `jsoncons::chars_format` used in `json_options` and `csv_options` has been deprecated and renamed to
  `float_chars_format`.
- The function name `floating_point_format()` used in `json_options` and `csv_options` has been deprecated and renamed to
  `float_format`.

New macros that serialize to given names (instead of the c++ names)

- `JSONCONS_MEMBER_TRAITS_NAMED_DECL(class_name,(member_name0,"name0"),(member_name1,"name1")...)`
- `JSONCONS_STRICT_MEMBER_TRAITS_NAMED_DECL(class_name,(member_name0,"name0"),(member_name1,"name1")...)`
- `JSONCONS_TEMPLATE_MEMBER_TRAITS_NAMED_DECL(num_template_params,class_name,(member_name0,"name0"),(member_name1,"name1")...)` 
- `JSONCONS_STRICT_TEMPLATE_MEMBER_TRAITS_NAMED_DECL(num_template_params,class_name,(member_name0,"name0"),(member_name1,"name1")...)`
- `JSONCONS_GETTER_CTOR_TRAITS_NAMED_DECL(class_name,(getter_name0,"name0"),(getter_name1,"name1")...) `
- `JSONCONS_ENUM_TRAITS_NAMED_DECL(enum_type_name,(identifier0,"name0"),(identifier1,"name1")...)`

Deprecated features removed

- Constructor `basic_json(double, const floating_point_options&, semantic_tag)` 

0.131.2
--------

Bug fix

- Fixed issue with serializing a C++ standard library container class to a binary format, 
and getting an indefinite length array or map. 

0.131.1
--------

Enhancements

- New non-member functions `make_array_iterator` and `make_object_iterator`
for creating a `staj_array_iterator` and a `staj_object_iterator`
(especially for CSV example)

0.131.0
--------

Changes

- The `decode_csv` and `encode_csv` functions have been moved from `csv_reader.hpp` and `csv_encoder.hpp`
  to `csv.hpp`. Rationale: avoid circular reference with new implementation of `decode_csv`.

Enhancements

- `basic_msgpack_parser` template class rewritten to avoid recursive function calls
- `basic_bson_parser` template class rewritten to avoid recursive function calls
- New `basic_msgpack_cursor` template class with `msgpack_stream_cursor` and `msgpack_bytes_cursor` typedefs
- New `basic_bson_cursor` template class with `bson_stream_cursor` and `bson_bytes_cursor` typedefs
- New `basic_csv_cursor` template class with `csv_cursor` and `wcsv_cursor` typedefs

Bug fix:

- Fixed issue with commented out first line of CSV file and column labels on next line
- Fixed issue with parsing BSON arrays

0.130.0
---------------

Bug fix:

- Fixed issue with `cbor_cursor` not reporting name events and terminal event

Enhancements

- New `basic_cbor_cursor` constructor that takes a filter parameter
- Rewrote `basic_ubjson_parser` template class to avoid recursive function calls
- New `basic_ubjson_cursor` template class with `ubjson_stream_cursor` and `ubjson_bytes_cursor` typedefs

0.129.1
---------------

- Fixed issue with deprecated class used in `decode_bson`, `decode_msgpack`, and `decode_ubjson`

0.129.0
---------------

New features:

- Pull parsers for reporting CBOR parse events, cbor_stream_cursor for streams and cbor_bytes_cursor 
for buffers, have been added.

- Added compile-time deprecation warnings 

Changes:

- The long since deprecated `basic_json` member type `null_type` has been removed.
  Instead, use `jsoncons::null_type`.

- The return type of `key_value::key()` has been changed from `string_view_type` to `key_type`.

- The classes `staj_filter` and `filtered_staj_reader` have been removed, instead, use a 
  lambda expression in the constructor of `json_pull_reader`.

- The names `default_parse_error_handler` and `strict_parse_error_handler` have been deprecated 
  and renamed to `default_json_parsing` and `strict_json_parsing`. Rationale: these apply only 
  to JSON. 

- The typedef'ed names `json_encoder`, `bson_encoder`, `cbor_encoder`,
  `csv_encoder`, `cbor_encoder`, `msgpack_encoder`, and `ubjson_encoder`
  have been deprecated and renamed to `json_stream_encoder`, 
  `bson_stream_encoder`, `cbor_stream_encoder`, `csv_stream_encoder`, 
  `cbor_stream_encoder`, `msgpack_stream_encoder`, and `ubjson_stream_encoder`.
  Rationale: consistency for type names that are different for 
  stream and string or bytes buffer specializations. 

0.128.0
---------------

Changes

- `JSONCONS_NONDEFAULT_MEMBER_TRAITS_DECL` macro name deprecated and replaced with `JSONCONS_STRICT_MEMBER_TRAITS_DECL`
- basic_json function name `get_semantic_tag()` deprecated and replaced with `tag()` 
- staj_reader function name `get_semantic_tag()` deprecated and replaced with `tag()` 

Defect fixes:

- This version fixes a defect in the macros `JSONCONS_MEMBER_TRAITS_DECL` etc. which meant that 
  the generated `json_traits_type` specializations were not compatible with `wchar_t` sources and results.

- This version fixes a defect when the `operator[](const string_view_type& key)` is applied to a non const
`basic_json`, which returns a proxy where the member associated with the key may or may not exist, 
and an accessor declared `noexcept` is called in the case that it doesn't. In particular, the
accessors `is_xxx` and `is<T>` now return `false` in this case.
 
New macros

- `JSONCONS_TEMPLATE_MEMBER_TRAITS_DECL(num_template_params,class_name,member_name1,member_name2,...)`
- `JSONCONS_TEMPLATE_STRICT_MEMBER_TRAITS_DECL(num_template_params,class_name,member_name1,member_name2,...)`
- `JSONCONS_TEMPLATE_GETTER_CTOR_TRAITS_DECL(num_template_params,class_name,getter_name1,getter_name2,...)`

`JSONCONS_TEMPLATE_MEMBER_TRAITS_DECL`, `JSONCONS_TEMPLATE_STRICT_MEMBER_TRAITS_DECL` and `JSONCONS_TEMPLATE_GETTER_CTOR_TRAITS_DECL`
are for specializing `json_type_traits` for template types. The parameter `num_template_params` gives the number of template parameters. 

- `JSONCONS_ENUM_TRAITS_DECL(enum_type_name,value1,value2,...)`

`JSONCONS_ENUM_TRAITS_DECL` allows you to encode and decode an enum type as a string.

0.127.0
---------------

Changes to staj streaming classes

- `json_cursor` no longer has a constructor that accepts a `staj_filter`.
  Use a `filtered_staj_reader` instead.

- The `staj_event` function `as<T>` has been deprecated and renamed to `get<T>`. 

- The `staj_event` function `accept` has been renamed to `read_to`. 

Enhancements

- New macro `JSONCONS_NONDEFAULT_MEMBER_TRAITS_DECL`, that, when decoding to a C++ object, 
  requires all data members declared for the object to be present in the JSON (or other supported
  format.)

- Types that are specialized in `json_type_traits` are no longer required to have a public default
  constructor. In particular, types that are specialized using `JSONCONS_MEMBER_TRAITS_DECL`
  or `JSONCONS_NONDEFAULT_MEMBER_TRAITS_DECL` may have a private default constructor + `JSONCONS_TYPE_TRAITS_FRIEND`, 
  and types that are specialized with `JSONCONS_GETTER_CTOR_TRAITS_DECL` are not required to have a default
  constructor at all.

0.126.0
---------------

Enhancements

- The `json_reader` and `csv_reader` constructors have been generalized to take either a value 
from which a `jsoncons::string_view` is constructible (e.g. std::string), or a value from which 
a `source_type` is constructible (e.g. std::istream). 

- With this enhancement, the convenience typedefs `json_string_reader` and `csv_string_reader` are no 
longer needed, and have been deprecated.

Name change

- The name `json_pull_reader` has been deprecated (still works) and renamed to `json_cursor` 

Changes to bigfloat mapping

In previous versions, jsoncons arrays that contained

- an int64_t or a uint64_t (defines base-2 exponent)
- an int64_t or a uint64_t or a string tagged with `semantic_tag::bigint` (defines the mantissa)

and that were tagged with `semantic_tag::bigfloat`, were encoded into CBOR bigfloats. 
This behaviour has been deprecated.

CBOR bigfloats are now decoded into a jsoncons string that consists of the following parts

- (optional) minus sign
- 0x
- nonempty sequence of hexadecimal digits (defines mantissa)
- p followed with optional minus or plus sign and nonempty sequence of hexadecimal digits (defines base-2 exponent)

and tagged with `semantic_tag::bigfloat` (before they were decoded into a jsoncons array and tagged with `semantic_tag::bigfloat`)

jsoncons strings that consist of the following parts

- (optional) plus or minus sign
- 0x or 0X
- nonempty sequence of hexadecimal digits optionally containing a decimal-point character
- (optional) p or P followed with optional minus or plus sign and nonempty sequence of decimal digits,

and tagged with `semantic_tag::bigfloat` are now encoded into CBOR bignums.

0.125.0
--------

CMake build fix

- The CMAKE_BUILD_TYPE variable is now left alone if already set

Non-breaking changes to lighten `semantic_tag` names

- The `semantic_tag` enum values `big_integer`, `big_decimal`, `big_float` and `date_time` 
  have been deprecated (still work) and renamed to `bigint`, `bigdec`, `bigfloat` and `datetime`.

- The `json_content_handler` functions `big_integer_value`, `big_decimal_value`, `date_time_value`
  and `timestamp_value` have been deprecated (still work.) Calls to these functions should be replaced 
  by calls to `string_value` with `semantic_tag::bigint`, `semantic_tag::bigdec`, and `semantic_tag::datetime`,
  and by calls to `int64_vaue` with `semantic_tag::timestamp`.

- The enum type `big_integer_chars_format` has been deprecated (still works) and renamed to 
  `bigint_chars_format`. 

- The `json_options` modifier `big_integer_format` has been deprecated (still works) and renamed to
  `bigint_format`.

Non-breaking changes to `ser_context`, `ser_error` and `jsonpath_error`

- The function names `line_number` and `column_number` have been deprecated (still work) and
  renamed to `line` and `column`.

0.124.0
--------

- Fixed bug in `json_encoder` with `pad_inside_array_brackets` and `pad_inside_object_braces` options

- Fixed issue with escape character in jsonpath quoted names.

- Fixed pedantic level compiler warnings

- Added doozer tests for CentOS 7.6 and Fedora release 24

- New macro `JSONCONS_GETTER_CTOR_TRAITS_DECL` that can be used to generate the `json_type_traits` boilerplate
from getter functions and a constructor.

0.123.2
--------

defect fix:

- Fixed name of JSONCONS_MEMBER_TRAITS_DECL

0.123.1
--------

jsonpath bug fixes:

- Fixed bug in construction of normalized paths [#177](https://github.com/danielaparker/jsoncons/issues/177). 

- Fixed bug in jsonpath recursive descent with filters which could result in too many values being returned

0.123.0
--------

Warning fix:

- Removed redundant macro continuation character [#176](https://github.com/danielaparker/jsoncons/pull/176)

Non-breaking change to names (old name still works)

- The name JSONCONS_TYPE_TRAITS_DECL has been deprecated and changed to JSONCONS_MEMBER_TRAITS_DECL

Changes to jsonpath:

- jsonpath unions now return distinct values (no duplicates)
- a single dot immediately followed by a left bracket now results in an error (illegal JSONPath)

Enhancements to jsonpath

- Union of completely separate paths are allowed, e.g.

    $[firstName,address.city]

- Names in the dot notation may be single or double quoted

Other enhancements:

- `basic_json` now supports operators `<`, `<=`, `>`, `>=` 

0.122.0
--------

Changes:

- The template parameter `CharT` has been removed from the binary encoders
  `basic_bson_encoder`, `basic_cbor_encoder`, `basic_msgpack_encoder`,
  and `basic_ubjson_encoder`. 

Enhancements:

- Added macro JSONCONS_TYPE_TRAITS_FRIEND

- Generalized the `csv_encode`, `bson_encode`, `cbor_encode`, `msgpack_encode` and `ubjson_encode` functions to convert from any type T that implements `json_type_traits` 

- Generalized the `csv_decode`, `bson_decode`, `cbor_decode`, `msgpack_decode` and `ubjson_decode` functions to convert to any type T that implements `json_type_traits` 

0.121.1
--------

Bug fix:

- Fixed issue with cbor_reader only reading tag values 0 through 23

Name change

- The name `json::semantic_tag()` has been renamed to `json::get_semantic_tag()`

Non-breaking changes (old names still work)

- The name `semantic_tag_type` has been deprecated and renamed to `semantic_tag`
- The names `json_serializer`, `bson_serializer`, `cbor_serializer`, `csv_serializer`, `msgpack_serializer`,
  and `ubjson_serializer` have been deprecated and renamed to `json_encoder`, `bson_encoder`, 
  `cbor_encoder`, `csv_encoder`, `msgpack_encoder`, and `ubjson_encoder`
- The names `bson_buffer_serializer`, `cbor_buffer_serializer`, `msgpack_buffer_serializer`,
  and `ubjson_buffer_serializer` have been deprecated and renamed to `bson_bytes_encoder`, 
  `cbor_bytes_encoder`, `msgpack_bytes_encoder`, and `ubjson_bytes_encoder`
- The names `bson_buffer_reader`, `cbor_buffer_reader`, `msgpack_buffer_reader`,
  and `ubjson_buffer_reader` have been deprecated and renamed to `bson_bytes_reader`, 
  `cbor_bytes_reader`, `msgpack_bytes_reader`, and `ubjson_bytes_reader`

Enhancements

- Cleanup of `encode_json` and `decode_json` functions and increased test coverage
- Rewrote cbor_reader to avoid recursive function call
- CBOR reader supports [stringref extension to CBOR](http://cbor.schmorp.de/stringref)
- New `cbor_options` has `packed_strings` option

0.120.0
--------

Bug fix:

- Fixed issue with `j.as<byte_string_view>()`

Non-breaking changes

- The name `is_json_type_traits_impl` has been deprecated and renamed to `is_json_type_traits_declared`
- The name `serializing_context` has been deprecated and renamed to `ser_context`
- The name `serialization_error` has been deprecated and renamed to `ser_error`

Enhancements

- json `as_byte_string` attempts to decode string values if `semantic_tag_type` is `base64`, `base64url`, or `base16`.

- New macro `JSONCONS_TYPE_TRAITS_DECL` that can be used to generate the `json_type_traits` boilerplate 
for your own types.

- New `basic_json` member function `get_allocator`

0.119.1
--------

Bug fix:

Fixed issue wjson dump() not formatting booleans correctly #174

0.119.0
--------

Name change:

- The name `json_staj_reader` has been deprecated and renamed to `json_pull_reader`

Bug fix:

- Fixed a bug in json function `empty()` when type is `byte_string`.
- Fixed a bug with preserving semantic_tag_type when copying json values of long string type.

Changes:

- Removed deprecated feature `cbor_view`

- CBOR decimal fraction and bigfloat string formatting now consistent with double string formatting

Enhancements:

- json `to_string()` and `to_double()` now work with CBOR bigfloat

- JSONPath operators in filter expressions now work with `big_integer`, `big_decimal`, and `big_float`
  tagged json values

- json `is_number()` function now returns `true` if string value is tagged with `big_integer` or 
  `big_decimal`, or if array value is tagged with `big_float`.

- json `as_string()` function now converts arrays tagged with `big_float` to decimal strings

- json `as_double()` function now converts arrays tagged with `big_float` to double values

0.118.0
--------

New features

- New csv option `lossless_number`. If set to `true`, parse numbers with exponent and fractional parts as strings with
  semantic tagging `semantic_tag_type::big_decimal` (instead of double.) Defaults to `false`.

- A class `jsonpointer::address` has been introduced to make it simpler to construct JSON Pointer addresses

Name change

- The `json_options` name `dec_to_str` has been deprecated and renamed to `lossless_number`.

0.117.0
--------

Deprecated features:

- cbor_view has been deprecated. Rationale: The complexity of supporting and documenting this component 
  exceeded its benefits.

New features

- New `json_options` option `dec_to_str`. If set to `true`, parse decimal numbers as strings with
  semantic tagging `semantic_tag_type::big_decimal` instead of double. Defaults to `false`.

- The `ojson` (order preserving) implementation now has an index to support binary search 
  for retrieval. 

- Added `std::string_view` detection

- jsoncons-CBOR semantic tagging supported for CBOR tags 32 (uri)

Name changes (non-breaking)

- The json options name `bignum_chars_format` has been deprecated and replaced with `big_integer_chars_format`.
- `big_integer_chars_format::integer` (`bignum_chars_format::integer`) has been deprecated and replaced with 
  `big_integer_chars_format::number`
- The `json_options function` `bignum_format` has been deprecated and replaced with `big_integer_format`

Changes to floating-point printing

- If the platform supports the IEEE 754 standard, jsoncons now uses the Grisu3 algorithm for printing floating-point numbers, 
  falling back to a safe method using C library functions for the estimated 0.5% of floating-point numbers that might be rejected by Grisu3.
  The Grisu3 implementation follows Florian Loitsch's [grisu3_59_56](https://www.cs.tufts.edu/~nr/cs257/archive/florian-loitsch/printf.pdf) 
  implementation. If the platform does not support the IEEE 754 standard, the fall back method is used. 

- In previous versions, jsoncons preserved information about the format, precision, and decimal places
  of the floating-point numbers that it read, and used that information when printing them. With the
  current strategy, that information is no longer needed. Consequently, the `floating_point_options`
  parameter in the `do_double_value` and `double_value` functions of the SAX-style interface have
  been removed. 

- The `json` functions `precision()` and `decimal_places()` have been deprecated and return 0
  (as this information is no longer preserved.)

- The constructor `json(double val, uint8_t precision)` has been deprecated.  

- Note that it is still possible to set precision as a json option when serializing.

0.116.0
--------

New features:

- New jsonpath functions `keys` and `tokenize`.

- jsoncons-CBOR data item mappings supported for CBOR tags 33 (string base64url) and 34 (string base64)

0.115.0
--------

New features:

- bson UTC datetime associated with jsoncons `semantic_tag_type::timestamp`

- New traits class `is_json_type_traits_impl` that addresses issues 
  [#133](https://github.com/danielaparker/jsoncons/issues/133) and 
  [#115](https://github.com/danielaparker/jsoncons/issues/115) (duplicates)

- Following a proposal from soberich, jsonpath functions on JSONPath expressions 
  are no longer restricted to filter expressions.

- New jsonpath functions `sum`, `count`, `avg`, and `prod`

- Added `semantic_tag_type::base16`, `semantic_tag_type::base64`, `semantic_tag_type::base64url`

Non-breaking changes:

- The json constructor that takes a `byte_string` and a `byte_string_chars_format` 
  has been deprecated, use a `semantic_tag_type` to supply an encoding hint for a byte string, if any.

- The content handler `byte_string_value` function that takes a `byte_string` and a `byte_string_chars_format` 
  has been deprecated, use a `semantic_tag_type` to supply an encoding hint for a byte string, if any.

Changes:

- The `byte_string_chars_format` parameter in the content handler `do_byte_string_value` function
  has been removed, the `semantic_tag_type` parameter is now used to supply an encoding hint for a byte string, if any.

0.114.0
--------

Bug fixes:

- On Windows platforms, fixed issue with macro expansion of max when 
  including windows.h  (also in 0.113.1)

- Fixed compile issue with `j = json::make_array()` (also in 0.113.2)

Breaking changes to jsoncons semantic tag type names: 

- semantic_tag_type::bignum to semantic_tag_type::big_integer
- semantic_tag_type::decimal_fraction to semantic_tag_type::big_decimal
- semantic_tag_type::epoch_time to semantic_tag_type::timestamp

Non-breaking name changes:

The following names have been deprecated and renamed (old names still work)

- `bignum_value` to `big_integer_value` in `json_content_handler`
- `decimal_value` to `big_decimal_value` in `json_content_handler`
- `epoch_time_value` to `timestamp_value` in `json_content_handler`

- `cbor_bytes_serializer` to `cbor_buffer_serializer`
- `msgpack_bytes_serializer` to `msgpack_buffer_serializer`

- `json_serializing_options` to `json_options`
- `csv_serializing_options` to `csv_options`

- `parse_error` to `serialization_error`

The rationale for renaming `parse_error` to `serialization_error`
is that we need to use error category codes for serializer 
errors as well as parser errors, so we need a more general name
for the exception type. 

Message Pack enhancements

- New `msgpack_serializer` that supports Message Pack bin formats 

- New `msgpack_parser` that supports Message Pack bin formats 

- `encode_msgpack` and `decode_msgpack` have been
  rewritten using `msgpack_serializer` and `msgpack_parser`,
  and also now support bin formats.

New features:

- decode from and encode to the [Universal Binary JSON Specification (ubjson)](http://bsonspec.org/) data format
- decode from and encode to the [Binary JSON (bson)](http://bsonspec.org/) data format

- The cbor, msgpack and ubjson streaming serializers now validate that the expected number of
  items have been supplied in an object or array of pre-determined length.

0.113.0
---------------

Bug fix

- Fixed issue with indefinite length byte strings, text strings, arrays, 
  and maps nested inside other CBOR items (wasn't advancing the
  input pointer past the "break" indicator.) 

Changes

- __FILE__ and __LINE__ macros removed from JSONCONS_ASSERT 
  if not defined _DEBUG (contributed by zhskyy.)

- semantic_tag_type name `decimal` changed to `decimal_fraction`

New CBOR feature

- CBOR semantic tagging of expected conversion of byte strings 
  to base64, base64url and base16 are preserved and respected in JSON 
  serialization (unless overridden in `json_serializing_options`.)

- CBOR semantic tagging of bigfloat preserved with `semantic_tag_type::bigfloat`

- CBOR non text string keys converted to strings when decoding
  to json values

Changes to `json_serializing_options`

New options 

- spaces_around_colon (defaults to `space_after`)
- spaces_around_comma (defaults to `space_after`)
- pad_inside_object_braces (defaults to `false`)
- pad_inside_array_brackets (defaults to `false`)
- line_length_limit (defaults to '120`)
- new_line_chars (for json serialization, defaults to `\n`)

`nan_replacement`, `pos_inf_replacement`, and `neg_inf_replacement` are deprecated (still work)
These have been replaced by

- nan_to_num/nan_to_str
- inf_to_num/inf_to_str
- neginf_to_num/neginf_to_str (default is `-` followed by inf_to_num/inf_to_str)

`nan_to_str`, `inf_to_str` and `neginf_to_str` are also used to substitute back to `nan`, `inf` and `neginf` in the parser. 

- Long since deprecated options `array_array_block_option`,
  `array_object_block_option`, `object_object_block_option` and
  `object_array_block_option` have been removed.

- The names `object_array_split_lines`, `object_object_split_lines`,
  `array_array_split_lines` and `array_object_split_lines` have
  been deprecated (still work) and renamed to `object_array_line_splits`, 
  `object_object_line_splits`, `array_array_line_splits` and `array_object_line_splits`.
  Rationale: consistency with `line_split_kind` name.  

Changes to json_serializer

- Previously the constructor of `json_serializer` took an optional argument to 
  indicate whether "indenting" was on. `json_serializer` now always produces
  indented output, so this argument has been removed.

- A new class `json_compressed_serializer` produces compressed json without 
  indenting.
   
  The jsoncons functions that perform serialization including `json::dump`, 
  `pretty_print` and the output stream operator are unaffected.

0.112.0
--------

Changes to `json_content_handler`

- The function `byte_string_value` no longer supports passing a byte string as

    handler.byte_string_value({'b','a','r'});

(shown in some of the examples.) Instead use

    handler.byte_string_value(byte_string({'b','a','r'}));

(or a pointer to utf8_t data and a size.)

- The function `bignum_value` no longer supports passing a CBOR signum and
  byte string, `bignum_value` now accepts only a string view. If you
  have a CBOR signum and byte string, you can use the bignum class to 
  convert it into a string. 

Name changes (non breaking)

- The name `json_stream_reader` has been deprecated and replaced with `json_staj_reader`.
- The name `stream_event_type` has been deprecated and replaced with `staj_event_type`
- The names `basic_stream_event` (`stream_event`) have been deprecated and replaced with `basic_staj_event` (`staj_event`)
- The names `basic_stream_filter` (`stream_filter`) have been deprecated and replaced with `basic_staj_filter` (`staj_filter`)
(staj stands for "streaming API for JSON, analagous to StAX in XML)

- The `json_parser` function `end_parse` has been deprecated and replaced with `finish_parse`.

Enhancements

- json double values convert to CBOR float if double to float 
  round trips.

- csv_parser `ignore_empty_values` option now applies to
  `m_columns` style json output.

- json_reader and json_staj_reader can be initialized with strings 
  in addition to streams.

Extension of semantic tags to other values

    - The `json_content_handler` functions `do_null_value`, `do_bool_value`,
      `do_begin_array` and `do_begin_object` have been given the
      semantic_tag_type parameter.  

    - New tag type `semantic_tag_type::undefined` has been added
    
    - The `cbor_parser` encodes a CBOR undefined tag to a json null
      value with a `semantic_tag_type::undefined` tag, and the 
      `cbor_serializer` maps that combination back to a CBOR undefined tag. 

Removed:

- Long since deprecated `value()` functions have been removed from `json_content_handler`

0.111.1
--------

Enhancements

- Improved efficiency of json_decoder

- Improved efficiency of json_proxy

- Conversion of CBOR decimal fraction to string uses exponential 
  notation if exponent is positive or if the exponent plus the 
  number of digits in the mantissa is negative.

Bug fix

- Fixed issue with conversion of CBOR decimal fraction to string 
  when mantissa is negative

0.111.0
--------

Bug and warning fixes:

- A case where the json parser performed validation on a string 
  before all bytes of the string had been read, and failed if 
  missing part of a multi-byte byte sequence, is fixed.  

- An issue with reading a bignum  with the pull parser 
`json_stream_reader` (in the case that an integer value 
overflows) has been fixed.

- GCC and clang warnings about switch fall through have 
  been fixed

Non-breaking changes:

- The functions `json::has_key` and `cbor::has_key` have been
  deprecated (but still work) and renamed to `json::contains` 
  and `cbor::contains`. Rationale: consistency with C++ 20
  associative map `contains` function.  

- The json function `as_integer()` is now a template function,

```c++
template <class T = int64_t>
T as_integer();
```

where T can be any integral type, signed or unsigned. The
default parameter is for backwards compatability, but is
a depreated feature, and may be removed in a future version.
Prefer j.as<int64_t>().

- The json functions `is_integer()` and `is_uinteger()`
have been deprecated and renamed to `is_int64()`, `is_uint64()`.
Prefer j.is<int64_t>() and j.is<uint64_t>().

- The json function `as_uinteger()` has been deprecated.
Prefer j.as<uint64_t>().

and `as_uinteger()` have been deprecated and renamed to 
`is_int64()`, `is_uint64()`, `as_int64()` and `as_uint64()`. 

Change to pull parser API:

- The `stream_filter` function `accept` has been changed to
  take a `const stream_event&` and a `const serializing_context&`.   

- `stream_event_type::bignum_value` has been removed. `stream_event`
  now exposes information about optional semantic tagging through
  the `semantic_tag()` function.

Enhancements:

- `j.as<bignum>()` has been enhanced to return a bignum value
if j is an integer, floating point value, or any string that
contains an optional minus sign character followed by a sequence 
of digits. 

- `j.as<T>()` has been enhanced to support extended integer
types that have `std::numeric_limits` specializations. In particular,
it supports GCC `__int128` and `unsigned __int128` when code is 
compiled with `std=gnu++NN`, allowing a `bignum` to be returned as 
an `__int128` or `unsigned __int128`. (when code is compiled with 
`-std=c++NN`, `__int128` and `unsigned __int128` do not have 
`std::numeric_limits` specializations.)

New feature:

This release accomodate the additional semantics for the 
CBOR data items date-time (a string), and epoch time (a positive or
negative integer or floating point value), and decimal fraction
(converted in the jsoncons data model to a string).

But first, some of the virtual functions in `json_content_handler` 
have to be modified to preserve these semantics. Consequently, 
the function signatures
  
    bool do_int64_value(int64_t, const serializing_context&)

    bool do_uint64_value(uint64_t, const serializing_context&)

    bool do_double_value(double, const floating_point_options&, const serializing_context&)

    bool do_string_value(const string_view_type&, const serializing_context&)
  
    bool do_byte_string_value(const uint8_t*, size_t, const serializing_context&)

have been given an additonal parameter, a `semantic_tag_type`, 
  
    bool do_int64_value(int64_t, semantic_tag_type, const serializing_context&)

    bool do_uint64_value(uint64_t, semantic_tag_type, const serializing_context&)

    bool do_double_value(double, const floating_point_options&, semantic_tag_type, const serializing_context&)

    bool do_string_value(const string_view_type&, semantic_tag_type, const serializing_context&)
  
    bool do_byte_string_value(const uint8_t*, size_t, semantic_tag_type, const serializing_context&)

For consistency, the virtual function

    bool do_bignum_value(const string_view_type&, const serializing_context&)

has been removed, and in its place `do_string_value` will be called
with semantic_tag_type::bignum_type.

For users who have written classes that implement all or part of 
`json_content_handler`, including extensions to `json_filter`, 
these are breaking changes. But otherwise users should be unaffected.

0.110.2
--------

Continuous integration:

- jsoncons is now cross compiled for ARMv8-A architecture on Travis using 
  clang and executed using the emulator qemu.

- UndefinedBehaviorSanitizer (UBSan) is enabled for selected gcc and clang builds.

Maintenance:

- Removed compiler warnings

0.110.1
--------

Bug fixes contributed by Cebtenzzre

- Fixed a case where `as_double()`, `as_integer()` etc on a `basic_json` 
  value led to an infinite recursion when the value was a bignum 

- Fixed undefined behavior in bignum class

0.110.0
--------

### New features

- A JSON pull parser, `json_stream_reader`, has been added. This 
  required a change to the `json_content_handler` function signatures,
  the return values has been changed to bool, to indicate whether 
  parsing is to continue. (An earlier version on master was
  called `json_event_reader`.)    

- `json_parser` has new member function `stopped()`. 

- The `json::is` function now supports `is<jsoncons::string_view>()`,
  and if your compiler has `std::string_view`, `is<std::string_view>()`
  as well. It returns `true` if the json value is a string, otherwise
  `false`.

- The `json::as` function now supports `as<jsoncons::string_view>()`,
  and if your compiler has `std::string_view`, `as<std::string_view>()`
  as well.

### Changes to `json_content_handler` and related streaming classes

#### Non-breaking changes

These changes apply to users that call the public functions defined by
`json_content_handler`, e.g. begin_object, end_object, etc., but are
non-breaking because the old function signatures, while deprecated,
have been preserved. Going forward, however, users should remove
calls to `begin_document`, replace `end_document` with `flush`,
and replace `integer_value` and `end_integer_value` with `int64_value`
and `uint64_value`.

- The public functions defined by `json_content_handler` have been changed 
  to return a bool value, to indicate whether parsing is to continue.

- The function names `integer_value` and `uinteger_value` have been 
  changed to `int64_value` and `uint64_value`.

- The function names `begin_document` and `end_document` have been 
  deprecated. The deprecated `begin_document` does nothing, and the 
  deprecated `end_document` calls `do_flush`.

- The function `flush` has been added, which calls `do_flush`.

- The `json` member function `dump_fragment` has been deprecated, as with
  the dropping of `begin_document` and `end_document`, it is now
  equivalent to `dump`. 

- The function `encode_fragment` has been deprecated, as with
  the dropping of `begin_document` and `end_document`, it is now
  equivalent to `encode_json`. 

- The `json_filter` member function `downstream_handler` has been
  renamed to `destination_handler`.

#### Breaking changes

These changes will affect users who have written classes that implement
all or part of `json_content_handler`, including extensions to `json_filter`.

- The virtual functions defined for `json_content_handler`, `do_begin_object`,
  `do_end_object`, etc. have been changed to return a bool value, to indicate 
  whether serializing or deserializing is to continue. 

- The virtual functions `do_begin_document` and `do_end_document` have been removed.
  A virtual function `do_flush` has been added to allow producers of json events to 
  flush whatever they've buffered.

- The function names `do_integer_value` and `do_uinteger_value` have been changed to 
  `do_int64_value` and `do_uint64_value`.

- The signature of `do_bignum_value` has been changed to 
```
    bool do_bignum_value(const string_view_type& value, 
                         const serializing_context& context)
```

0.109.0
--------

Enhancements

- Added byte string formatting option `byte_string_chars_format::base16`

Changes

- Scons dropped as a build system for tests and examples, use CMake

- Tests no longer depend on boost, boost test framework replaced by Catch2.
  boost code in tests moved to `examples_boost` directory.

- Previously, if `json_parser` encountered an unopened object or array, e.g. "1]",
  this would cause a JSONCONS_ASSERT failure, resulting in an `std::runtime_error`. This has been
  changed to cause a `json_parse_errc::unexpected_right_brace` or `json_parse_errc::unexpected_right_bracket` error code. 

Warning fixes

- Eliminated vs2017 level 3 and level 4 warnings

0.108.0
--------

Enhancements

- `bignum_chars_format::base64` is supported

- The incremental parser `json_parser` has been documented

Changes (non-breaking)

- Previously, jsonpointer::get returned values (copies)
  Now, jsonpointer::get returns references if applied to `basic_json`, and values if applied to `cbor_view`

- `bignum_chars_format::string` has been deprecated (still works) and replaced with `bignum_chars_format::base10`

- `json_parser_errc`, `cbor_parser_errc`, and `csv_parser_errc` have been deprecated (still work) and renamed to 
  `json_parse_errc`, `cbor_parse_errc`, and `csv_parse_errc`

0.107.2
--------

Bug fixes:

- Fixed issue with `UINT_MAX` not declared in `bignum.hpp`

0.107.1
--------

Bug fixes:

- Fixed issue with cbor_view iterators over indefinite length arrays and maps 

Enhancements:

- csv_serializer recognizes byte strings and bignums.

0.107.0
--------

Enhancements

- Support for CBOR bignums
- Added json serializing options for formatting CBOR bignums as integer, string, or base64url encoded byte string
- Added json serializing options for formatting CBOR bytes strings as base64 or base64url
- Enhanced interface for `cbor_view` including `dump`, `is<T>`, and `as<t>` functions 

Changes

- If the json parser encounters an integer overflow, the value is now handled as a bignum rather than a double value.

- The `json_content_handler` names `begin_json` and `end_json` have been 
  deprecated and replaced with `begin_document` and `end_document`, and the 
  names `do_begin_json` and `do_end_json` have been removed and replaced with 
  `do_begin_document`, and `do_end_document`. 
  Rationale: meaningfullness across JSON and other data formats including
  CBOR.

Bug fixes:

- Fixed bug in base64url encoding of CBOR byte strings
- Fixed bug in parsing indefinite length CBOR arrays and maps

0.106.0
--------

Changes

- If a fractional number is read in in fixed format, serialization now preserves
  that fixed format, e.g. if 0.000071 is read in, serialization gives 0.000071
  and not 7.1e-05. In previous versions, the floating point format, whether
  fixed or scientific, was determined by the behavior of snprintf using the g
  conversion specifier.

Bug fix:

- Fixed issue with parsing cbor indefinite length arrays and maps

Warning fix:

- Use memcpy in place of reinterpret_cast in binary data format utility 
  `from_big_endian`

Compiler fix:

- Fixed issues with g++ 4.8

0.105.0
--------

Enhancements

- The CSV extension now supports multi-valued fields separated by subfield delimiters

- New functions `decode_json` and `encode_json` convert JSON 
  formatted strings to C++ objects and back. These functions attempt to 
  perform the conversion by streaming using `json_convert_traits`, and if
  streaming is not supported, fall back to using `json_type_traits`. `decode_json` 
  and `encode_json` will work for all types that have `json_type_traits` defined.

- The json::parse functions and the json_parser and json_reader constructors 
  optionally take a json_serializing_options parameter, which allows replacing
  a string that matches nan_replacement(), pos_inf_replacement(), and neg_inf_replacement(). 

Changes to Streaming

- The `basic_json_input_handler` and `basic_json_output_handler` interfaces have been 
  combined into one class `basic_json_content_handler`. This greatly simplifies the
  implementation of `basic_json_filter`. Also, the name `parsing_context` has been 
  deprecated and renamed to `serializing_context`, as it now applies to both 
  serializing and deserializing.

  If you have subclassed `json_filter` or have fed JSON events directlty to a 
  `json_serializer`, you shouldn't have to make any changes. In the less likely
  case that you've implemented the `basic_json_input_handler` or 
  `basic_json_output_handler` interfaces, you'll need to change that to
  `json_content_handler`.

Other Changes

- `serialization_traits` and the related `dump` free functions have been deprecated,
  as their functionality has been subsumed by `json_convert_traits` and the
  `encode_json` functions. 

- The option bool argument to indicate pretty printing in the `json` `dump` functions 
  and the `json_serializer` class has been deprecated. It is replaced by the enum class 
  `indenting` with enumerators `indenting::no_indent` and `indenting::indent`.

- The name `serialization_options` has been deprecated (still works) and renamed to 
  `json_serializing_options`. Rationale: naming consistency.

- The `json_reader` `max_nesting_depth` getter and setter functions have been deprecated.
  Use the `json_serializing_options` `max_nesting_depth` getter and setter functions instead.

- The name `csv_parameters` has been deprecated (still works) and renamed to 
  `csv_serializing_options`. Rationale: naming consistency.

0.104.0
--------

Changes

- `decode_csv` by default now attempts to infer null, true, false, integer and floating point values
  in the CSV source. In previous versions the default was to read everything as strings,
  and other types had to be specified explicitly. If the new default behavior is not desired, the
  `csv_parameters` option `infer_types` can be set to `false`. Column types can still be set explicitly
  if desired.

0.103.0
--------

Changes

- Default `string_view_type` `operator std::basic_string<CharT,Traits,Allocator>() const` made explicit
  to be consistent with `std::string_view`

- The virtual method `do_double_value` of `json_input_handler` and `json_output_handler` takes a `number_format` parameter

Performance improvements

- Faster json dump to string (avoids streams)
- Faster floating point conversions for linux and MacOSX
- Memory allocation decoding larger string values reduced by half 
- Optimization to json_parser parse_string 
- Improvements to json_decoder

0.102.1
--------

Bug fix:

Fixed an off-by-one error that could lead to an out of bounds read. Reported by mickcollyer (issue #145)

0.102.0
--------

Bug fixes:

Fixed issue with how jsonpath filters are applied to arrays in the presence of recursion, resulting in
duplicate results.

Changes:

The signatures of `jsonpointer::get`, `jsonpointer::insert`, `jsonpointer::insert_or_assign`, 
`jsonpointer::remove` and `jsonpointer::replace` have been changed to be consistent
with other functions in the jsoncons library. Each of these functions now has two overloads,
one that takes an `std::error_code` parameter and uses it to report errors, and one that 
throws a `jsonpointer_error` exception to report errors.

The function `jsonpatch::patch` has been replaced by `jsonpatch::apply_patch`, which takes
a json document, a patch, and a `std::error_code&` to report errors. The function
`jsonpatch::diff` has been renamed to `jsonpatch::from_diff`

The old signatures for `encode_cbor` and `encode_msgpack` that returned a `std::vector<uint8_t>` 
have been deprecated and replaced by new signatures that have void return values and have
an output parameter 'std::vector<uint8_t>&'. The rationale for this change is consistency
with other functions in the jsoncons library.

0.101.0
--------

Fixes:

- Fixes to `string_view` code when `JSONCONS_HAS_STRING_VIEW` is defined in `jsoncons_config.hpp` 

Changes:

- `as_double` throws if `json` value is null (previously returned NaN)

Enhancements:

- Added convenience functions `decode_csv` and `encode_csv`
- Support custom allocaor (currently stateless only) in `json_decoder`, `json_reader`, 
  `csv_reader`, `csv_parameters`

0.100.2
-------

Resolved warnings on GCC Issue #127

0.100.1
-------

Fix for platform issue with vs2017:

- Renamed label `minus` to `minus_sign` in `json_parser.hpp` 

Enhancements:

- New classes `byte_string` and `byte_string_view`, to augment support for cbor byte strings in `json` values

0.100.0
-------

Changes:

- `template <class CharT> json_traits<CharT>` replaced with `sorted_policy` 
- `template <class CharT> o_json_traits<CharT>` replaced with `preserve_order_policy`

- The return type for the json::get_with_default function overload for `const char*` has been
  changed from `const char*` to `json::string_view_type`, which is assignable to `std::string`.

- New functions `byte_string_value` and `do_byte_string_value` have been added to
  `basic_json_input_handler` and `basic_json_output_handler`

- json::is<const char*>() and json::as<const char*>() specializations (supported but never 
  documented) have been deprecated

- In android specific `string_to_double`, `strtod_l` changed to `strtold_l`

Enhancements:

- The `json` class and the `decode_cbor` and `encode_cbor` functions now support byte strings
  A `json` byte string value will, when serialized to JSON, be converted to a base64url string.

- `version.hpp` added to `include` directory

0.99.9.2
--------

Bug fixes:

- Fixed issue with jsonpatch::diff (fix contributed by Alexander (rog13)) 

Enhancements:

- New class `cbor_view` for accessing packed `cbor` values. A `cbor_view` satisfies the requirements for `jsonpointer::get`.

Changes (non breaking)
----------------------

- `jsonpointer::erase` renamed to `jsonpointer::remove`, old name deprecated

0.99.9.1
--------

New features
------------

- JSON Pointer implementation

- JSON Patch implementation, includes patch and diff

- `json::insert` function for array that inserts values from range [first, last) before pos. 

Bug fixes

- Fixed issue with serialization of json array of objects to csv file

Changes (non breaking)
----------------------

- The member function name `json::dump_body` has been deprecated and replaced with `json::dump_fragment`. 
- The non member function name `dump_body` has been deprecated and replaced with `dump_fragment`. 

- The class name `rename_name_filter` has been deprecated and replaced with `rename_object_member_filter`.

- In the documentation and examples, the existing function `json::insert_or_assign` 
  is now used instead of the still-supported `json::set`. The reason is that 
  `insert_or_assign` follows the naming convention of the C++ standard library.   

Changes
-------

- The recently introduced class `json_stream_traits` has been renamed to `serialization_traits`

- Removed template parameter `CharT` from class `basic_parsing_context` and renamed it to `parsing_context`

- Removed template parameter `CharT` from class `basic_parse_error_handler` and renamed it to `parse_error_handler`

0.99.8.2
--------

New features

- Added `json` functions `push_back` and `insert` for appending values 
  to the end of a `json` array and inserting values at a specifed position

Rationale: While these functions provide the same functionality as the existing
`json::add` function, they have the advantage of following the naming conventions 
of the C++ library, and have been given prominence in the examples and documentation 
(`add` is still supported.)

0.99.8.1
--------

New features

- cbor extension supports encoding to and decoding from the cbor binary serialization format.

- `json_type_traits` supports `std::valarray`

Documentation

- Documentation is now in the repository itself. Please see the documentation
  link in the README.md file

Changed

- Removed `CharT` template parameter from `json_stream_traits`

0.99.8
------
 
Changes

- Visual Studio 2013 is no longer supported (jsonpath uses string initilizer lists)

- `json_input_handler` overloaded functions value(value,context)` have been deprecated.
  Instead use `string_value(value,context)`, `integer_value(value,context)`, 
  `uinteger_value(value,context)`, `double_value(value,precision,context)`, 
  `bool_value(value,context)` and `null_value(context)`

- `json_output_handler` overloaded functions value(value)` have been deprecated.
  Instead use `string_value(value)`, `integer_value(value)`, `uinteger_value(value)`,
  `double_value(value,precision=0)`, `bool_value(value)` and `null_value(context)`

- For consistency, the names `jsoncons_ext/msgpack/message_pack.hpp`, 
  `encode_message_pack` and `decode_message_pack` have been deprecated and 
  replaced with `jsoncons_ext/msgpack/msgpack.hpp`, `encode_msgpack` and `decode_msg_pack`

Bug fixes

- Fixed operator== throws when comparing a string against an empty object

- Fixed jsonpath issue with array 'length' (a.length worked but not a['length'])

- `msgpack` extension uses intrinsics for determing whether to swap bytes 

New features

- Stream supported C++ values directly to JSON output, governed by `json_stream_traits` 

- json::is<T>() and json::as<T>() accept template packs, which they forward to the 
  `json_type_traits` `is` and `as` functions. This allows user defined `json_type_traits` 
  implementations to resolve, for instance, a name into a C++ object looked up from a
  registry. See [Type Extensibility](https://github.com/danielaparker/jsoncons), Example 2. 

- jsonpath `json_query` now supports returning normalized paths (with
  optional `return_type::path` parameter)

- New jsonpath `max` and `min` aggregate functions over numeric values

- New `json::merge` function that inserts another json object's key-value pairs 
  into a json object, if they don't already exist.

- New `json::merge_or_update` function that inserts another json object's key-value 
  pairs into a json object, or assigns them if they already exist.

0.99.7.3
--------

- `json_type_traits` supports `std::pair` (convert to/from json array of size 2)

- `parse_stream` renamed to `parse` (backwards compatible)

- `kvp_type` renamed to `key_value_pair_type` (backwards compatible)

- The `_json` and `_ojson` literal operators have been moved to the namespace `jsoncons::literals`.
  Access to these literals now requires
```c++
    using namespace jsoncons::literals;    
```
Rationale: avoid name clashes with other `json` libraries        

- The name `owjson` has been deprecated (still works) and changed to `wojson`. Rationale: naming consistency

- Added json array functions `emplace_back` and `emplace`, and json object functions `try_emplace`
  and `insert_or_assign`, which are analagous to the standard library vector and map functions. 

0.99.7.2
--------

Bug fix

- A bug was introduced in 0.99.7 causing the values of existing object members to not be changed wiht set or assignment operations. This has been fixed.

Change

- jsoncons_ext/binary changed to jsoncons_ext/msgpack
- namespace jsoncons::binary changed to jsoncons::msgpack

0.99.7.1
--------

- Workarounds in unicode_traits and jsonpath to maintain support for vs2013 
- Added `mapping_type::n_rows`, `mapping_type::n_objects`, and `mapping_type::m_columns` options for csv to json 

0.99.7
------

Bug fixes

- Issues with precedence in JsonPath filter evaluations have been fixed
- An issue with (a - expression) in JsonPath filter evaluations has been fixed

New feature

- The new binary extension supports encoding to and decoding from the MessagePack binary serialization format.
- An extension to JsonPath to allow filter expressions over a single object.
- Added support for `*` and `/` operators to jsonpath filter
- literal operators _json and _ojson have been introduced

Non-breaking changes

- The `json` `write` functions have been renamed to `dump`. The old names have been deprecated but still work.
- Support for stateful allocators
- json function object_range() now returns a pair of RandomAccessIterator (previously BidirectionalIterator)
- json operator [size_t i] applied to a json object now returns the ith object (previously threw) 

Breaking change (if you've implemented your own input and output handlers)

In basic_json_input_handler, the virtual functions
```c++
virtual void do_name(const CharT* value, size_t length, 
                     const basic_parsing_context<CharT>& context)

virtual void do_string_value(const CharT* value, size_t length, 
                             const basic_parsing_context<CharT>& context)
```
have been changed to
```c++
virtual void do_name(string_view_type val, 
                     const basic_parsing_context<CharT>& context) 

virtual void do_string_value(string_view_type val, 
                             const basic_parsing_context<CharT>& context) 
```

In basic_json_output_handler, the virtual functions
```c++
virtual void do_name(const CharT* value, size_t length) 

virtual void do_string_value(const CharT* value, size_t length) 
```
have been changed to
```c++
virtual void do_name(string_view_type val)

virtual void do_string_value(string_view_type val)
```

Removed features:

- The jsonx extension has been removed 

0.99.5
------

- Validations added to utf8 and utf16 string parsing to pass all [JSONTestSuite](https://github.com/nst/JSONTestSuite) tests
- The name `json_encoder` introduced in 0.99.4 has been changed to `json_decoder`. Rationale: consistencty with common usage (encoding and serialization, decoding and deserialization)

0.99.4a
-------

Fixes Issue #101, In json.hpp, line 3376 change "char__type" to "char_type" 
Fixes Issue #102, include cstring and json_error_category.hpp in json.hpp

0.99.4
------

Changes

- The deprecated class `json::any` has been removed. 
- The jsoncons `boost` extension has been removed. That extension contained a sample `json_type_traits` specialization for `boost::gregorian::date`, which may still be found in the "Type Extensibility" tutorial.  
- The member `json_type_traits` member function `assign` has been removed and replaced by `to_json`. if you have implemented your own type specializations, you will also have to change your `assign` function to `to_json`.
- `json_type_traits` specializations no longer require the `is_assignable` data member

Non-breaking name changes

- The names `json_deserializer`,`ojson_deserializer`,`wjson_deserializer`,`owjson_deserializer` have been deprecated (they still work) and replaced by `json_encoder<json>`, `json_encoder<ojson>`, `json_encoder<wjson>` and `json_encoder<owjson>`.  
- The name `output_format` has been deprecated (still works) and renamed to `serialization_options`.  
- The name `wojson` has been deprecated (still works) and renamed to `owjson`.  
- The `json_filter` member function `input_handler` has been deprecated (still works) and renamed to `downstream_handler`.  
- The name `elements` has been deprecated (still works) and renamed to `owjson`.  
- The `json` member function `members()` has been deprecated (still works) and renamed to `object_range()`.  
- The `json` member function `elements()` has been deprecated (still works) and renamed to `array_range()`.  
- The `json` member_type function `name()` has been  deprecated (still works) and renamed to `key()`. Rationale: consistency with more general underlying storage classes.

New features

- `json_filter` instances can be passed to functions that take a `json_output_handler` argument (previously only a `json_input_handler` argument)
- New `jsonpath` function `json_replace` that searches for all values that match a JsonPath expression and replaces them with a specified value.
- `json` class has new method `has_key()`, which returns `true` if a `json` value is an object and has a member with that key
- New filter class `rename_name` allows search and replace of JSON object names

0.99.3a
-------

Changes

The `json` initializer-list constructor has been removed, it gives inconsistent results when an initializer has zero elements, or one element of the type being initialized (`json`). Please replace

`json j = {1,2,3}` with `json j = json::array{1,2,3}`, and 

`json j = {{1,2,3},{4,5,6}}` with `json j = json::array{json::array{1,2,3},json::array{4,5,6}}`

- Initializer-list constructors are now supported in `json::object` as well as `json::array`, e.g.
```c++
json j = json::object{{"first",1},{"second",json::array{1,2,3}}};
```

- json::any has been deprecated and will be removed in the future

- The json method `to_stream` has been renamed to `write`, the old name is still supported.

- `output_format` `object_array_block_option`, `array_array_block_option` functions have been deprecated and replaced by 
   `object_array_split_lines`, `array_array_split_lines` functions. 

Enhancements

- A new method `get_with_default`, with return type that of the default, has been added to `json`

- A new template parameter, `JsonTraits`, has been added to the `basic_json` class template. 

- New instantiations of `basic_json`, `ojson` and `wojson`, have been added for users who wish to preserve the alphabetical sort of parsed json text and to insert new members in arbitrary name order.

- Added support for `json` `is<T>`, `as<T>`, constructor, and assignment operator for any sequence container (`std::array`, `std::vector`, `std::deque`, `std::forward_list`, `std::list`) whose values are assignable to JSON types (e.g., ints, doubles, bools, strings, STL containers of same) and for associative containers (`std::set`, `std::multiset`, `std::unordered_set`, `std::unordered_multiset`.)

- Added static method `null()` to `json` class to return null value

- A new extension jsonx that supports serializing JSON values to [JSONx](http://www.ibm.com/support/knowledgecenter/SS9H2Y_7.5.0/com.ibm.dp.doc/json_jsonx.html) (XML)

- json parser will skip `bom` in input if present

Fixes:

- Fixes to the `jsonpath` extension, including the union operator and applying index operations to string values

- Fixes to remove warnings and issues reported by VS2015 with 4-th warnings level, PVS-Studio static analyzer tool, and UBSAN. 

0.99.2
------

- Included workaround for a C++11 issue in GCC 4.8, contributed by Alex Merry
- Fixed operator== so that json() == json(json::object())
- Fixed issue with `json` assignment to initializer list
- Fixed issue with assignment to empty json object with multiple keys, e.g. 

    json val; 
    val["key1"]["key2"] = 1; 

0.99.1
------

- Fix to json_filter class
- Fix to readme_examples

0.99
----

- Fixes to deprecated json parse functions (deprecated, but still supposed to work)
- The Visual C++ specific implementation for reading floating point numbers should have freed a `_locale_t` object, fixed 
- Added `json_type_traits` specialization to support assignment from non-const strings 
- When parsing fractional numbers in text, floating point number precision is retained, and made available to serialization to preserve round-trip. The default output precision has been changed from 15 to 16.
- Added json `std::initializer_list` constructor for constructing arrays
- The deprecated json member constants null, an_object, and an_array have been removed
- Microsoft VC++ versions earlier than 2013 are no longer supported

0.98.4
------

- Fixes issues with compilation with clang

0.98.3
------

New features

- Supports [Stefan Goessner's JsonPath](http://goessner.net/articles/JsonPath/).
- json member function `find` added
- json member function `count` added
- json array range accessor `elements()` added, which supports range-based for loops over json arrays, and replaces `begin_elements` and `end_elements`
- json object range accessor `members()` added, which supports range-based for loops over json objects, and replaces `begin_members` and `end_members`
- New version of json `add` member function that takes a parameter `array_iterator` 
- json member function `shrink_to_fit` added

API Changes 

- The json internal representation of signed and unsigned integers has been changed from `long long` and `unsigned long long` to `int64_t` and `uint64_t`. This should not impact you unless you've implemented your own `json_input_handler` or `json_output_handler`, in which case you'll need to change your `json_input_handler` function signatures
 
    void do_longlong_value(long long value, const basic_parsing_context<Char>& context) override
    void do_ulonglong_integer_value(unsigned long long value, const basic_parsing_context<Char>& context) override

to

    void do_integer_value(int64_t value, const basic_parsing_context<Char>& context) override
    void do_uinteger_value(uint64_t value, const basic_parsing_context<Char>& context) override
  
and your `json_output_handler` function signatures from     

    void do_longlong_value(long long value) override
    void do_ulonglong_integer_value(unsigned long long value) override

to

    void do_integer_value(int64_t value) override
    void do_uinteger_value(uint64_t value) override

- `output_format` drops support for `floatfield` property

Non-beaking API Changes
- remove_range has been deprecated, use erase(array_iterator first, array_iterator last) instead
- remove has been deprecated, use erase(const std::string& name ) instead
- `json::parse_string` has been renamed to `json::parse`, `parse_string` is deprecated but still works
- `json member function `is_empty` has been renamed to `empty`, `is_empty` is deprecated but still works. Rationale: consistency with C++ containers
- json member functions `begin_elements` and `end_elements` have been deprecated, instead use `elements().begin()` and `elements.end()`
- json member functions `begin_members` and `end_members` have been deprecated, instead use `members().begin()` and `members.end()`
- json member function `has_member` has been deprecated, instead use `count`. Rationale: consistency with C++ containers
- json member function `remove_member` has been deprecated, instead use `remove`. Rationale: only member function left with _element or _member suffix 
- json_parse_exception renamed to parse_error, json_parse_exception typedef to parse_error
- json::parse(std::istream& is) renamed to json::parse_stream. json::parse(std::istream is) is deprecated but still works.

0.98.2 Release
--------------

- `json` constructor is now templated, so constructors now accept extended types
- Following [RFC7159](http://www.ietf.org/rfc/rfc7159.txt), `json_parser` now accepts any JSON value, removing the constraint that it be an object or array.
- The member `json_type_traits` member functions `is`, `as`, and `assign` have been changed to static functions. if you have implemented your own type specializations, you will also have to change your `is`, `as` and `assign` functions to be static.
- Removed json deprecated functions `custom_data`, `set_custom_data`, `add_custom_data`
- `json_reader` member function `max_depth` has been renamed to `max_nesting_depth`, the former name is still supported. 
- `json` member function `resize_array` has been renamed to `resize`, the former name is still supported. 

jsoncons supports alternative ways for constructing  `null`, `object`, and `array` values.

null:

    json a = jsoncons::null_type();  // Using type constructor
    json b = json::null_type();      // Using alias
    json c(json::null);              // From static data member prototype

object:

    json a();                 // Default is empty object
    json b = json::object();  // Using type constructor
    json c(json::an_object);  // From static data member prototype

array:

    json a = json::array();      // Using type constructor
    json b = json::make_array(); // Using factory method
    json c(json::an_array);      // From static data member prototype

Since C++ has possible order issues with static data members, the jsoncons examples and documentation have been changed to consistently use the other ways, and `json::null`, `json::an_object` and `json::an_array` have been, while still usable, deprecated.

0.98.1 Release
--------------

- Enhances parser for CSV files that outputs JSON, see example below. 
- Adds `get_result` member function to `json_deserializer`, which returns the json value `v` stored in a `json_deserializer` as `std::move(v)`. The `root()` member function has been deprecated but is still supported.
- Adds `is_valid` member function to `json_deserializer`
- Enhances json::any class, adds type checks when casting back to original value
- Fixes some warning messages

0.98 Release
--------------

Bug fixes:

- Fixes the noexcept specification (required for Visual Studio 2015 and later.) Fix
  contributed by Rupert Steel.
- Fixes bug with proxy operator== when comparing object member values,
  such as in val["field"] == json("abc")

Enhancements:

- Refines error codes and improves error messages

- Renames `json_reader` method `read` to `read_next`, reflecting the fact that it supports reading a sequence of JSON texts from a stream. The 
  former name is deprecated but still works.

- Adds `json_reader` method `check_done` that throws if there are unconsumed non-whitespace characters after one or more calls to `read_next`.

- Adds getter and setter `max_depth` methods to allow setting the maximum JSON parse tree depth if desired, by default
it is arbitrarily large (limited by heap memory.)

- Modifies `json` static methods `parse_string`, `parse_file`, and `parse` behaviour to throw if there are unconsumed non-whitespace characters after reading one JSON text.  

Changes to extensions:

- Changes the top level namespace for the extensions from `jsoncons_ext` to `jsoncons`, e.g. `jsoncons_ext::csv::csv_reader` becomes `jsoncons::csv::csv_reader`
- Modifies csv_reader and csv_serializer so that the constructors are passed parameters in a `csv_parameters` object rather than a `json` object.
- Adds more options to csv_reader

0.97.2 Release
--------------

- Incorporates test suite files from http://www.json.org/JSON_checker/ into test suite
- The `jsoncons` parser accepts all of the JSON_checker files that its supposed to accept.
- Failures to reject incorrect exponential notation (e.g. [0e+-1]) have been fixed.
- The `jsoncons` parser now rejects all of the JSON_checker files that its supposed to reject except ones with stuff after the end of the document, e.g.

    ["Extra close"]]

  (Currently the `jsoncons` parser stops after reading a complete JSON text, and supports reading a sequence of JSON texts.)  

- Incorporates a fix to operator== on json objects, contributed by Alex Merry

0.97.1 Release
--------------

- "Transforming JSON with filters" example fixed
- Added a class-specific in-place new to the json class that is implemented in terms of the global version (required to create json objects with placement new operator.)
- Reorganized header files, removing unnecessary includes. 
- Incorporates validation contributed by Alex Merry for ensuring that there is an object or array on parse head.
- Incorporates fix contributed by Milan Burda for “Switch case is in protected scope” clang build error

0.97 Release
------------

- Reversion of 0.96 change:

The virtual methods `do_float_value`, `do_integer_value`, and `do_unsigned_value` of `json_input_handler` and `json_output_handler` have been restored to `do_double_value`, `do_longlong_value` and `do_ulonglong_value`, and their typedefed parameter types `float_type`, `integer_type`, and `unsigned_type` have been restored to `double`, `long long`, and `unsigned long long`.

The rationale for this reversion is that the change doesn't really help to make the software more flexible, and that it's better to leave out the typedefs. There will be future enhancements to support greater numeric precision, but these will not affect the current method signatures.

- Fix for "unused variable" warning message

0.96 Release
------------

This release includes breaking changes to interfaces. Going forward, the interfaces are expected to be stable.

Breaking changes:

- Renamed `error_handler` to `parse_error_handler`.

- Renamed namespace `json_parser_error` to `json_parser_errc`

- Renamed `value_adapter` to `json_type_traits`, if you have implemented your own type specializations,
  you will have to rename `value_adapter` also.

- Only json arrays now support `operator[](size_t)` to loop over values, this is no longer supported for `json` objects. Use a json object iterator instead.

- The virtual methods `do_double_value`, `do_integer_value` and `do_uinteger_value` of `json_input_handler` and `json_output_handler` have been renamed to `do_float_value`, `do_integer_value`, and `do_unsigned_value`, 
  and their parameters have been changed from `double`, `long long`, and `unsigned long long` to typedefs `float_type`, `integer_type`, and `unsigned_type`.
  The rationale for this change is to allow different configurations for internal number types (reversed in 0.97.)

General changes

- `json` member function `begin_object` now returns a bidirectional iterator rather than a random access iterator.

- Static singleton `instance` methods have been added to `default_parse_error_handler`
  and `empty_json_input_handler`. 

- Added to the `json` class overloaded static methods parse, parse_string 
  and parse_file that take a `parse_error_handler` as a parameter. 

- Added methods `last_char()` and `eof()` to `parsing_context`.

- Enhancements to json parsing and json parse event error notification.

- Added to `json_input_handler` and `json_output_handler` a non virtual method `value` that takes a null terminated string.

- Added methods `is_integer`, `is_unsigned` and `is_float` to `json` to replace `is_longlong`, `is_ulonglong` and `is_double`, which have been deprecated.

- Added methods `as_integer`, `as_unsigned` and `as_float` to `json` to replace `is_longlong`, `is_ulonglong` and `is_double`, which have been deprecated.

Bug fixes:

- Fixed issue with column number reported by json_reader

- Where &s[0] and s.length() were passed to methods, &s[0] has been replaced with s.c_str(). 
  While this shouldn't be an issue on most implementations, VS throws an exception in debug modes when the string has length zero.

- Fixes two issues in 0.95 reported by Alex Merry that caused errors with GCC: a superfluous typename has been removed in csv_serializer.hpp, and a JSONCONS_NOEXCEPT specifier has been added to the json_error_category_impl name method.

- Fixed a number of typename issues in the 0.96 candidate identifed by Ignatov Serguei.

- Fixes issues with testsuite cmake and scons reported by Alex Merry and Ignatov Serguei

0.95
----

Enhancements:

- Added template method `any_cast` to `json` class.

- The allocator type parameter in basic_json is now supported, it allows you to supply a 
  custom allocator for dynamically allocated, fixed size small objects in the json container.
  The allocator type is not used for structures including vectors and strings that use large 
  or variable amounts of memory, these always use the default allocators.

Non-breaking Change:
 
- `json_filter` method `parent` has been renamed to `input_handler` (old name still works) 

Breaking change (if you've implemented your own input and output handlers, or if you've
passed json events to input and output handlers directly):

- The input handler virtual method 
  `name(const std::string& name, const parsing_context& context)` 
  has been changed to
  `do_name(const char* p, size_t length, const parsing_context& context)` 

- The output handler virtual method 
  `name(const std::string& name)` 
  has been changed to
  `do_name(const char* p, size_t length)` 

- The input handler virtual method 
  `string_value(const std::string& value, const parsing_context& context)` 
  has been changed to
  `do_string_value(const char* p, size_t length, const parsing_context& context)` 

- The output handler virtual method 
  `string_value(const std::string& value)` 
  has been changed to
  `do_string_value(const char* p, size_t length)` 

The rationale for the method parameter changes is to allow different internal
representations of strings but preserve efficiency. 

- The input and output handler virtual implementation methods begin_json, end_json,
  begin_object, end_object, begin_array, end_array, name, string_value, 
  longlong_value, ulonglong_value, double_value, bool_value and null_value 
  have been renamed to do_begin_json, do_end_json, do_begin_object, do_end_object, 
  do_begin_array, do_end_array, do_name, do_string_value, do_longlong_value, 
  do_ulonglong_value, do_double_value, do_bool_value and do_null_value and have been 
  made private. 
  
- Public non-virtual interface methods begin_json, end_json,
  begin_object, end_object, begin_array, end_array, name
  have been added to json_input_handler and json_output_handler. 

The rationale for these changes is to follow best C++ practices by making the
json_input_handler and json_output_handler interfaces public non-virtual and 
the implementations private virtual. Refer to the documentation and tutorials for details.   

- The error_handler virtual implementation methods have been renamed to `do_warning` and 
  `do_error`, and made private. Non virtual public interface methods `warning` and `error` 
  have been added. Error handling now leverages `std::error_code` to communicate parser 
  error events in an extendable way.

Bug fixes:

- Fixed bug in csv_reader

0.94.1
------

Bug fixes:

- Incorporates fix from Alex Merry for comparison of json objects

0.94
----

Bug fixes 

- Incorporates contributions from Cory Fields for silencing some compiler warnings
- Fixes bug reported by Vitaliy Gusev in json object operator[size_t]
- Fixes bug in json is_empty method for empty objects

Changes

- json constructors that take string, double etc. are now declared explicit (assignments and defaults to get and make_array methods have their own implementation and do not depend on implicit constructors.)
- make_multi_array renamed to make_array (old name is still supported)
- Previous versions supported any type values through special methods set_custom_data, add_custom_data, and custom_data. This version introduces a new type json::any that wraps any values and works with the usual accessors set, add and as, so the specialized methods are no longer required.

Enhancements 

- json get method with default value now accepts extended types as defaults
- json make_array method with default value now accepts extended types as defaults

New extensions

- Added jsoncons_ext/boost/type_extensions.hpp to collect 
  extensions traits for boost types, in particular, for
  boost::gregorian dates.

0.93 Release
------------

New features

- Supports wide character strings and streams with wjson, wjson_reader etc. Assumes UTF16 encoding if sizeof(wchar_t)=2 and UTF32 encoding if sizeof(wchar_t)=4.
- The empty class null_type  is added to the jsoncons namespace, it replaces the member type json::null_type (json::null_type is typedefed to jsoncons::null_type for backward compatibility.)

Defect fixes:

- The ascii character 0x7f (del) was not being considered a control character to be escaped, this is fixed.
- Fixed two issues with serialization when the output format property escape_all_non_ascii is enabled. One, the individual bytes were being checked if they were non ascii, rather than first converting to a codepoint. Two, continuations weren't being handled when decoding.

0.92a Release
-------------

Includes contributed updates for valid compilation and execution in gcc and clang environments

0.92 Release
------------

Breaking change (but only if you have subclassed json_input_handler or json_output_handler)

- For consistency with other names, the input and output handler methods new to 0.91 - value_string, value_double, value_longlong, value_ulonglong and value_bool - have been renamed to string_value, double_value, longlong_value, ulonglong_value and bool_value.

Non breaking changes (previous features are deprecated but still work)

- name_value_pair has been renamed to member_type (typedefed to previous name.)

- as_string(output_format format) has been deprecated, use the existing to_string(output_format format) instead

Enhancements:

- json now has extensibilty, you can access and modify json values with new types, see the tutorial Extensibility 

Preparation for allocator support:

- The basic_json and related classes now have an Storage template parameter, which is currently just a placeholder, but will later provide a hook to allow users to control how json storage is allocated. This addition is transparent to users of the json and related classes.

0.91 Release
------------

This release should be largely backwards compatible with 0.90 and 0.83 with two exceptions: 

1. If you have used object iterators, you will need to replace uses of std::pair with name_value_pair, in particular, first becomes name() and second becomes value(). 

2. If you have subclassed json_input_handler, json_output_handler, or json_filter, and have implemented value(const std::string& ...,  value(double ..., etc., you will need to modify the names to  value_string(const std::string& ...,  value_double(double ... (no changes if you are feeding existing implementations.)

The changes are

- Replaced std::pair<std::string,json> with name_value_pair that has accessors name() and value()

- In json_input_handler and json_output_handler, allowed for overrides of the value methods by making them non-virtual and adding virtual methods value_string, value_double, value_longlong, value_ulonglong, and value_bool

Other new features:

- Changed implementation of is<T> and as<T>, the current implementation should be user extensible

- make_multi_array<N> makes a multidimensional array with the number of dimensions specified as a template parameter. Replaces make_2d_array and make_3d_array, which are now deprecated.

- Added support for is<std::vector<T>> and as<std::vector<T>>

- Removed JSONCONS_NO_CXX11_RVALUE_REFERENCES, compiler must support move semantics

Incorporates a number of contributions from Pedro Larroy and the developers of the clearskies_core project:

- build system for posix systems
- GCC to list of supported compilers
- Android fix
- fixed virtual destructors missing in json_input_handler, json_output_handler and parsing_context
- fixed const_iterator should be iterator in json_object implementation 

To clean up the interface and avoid too much duplicated functionality, we've deprecated some json methods (but they still work)

    make_array
Use json val(json::an_array) or json::make_multi_array<1>(...) instead (but make_array will continue to work)

    make_2d_array
    make_3d_array
Use make_multi_array<2> and make_multi_array<3> instead

    as_vector
Use as<std::vector<int>> etc. instead

    as_int
    as_uint
    as_char
Use as<int>, as<unsigned int>, and as<char> instead

Release 0.90a
-------------

Fixed issue affecting clang compile

Release 0.90
------------

This release should be fully backwards compatible with 0.83. 

Includes performance enhancements to json_reader and json_deserializer

Fixes issues with column numbers reported with exceptions

Incorporates a number of patches contributed by Marc Chevrier:

- Fixed issue with set member on json object when a member with that name already exists
- clang port
- cmake build files for examples and test suite
- json template method is<T> for examining the types of json values
- json template method as<T> for accessing json values

0.83
------------

Optimizations (very unlikely to break earlier code)

- get(const std::name& name) const now returns const json& if keyed value exists, otherwise a const reference to json::null

- get(const std::string& name, const json& default_val) const now returns const json (actually a const proxy that evaluates to json if read)

Bug fixes

- Line number not incremented within multiline comment - fixed

Deprecated features removed

- Removed deprecated output_format properties (too much bagage to carry around)

0.82a
-------------

- The const version of the json operator[](const std::string& name) didn't need to return a proxy, the return value has been changed to const json& (this change is transparent to the user.) 

- get(const std::name& name) has been changed to return a copy (rather than a reference), and json::null if there is no member with that name (rather than throw.) This way both get methods return default values if no keyed value exists.

- non-const and const methods json& at(const std::name& name) have been added to replace the old single argument get method. These have the same behavior as the corresponding operator[] functions, but the non-const at is more efficient.

0.81
------------

- Added accessor and modifier methods floatfield to output_format to provide a supported way to set the floatfield format flag to fixed or scientific with a specified number of decimal places (this can be done in older versions, but only with deprecated methods.)

- The default constructor now constructs an empty object (rather than a null object.) While this is a change, it's unlikely to break exisitng code (all test cases passed without modification.)

This means that instead of

    json obj(json::an_object);
    obj["field"] = "field";

you can simply write

    json obj;
    obj["field"] = "field";

The former notation is still supported, though.

- Added a version of 'resize_array' to json that resizes the array to n elements and initializes them to a specified value.

- Added a version of the static method json::make_array that takes no arguments and makes an empty json array

Note that

    json arr(json::an_array);

is equivalent to

    json arr = json::make_array();

and

    json arr(json::an_array);
    arr.resize_array(10,0.0);

is equivalent to

    json arr = json::make_array(10,0.0);

For consistency the json::make_array notation is now favored in the documentation. 

0.71
-------------

- Added resize_array method to json for resizing json arrays 

- Fixed issue with remove_range method (templated code failed to compile if calling this method.)

- Added remove_member method to remove a member from a json object

- Fixed issue with multiline line comments, added test case

- Fixed issue with adding custom data to a json array using add_custom_data, added examples.

0.70
-------------

- Since 0.50, jsoncons has used snprintf for default serialization of double values to string values. This can result in invalid json output when running on a locale like German or Spanish. The period character (â€˜.â€™) is now always used as the decimal point, non English locales are ignored.

- The output_format methods that support alternative floating point formatting, e.g. fixed, have been deprecated.

- Added a template method as_vector<T> to the json class. If a json value is an array and conversion is possible to the template type, returns a std::vector of that type, otherwise throws an std::exception. Specializations are provided for std::string, bool, char, int, unsigned int, long, unsigned long, long long, unsigned long long, and double. For example

    std::string s("[0,1,2,3]");

    json val = json::parse_string(s);

    std::vector<int> v = val.as_vector<int>(); 

- Undeprecated the json member function precision

0.60b
-------------

This release (0.60b) is fully backwards compatible with 0.50.

A change introduced with 0.60 has been reversed. 0.60 introduced an alternative method of constructing a json arrray or object with an initial default constructor, a bug with this was fixed in 0.60a, but this feature and related documentation has been removed because it added complexity but no real value.

### Enhancements

- Added swap member function to json

- Added add and add_custom_data overrides to json that take an index value, for adding a new element at the specified index and shifting all elements currently at or above that index to the right.

- Added capacity member functions to json

### 0.60  extensions

- csv_serializer has been added to the csv extension

0.50
------------

This release is fully backwards compatible with 0.4*, and mostly backwards compatible to 0.32 apart from the two name changes in 0.41

Bug fixes

- When reading the escaped characters "\\b", "\\f", "\\r" and "\\t" appearing in json strings, json_reader was replacing them with the linefeed character, this has been fixed.

Deprecated 

- Deprecated modifiers precision and fixed_decimal_places from output_format. Use set_floating_point_format instead.
- Deprecated constructor that takes indenting parameter from output_format. For pretty printing with indenting, use the pretty_print function or pass the indenting parameter in json_serializer.

Changes

- When serializing floating point values to a stream, previous versions defaulted to default floating point precision with a precision of 16. This has been changed to truncate trailing zeros  but keep one if immediately after a decimal point.

New features

- For line reporting in parser error messages, json_reader now recognizes \\r\\n, \\n alone or \\r alone (\\r alone is new.)
- Added set_floating_point_format methods to output_format to give more control over floating point notation.

Non functional enhancements

- json_reader now estimates the minimum capacity for arrays and objects, and reports that information for the begin_array and begin_object events. This greatly reduces reallocations.

0.42
------------

- Fixed another bug with multi line /**/ comments 
- Minor fixes to reporting line and column number of errors
- Added fixed_decimal_places setter to output_format
- Added version of as_string to json that takes output_format as a parameter
- Reorganization of test cases and examples in source tree

0.41
------------

- Added non-member overload swap(json& a, json& b)
- Fixed bug with multi line /**/ comments 
- Added begin_json and end_json methods to json_output_handler
- json_deserializer should now satisfy basic exception safety (no leak guarantee)
- Moved csv_reader.hpp to jsoncons_ext/csv directory
- Changed csv_reader namespace to jsoncons::csv
- json::parse_file no longer reads the entire file into memory before parsing
  (it now uses json_reader default buffering)

0.40
------------

- json_listener renamed to json_input_handler
- json_writer renamed to json_output_handler

- Added json_filter class

- json get method that takes default argument now returns a value rather than a reference
- Issue in csv_reader related to get method issue fixed
- Issue with const json operator[] fixed
- Added as_char method to json
- Improved exception safety, some opportunites for memory leaks in the presence of exceptions removed

0.33
------------

Added reserve method to json

Added static make_3d_array method to json

json_reader now configured for buffered reading

Added csv_reader class for reading CSV files and producing JSON events

Fixed bug with explicitly passing output_format in pretty_print.

0.32
------------

Added remove_range method, operator== and  operator!= to proxy and json objects

Added static methods make_array and make_2d_array to json

0.31
------------

error_handler method content_error renamed to error

Added error_code to warning, error and fatal_error methods of error_handler

0.30
------------

json_in_stream renamed to json_listener

json_out_stream renamed to json_writer

Added buffer accessor method to parsing_context

0.20
------------

Added parsing_context class for providing information about the
element being parsed.

error_handler methods take message and parsing_context parameters

json_in_stream handlers take parsing_context parameter

0.19
------------

Added error_handler class for json_reader

Made json_exception a base class for all json exceptions

Added root() method to json_deserializer to get a reference to the json value

Removed swap_root() method from json_deserializer

0.18
------------

Renamed serialize() class method to to_stream() in json  

Custom data serialization supported through template function specialization of serialize
(reverses change in 0.17)


0.17
------------

Added is_custom() method to json and proxy

get_custom() method renamed to custom_data() in json and proxy

Added clear() method to json and proxy

set_member() method renamed to set()

set_custom() method renamed to set_custom_data()

push_back() method renamed to add() in json and proxy

Added add_custom_data method() in json and proxy

Custom data serialization supported through template class specialization of custom_serialization
(replaces template function specialization of serialize)

0.16
------------

Change to json_out_stream and json_serializer:

    void value(const custom_data& value)

removed.

Free function serialize replaces free function to_stream for
serializing custom data.

pretty print tidied up for nested arrays

0.15
------------

Made eof() method on json_reader public, to support reading
multiple JSON objects from a stream.

0.14
------------

Added pretty_print class

Renamed json_stream_writer to json_serializer, 
implements pure virtual class json_out_stream
 
Renamed json_stream_listener to json_deserializer
implements pure virtual class json_in_stream

Renamed json_parser to json_reader, parse to read.

Changed indenting so object and array members start on new line.

Added support for storing user data in json object, and
serializing to JSON.

0.13
------------

Replaced simple_string union member with json_string that 
wraps std::basic_string<Char>

name() and value() event handler methods on 
basic_json_stream_writer take const std::basic_string<Char>&
rather than const Char* and length.

0.12
------------

Implemented operator<< for json::proxy

Added to_stream methods to json::proxy

0.11
------------

Added members to json_parser to access and modify the buffer capacity

Added checks when parsing integer values to determine overflow for 
long long and unsigned long long, and if overflow, parse them as
doubles. 


