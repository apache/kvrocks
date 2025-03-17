Release Notes           {#release_notes}
=============

\section v0-12-0 Version 0.12.0 "Dude, Where's My Bored Ape?"

_Released:_ June 19, 2022

> **IMPORTANT:** This release deprecates `views::group_by` which was
> an endless source of confusion. `group_by` is replaced with
> `views::chunk_by` (which, beware, has _subtly_ different semantics,
> see below.)

Changes:
* **NEW:** `views::chunk_by` which, like the old `views::group_by` it replaces,
  splits a range into a range-of-ranges, where adjacent elements satisfy a binary
  predicate  ([\#1648](https://github.com/ericniebler/range-v3/pull/1648)). [_Note:_ Whereas `views::group_by` evaluated the predicate
  between the current element and the _first_ element in the chunk, `views::chunk_by`
  evaluates the predicate between _adjacent_ elements. -- _end note_]
* **NEW:** `constexpr` all the algorithms that are `constexpr` in C++20's `std::ranges`
  ([\#1683](https://github.com/ericniebler/range-v3/pull/1683)).
* **NEW:** Fold algorithms from [P2322](http://wg21.link/P2322) ([\#1628](https://github.com/ericniebler/range-v3/pull/1628)), ([\#1668](https://github.com/ericniebler/range-v3/pull/1668)).
* **NEW:** `ranges::unformatted_ostream_iterator` ([\#1586](https://github.com/ericniebler/range-v3/pull/1586)).
* **NEW:** Support for the `build2` build system ([\#1562](https://github.com/ericniebler/range-v3/pull/1562)).
* Implement [P2328](http://wg21.link/P2328): relax the constraint on `ranges::join_view`
  to support joining ranges of prvalue non-`view` ranges ([\#1655](https://github.com/ericniebler/range-v3/pull/1655)).
* Improved algorithm for `ranges::linear_distribute` ([\#1679](https://github.com/ericniebler/range-v3/pull/1679)).
* Renamed `safe_subrange_t` to `borrowed_subrange_t` ([\#1542](https://github.com/ericniebler/range-v3/pull/1542)).
* Extend `ranges::to` to support conversion to container-of-containers ([\#1553](https://github.com/ericniebler/range-v3/pull/1553)).
* `views::enumerate` can be a `borrowed_view` ([\#1571](https://github.com/ericniebler/range-v3/pull/1571)).
* `ranges::upper_bound` works in the presence of overloaded `operator&` ([\#1632](https://github.com/ericniebler/range-v3/pull/1632)).
* Input iterators are no longer required to be default-constructible ([\#1652](https://github.com/ericniebler/range-v3/pull/1652)).

Bugs fixed:
* `ranges::to<std::map>(v)` does not work ([\#1700](https://github.com/ericniebler/range-v3/pull/1700))
* `ranges::reverse_iterator` has the wrong `value_type` when reversing a proxy
  range ([\#1670](https://github.com/ericniebler/range-v3/pull/1670)).
* A post-increment of a `ranges::counted_iterator` wrapping an input iterator with
  a `void`-returning post-increment operator isn't incrementing the count ([\#1664](https://github.com/ericniebler/range-v3/pull/1664)).
* Bad assert in `views::drop_last` ([\#1599](https://github.com/ericniebler/range-v3/pull/1599)).
* Read of uninitialized `bool` in `views::cache1` ([\#1610](https://github.com/ericniebler/range-v3/pull/1610)).
* `ranges::unstable_remove_if` calls predicate on same element twice ([\#1629](https://github.com/ericniebler/range-v3/pull/1629)).
* `ranges::on(f,g)(x...)` should be `f(g(x)...)` instead of `f(g(x...))` ([\#1661](https://github.com/ericniebler/range-v3/pull/1661)).
* Broken qualification of cmake targets ([\#1557](https://github.com/ericniebler/range-v3/pull/1557)).
* Various portability and documentation fixes.

**Credits:** I would like to thank the following people who contributed to this release
(in no particular order): Barry Revzin, @dvirtz, Gonzalo Brito, Johel Ernesto Guerrero
Peña, Joël Lamotte, Doug Roeper, Facundo Tuesca, Vitaly Zaitsev, @23rd, @furkanusta,
Jonathan Haigh, @SmorkalovG, @marehr, Matt Beardsley, Chris Glover, Louis Dionne, Jin
Shang (@js8544), Hui Xie, @huixie90, Robert Maynard, Silver Zachara, @sergegers,
Théo DELRIEU, @LesnyRumcajs, Yehezkel Bernat, Maciej Patro, Klemens Nanni, Thomas
Madlener, and Jason Merrill.

&#127881; Special thanks to Barry Revzin for stepping up to be part-time co-maintainer of
range-v3. &#127881;

\section v0-11-0 Version 0.11.0 "Thanks, ISO"

_Released:_ August 6, 2020

> **IMPORTANT:** This release removes the heuristic that tries to guess whether a range type
is a "view" (lightweight, non-owning range), in accordance with the C++20. **This is a
potentially source-breaking change.** Code that previously used an rvalue range as the
start of a pipeline could stop compiling if the range library is not explicitly told that
that range type is a view. To override the new default, please specialize the
`ranges::enable_view<R>` Boolean variable template.

> **IMPORTANT:** This release removes the implicit conversion from views to containers.
To construct a container from an arbitrary range, you must now explicitly use
`ranges::to`. For example, the following code no longer works:
>
> ```c++
> std::vector<int> is = ranges::views::ints(0, 10); // ERROR: no conversion
> ```
>
> Instead, please write this as:
>
> ```c++
> auto is = ranges::views::ints(0, 10) | ranges::to<std::vector>; // OK
> ```
>
> `ranges::to` lives in header `<range/v3/range/conversion.hpp>`

> **IMPORTANT:** This release drops support for llvm-3.9.

Changes:
* **NEW:** A new concepts portability layer that short-circuits atomic constraints
  in `requires` clauses for better compile times when emulating concepts.
* **NEW:** Restored support for MSVC in `/std:c++17` mode, and for MSVC's default preprocessor.
* Remove the implicit conversion from views to containers.
* Rename the following entities to be consistent with C++20's `std::ranges` support:
  * `safe_range<R>` -> `borrowed_range<R>`
  * `enable_safe_range<R>` -> `enable_borrowed_range<R>`
  * `safe_iterator_t<R>` -> `borrowed_iterator_t<R>`
  * `safe_subrange_t<R>` -> `borrowed_subrange_t<R>`
  * `readable_traits<I>` -> `indirectly_readable_traits<I>`
  * `readable<I>` -> `indirectly_readable<I>`
  * `writable<I>` -> `indirectly_writable<I>`
* Added the following to the `ranges::cpp20` namespace:
  * Algorithm `for_each_n`
  * Algorithm `sample`
  * Class `view_base`
  * Alias `views::all_t`
* Type `__int128` is recognized as "integer-like".
* Adds concepts `three_way_comparable[_with]` when `<=>` is supported.
* Adds concepts `partially_ordered[_with]`.
* Better conformance with C++20's use of the _`boolean-testable`_ concept.
* Support C++20 coroutines.
* Honor CMake's `CMAKE_CXX_STANDARD` variable.
* A fix for the cardinality of `views::zip[_with]` ([\#1486](https://github.com/ericniebler/range-v3/pull/1486)).
* Add `view_interface::data()` member function.
* Add necessary specializations for `std::basic_common_reference` and
  `std::common_type`.
* Numerous workarounds for MSVC.
* Various CMake fixes and improvements.
* `drop_while_view` is not a `sized_range`.
* Added support for Wind River Systems.
* Bug fixes to `views::group_by` ([\#1393](https://github.com/ericniebler/range-v3/pull/1393)).
* `common_[reference|type]` of `common_[tuple|pair]` now yields a `common_[tuple|pair]`
  instead of a `std::[tuple|pair]` ([\#1422](https://github.com/ericniebler/range-v3/pull/1422)).
* Avoid UB when currying an lvalue in some views and actions ([\#1320](https://github.com/ericniebler/range-v3/pull/1320)).

**Credits:** I would like to thank the following people who contributed to this release
(in no particular order): Christopher Di Bella, @marehr, Casey Carter, Dvir Yitzchaki,
Justin Riddell, Johel Ernesto Guerrero Peña, Barry Revzin, Kamlesh Kumar, and Vincas
Dargis.

\section v0-10-0 Version 0.10.0 "To Err is Human"

_Released:_ Dec 6, 2019.

**IMPORTANT:** Before upgrading, please note that several older compiler versions
and build configurations are no longer supported! In particular, MSVC now needs
`/std:c++latest`.

**ALSO:** When taking a dependency on the `range-v3`, `meta`, or `concepts`
libraries via CMake, please now use the namespace qualified target names:
  - `range-v3::range-v3`
  - `range-v3::meta`
  - `range-v3::concepts`

Changes:
* **NEW:** Rewritten concepts portability layer with simpler macros for better
  diagnostics.
* **NEW:** The `views::cache1` view caches the most recent value in the
  range. This can help avoid reevaluation of transformations in complex view
  pipelines.
* **NEW:** `ranges::contains` algorithm.
* **NEW:** `enable_safe_range` trait for opting in to the _forwarding-range_
  concept. These are ranges whose iterators remain valid even after the
  range itself has been destroyed; _e.g._, `std::string_view` and
  `ranges::subrange`.
* The `readable` concept has changed such that types that are not _indirectly_
  readable with `operator*` (_e.g., `std::optional`) no longer satisfy that
  concept.
* Using `views::join` to join a range of xvalue ranges works again.
* The following range access primitives no longer accept temporary containers
  (_i.e._, they refuse to return references known to be dangling):
  - `range::front`
  - `range::back`
  - `range::at`
  - `range::index`
* `views::concat` with a single argument now simply returns its argument.
* `ranges::ostream_iterator<T>` now coerces arguments to `T` before inserting
  them into the wrapped ostream.
* Smaller iterators for `views::transform` and `views::take_while`.
* `actions::split` and `actions::split_when` now support partial application and
  pipelining ([\#1085](https://github.com/ericniebler/range-v3/issues/1085)).
* `views::group_by` and its iterator both get a `.base()` member to access the
  underlying range and iterator, respectively.
* Improved diagnostics with clang.
* Assorted bug fixes and compiler work-arounds:
  [\#284](https://github.com/ericniebler/range-v3/issues/284),
  [\#491](https://github.com/ericniebler/range-v3/issues/491),
  [\#499](https://github.com/ericniebler/range-v3/issues/499),
  [\#871](https://github.com/ericniebler/range-v3/issues/871),
  [\#1022](https://github.com/ericniebler/range-v3/issues/1022),
  [\#1043](https://github.com/ericniebler/range-v3/issues/1043),
  [\#1081](https://github.com/ericniebler/range-v3/issues/1081),
  [\#1085](https://github.com/ericniebler/range-v3/issues/1085),
  [\#1101](https://github.com/ericniebler/range-v3/issues/1101),
  [\#1116](https://github.com/ericniebler/range-v3/issues/1116),
  [\#1296](https://github.com/ericniebler/range-v3/issues/1296),
  [\#1305](https://github.com/ericniebler/range-v3/issues/1305), and
  [\#1335](https://github.com/ericniebler/range-v3/issues/1335).

Many thanks to GitHub users @CaseyCarter, @morinmorin, @h-2, @MichaelWJung,
@johelegp, @marehr, @alkino, @xuning97, @BRevzin, and @mpusz for their
contributions.

\section v0-9-1 Version 0.9.1

_Released:_ Sept 1, 2019.

gcc-9.x portability fixes.

\section v0-9-0 Version 0.9.0 "Std::ranger Things"

_Released:_ Aug 26, 2019.

Bring many interfaces into sync with the C++20 draft.

* **NEW:** An improved concepts portability layer with macros that use C++20
  concepts when the compiler supports them.
* **NEW:** An improved directory structure that keeps disjoint parts of the
  library -- iterators, ranges, algorithms, actions, views, functional
  programming support, and general utilities -- physically separate.
* **NEW:** A `RANGES_DEEP_STL_INTEGRATION` configuration option that makes your
  STL implementation default to structural conformance to infer iterator
  category, as in C++20. Applies to libc++, libstdc++, and MSVC's Standard
  Library.
* **NEW:** A `ranges::cpp20` namespace that contains all the functionality of
  C++20's `std::ranges` namespace.
* All concept names have been given standard_case (renamed from PascalCase) and
  have been harmonized with the C++20 draft.
* The following range access customization points no longer accept rvalue ranges
  by default:
  - `ranges::begin`
  - `ranges::end`
  - `ranges::rbegin`
  - `ranges::rend`
  - `ranges::cbegin`
  - `ranges::cend`
  - `ranges::crbegin`
  - `ranges::crend`
  - `ranges::data`
  - `ranges::cdata`
* Iterators may specify an `iterator_concept` type alias in addition to
  `iterator_category` -- either as a nested type or as a member of a
  `std::iterator_traits` specialization -- to denote conformance to the C++20
  iterator concepts as distinct from the C++98 iterator requirements.
  (See [P1037 "Deep Integration of the Ranges TS"](http://wg21.link/p1037)
  for more information.)
* The `ranges::value_type` trait has been renamed to `readable_traits`.
* The `ranges::difference_type` trait has been renamed to `incrementable_traits`.
* The `ranges::iterator_category` trait has been deprecated. Specialize
  `std::iterator_traits` to non-intrusively specify an iterator's category
  and (optionally) concept.
* Rename the `ranges::view` namespace to `ranges::views` and `ranges::action` to
  `ranges::actions` (with deprecated namespace aliases for migration).
* Rename `view::bounded` to `views::common`.
* Rename `unreachable` to `unreachable_sentinel_t`.
* Change `dangling` from a class template that wraps an iterator to a class that
  acts as a placeholder for an iterator that would otherwise dangle.
* Implement C++20's `subrange` as a view that wraps an iterator/sentinel pair;
  deprecate `iterator_range`.
* Deprecate implicit conversion from view types to containers; rename
  `ranges::to_` to `ranges::to` and extend it to support converting a
  range-of-ranges to a container-of-containers.
* Deprecate the `ranges::v3` inline versioning namespace.
* The following views have had minor changes to bring them into conformance with
  the C++20 working draft:
  - `join_view`
  - `single_view`
  - `empty_view`
  - `split_view`
  - `reverse_view`
  - `all_view`
  - `take_view`
  - `iota_view`
  <p/>`iota_view<std::[u]intmax_t>`, in particular, is given a user-defined
  `difference_type` that avoids integer overflow.
* New names for the iterator and range type aliases:
  | Old Name                      | New Name                    |
  |-------------------------------|-----------------------------|
  | `value_type_t`                | `iter_value_t`              |
  | `reference_t`                 | `iter_reference_t`          |
  | `difference_type_t`           | `iter_difference_t`         |
  | `size_type_t`                 | _deprecated_                |
  | `rvalue_reference_t`          | `iter_rvalue_reference_t`   |
  | `range_value_type_t`          | `range_value_t`             |
  | `range_difference_type_t`     | `range_difference_t`        |
  | `range_size_type_t`           | `range_size_t`              |

\section v0-5-0 Version 0.5.0

_Released:_ Apr 30, 2019.

* **NEW:** MSVC support, from @CaseyCarter :tada: (See the docs for the list of supported compilers.)
* **NEW:** `view::enumerate`, from @MikeGitb
* **NEW:** `view::addressof`, from @tower120
* **NEW:** `unstable_remove_if` algorithm and action, from @tower120
* **NEW:** `adjacent_remove_if` algorithm and action, from @cjdb
* **NEW:** `ostream_joiner`, from @sv1990
* `view::drop_while` and `view::take_while` get projection support, from @mrpi
* `view::filter` and `view::remove_if` get projection support, from @mrpi
* `view::unique` accepts optional comparison operator, from @tete17
* `action::slice` supports sliding from the end, from @tete17
* Support coroutines on MSVC, from @CaseyCarter
* Faster `view::generate_n`, from GitHub user @tower120
* Improved aligned new detection for libc++ on iOS, from @mtak-
* Various CMake improvements, from @johelegp
* `view_adaptor` supports `basic_iterator`-style mixins, from @tower120
* Fix `ranges::advance` for random-access iterators for `n==0`, from @tower120
* Bugs fixed: [#755](https://github.com/ericniebler/range-v3/issues/755), [#759](https://github.com/ericniebler/range-v3/issues/759), [#942](https://github.com/ericniebler/range-v3/issues/942), [#946](https://github.com/ericniebler/range-v3/issues/946), [#952](https://github.com/ericniebler/range-v3/issues/952), [#975](https://github.com/ericniebler/range-v3/issues/975), [#978](https://github.com/ericniebler/range-v3/issues/978), [#986](https://github.com/ericniebler/range-v3/issues/986), [#996](https://github.com/ericniebler/range-v3/issues/996), [#1041](https://github.com/ericniebler/range-v3/issues/1041), [#1047](https://github.com/ericniebler/range-v3/issues/1047), [#1088](https://github.com/ericniebler/range-v3/issues/1088), [#1094](https://github.com/ericniebler/range-v3/issues/1094), [#1107](https://github.com/ericniebler/range-v3/issues/1107), [#1129](https://github.com/ericniebler/range-v3/issues/1129)

\section v0-4-0 Version 0.4.0

_Released:_ Oct 18, 2018.

- Minor interface-breaking changes:
  * `single_view` returns by `const &` (see [#817](https://github.com/ericniebler/range-v3/issues/817)).
  * `reverse_view` of a non-Sized, non-Bounded RandomAccess range (eg., a null-terminated string) no longer satisfies SizedRange.
  * The `generate` and `generate_n` views now return the generated values by xvalue reference (`T &&`) to the value cached within the view (see [#905](https://github.com/ericniebler/range-v3/issues/905)).
  * Views no longer prefer returning constant iterators when they can; some views have different constant and mutable iterators.
- Enhancements:
  * Views can successfully adapt other views that have different constant and mutable iterators.
  * The `single` and `empty` views are much closer to the versions as specified in [P0896](http://wg21.link/P0896).
- Bug fixes:
  * "single_view should not copy the value" [#817](https://github.com/ericniebler/range-v3/issues/817).
  * "Calling back() on strided range does not return the correct last value in range" [#901](https://github.com/ericniebler/range-v3/issues/901).
  * "generate(foo) | take(n) calls foo n+1 times" [#819](https://github.com/ericniebler/range-v3/issues/819).
  * "generate seems broken with move-only return types" [#905](https://github.com/ericniebler/range-v3/issues/905).
  * "Unexpected behavior in generate with return by reference" [#807](https://github.com/ericniebler/range-v3/issues/807).
  * "Inconsistent behaviour of ranges::distance with ranges::view::zip using infinite views." [#783](https://github.com/ericniebler/range-v3/issues/783).
  * "Infinite loop when using ranges::view::cycle with an infinite range" [#780](https://github.com/ericniebler/range-v3/issues/780).
  * "Composing ranges::view::cycle with ranges::view::slice" [#778](https://github.com/ericniebler/range-v3/issues/778).
  * "cartesian_product view, now with moar bugs." [#919](https://github.com/ericniebler/range-v3/issues/919).


\section v0-3-7 Version 0.3.7

_Released:_ Sept 19, 2018.

- Improved support for clang-cl (thanks to @CaseyCarter).
- Fix for `any_view<T, category::sized | category::input>` (see #869).
- Fix `iter_move` of a `ranges::reverse_iterator` (see #888).
- Fix `move_sentinel` comparisons (see #889).
- Avoid ambiguity created by `boost::advance` and `std::advance` (see #893).

\section v0-3-6 Version 0.3.6

_Released:_ May 15, 2018.

- NEW: `view::exclusive_scan` (thanks to GitHub user @mitsutaka-takeda).
- All views get non-`const` overloads of `.empty()` and `.size()` (see [ericniebler/stl2\#793](https://github.com/ericniebler/stl2/issues/793)).
- Upgrade Conan support for conan 1.0.
- `subspan` interface tweaks.
- Fix bug in `view::split` (see [this stackoverflow question](https://stackoverflow.com/questions/49015671)).
- Fix bug in `view::stride` (see [ericniebler/stl2\#805](https://github.com/ericniebler/stl2/issues/805)).
- Fix `const`-correctness problem in `view::chunk` (see [this stackoverflow question](https://stackoverflow.com/questions/49210190)).
- Replace uses of `ranges::result_of` with `ranges::invoke_result`.
- Fix potential buffer overrun of `view::drop` over RandomAccessRanges.
- Lots of `view::cartesian_product` fixes (see [ericniebler/stl2\#820](https://github.com/ericniebler/stl2/issues/820), [ericniebler/stl2\#823](https://github.com/ericniebler/stl2/issues/823)).
- Work around gcc-8 regression regarding `volatile` `std::initializer_list`s (see [ericniebler/stl2\#826](https://github.com/ericniebler/stl2/issues/826)).
- Fix `const`-correctness problem of `view::take`.

\section v0-3-5 Version 0.3.5

_Released:_ February 17, 2018.

- Rvalues may satisfy `Writable` (see [ericniebler/stl2\#387](https://github.com/ericniebler/stl2/issues/387)).
- `view_interface` gets a bounds-checking `at` method.
- `chunk_view` works on Input ranges.
- Fix bug in `group_by_view`.
- Improved concept checks for `partial_sum` numeric algorithm.
- Define `ContiguousIterator` concept and `contiguous_iterator_tag` iterator
  category tag.
- Sundry `span` fixes.
- `action::insert` avoids interfering with `vector`'s exponentional growth
  strategy.
- Add an experimental `shared` view for views that need container-like scratch
  space to do their work.
- Faster, simpler `reverse_view`.
- Rework `ranges::reference_wrapper` to avoid [LWG\#2993](https://wg21.link/lwg2993).
- Reworked `any_view`, the type-erased view wrapper.
- `equal` algorithm is `constexpr` in C++14.
- `stride_view` no longer needs an `atomic` data member.
- `const`-correct `drop_view`.
- `adjacent_filter_view` supports bidirectional iteration.
- Massive `view_adaptor` cleanup to remove the need for a `mutable` data
  member holding the adapted view.
- Fix `counting_iterator` post-increment bug.
- `tail_view` of an empty range is an empty range, not undefined behavior.
- Various portability fixes for gcc and clang trunk.

\section v0-3-0 Version 0.3.0

_Released:_ June 30, 2017.

- Input views may now be move-only (from @CaseyCarter)
- Input `any_view`s are now *much* more efficient (from @CaseyCarter)
- Better support for systems lacking a working `<thread>` header (from @CaseyCarter)

\section v0-2-6 Version 0.2.6

_Released:_ June 21, 2017.

- Experimental coroutines with `ranges::experimental::generator` (from @CaseyCarter)
- `ranges::optional` now behaves like `std::optional` (from @CaseyCarter)
- Extensive bug fixes with Input ranges (from @CaseyCarter)

\section v0-2-5 Version 0.2.5

_Released:_ May 16, 2017.

- `view::chunk` works on Input ranges (from @CaseyCarter)
- `for_each_n` algorithm (from @khlebnikov)
- Portability fixes for MinGW, clang-3.6 and -3.7, and gcc-7; and cmake 3.0

\section v0-2-4 Version 0.2.4

_Released:_ April 12, 2017.

Fix the following bug:
- `action::stable_sort` of `vector` broken on Clang 3.8.1 since ~last Xmas (ericniebler/range-v3#632).

\section v0-2-3 Version 0.2.3

_Released:_ April 4, 2017.

Fix the following bug:
- iterators that return move-only types by value do not satisfy Readable (ericniebler/stl2#399).

\section v0-2-2 Version 0.2.2

_Released:_ March 30, 2017.

New in this release:
- `view::linear_distribute(from,to,n)` - A view of `n` elements between `from` and `to`, distributed evenly.
- `view::indices(n)` - A view of the indices `[0,1,2...n-1]`.
- `view::closed_indices(n)` - A view of the indices `[0,1,2...n]`.

This release deprecates `view::ints(n)` as confusing to new users.

\section v0-2-1 Version 0.2.1

_Released:_ March 22, 2017.

New in this release:
- `view::cartesian_product`
- `action::reverse`

\section v0-2-0 Version 0.2.0

_Released:_ March 13, 2017.

Bring many interfaces into sync with the Ranges TS.
- Many interfaces are simply renamed. The following table shows the old names
  and the new. (All names are in the `ranges::v3` namespace.)
  | Old Name                      | New Name                  |
  |-------------------------------|---------------------------|
  | `indirect_swap`               | `iter_swap`               |
  | `indirect_move`               | `iter_move`               |
  | `iterator_value_t`            | `value_type_t`            |
  | `iterator_reference_t`        | `reference_t`             |
  | `iterator_difference_t`       | `difference_type_t`       |
  | `iterator_size_t`             | `size_type_t`             |
  | `iterator_rvalue_reference_t` | `rvalue_reference_t`      |
  | `iterator_common_reference_t` | `iter_common_reference_t` |
  | `range_value_t`               | `range_value_type_t`      |
  | `range_difference_t`          | `range_difference_type_t` |
  | `range_size_t`                | `range_size_type_t`       |
  | `range_iterator_t`            | `iterator_t`              |
  | `range_sentinel_t`            | `sentinel_t`              |
- `common_iterator` now requires that its two types (`Iterator` and `Sentinel`)
  are different. Use `common_iterator_t<I, S>` to get the old behavior (i.e., if the two types are the same, it is an alias for `I`; otherwise, it is
  `common_iterator<I, S>`).
- The following iterator adaptors now work with iterators that return proxies
  from their postfix increment operator (i.e., `operator++(int)`):
  * `common_iterator`
  * `counted_iterator`
- The following customization points are now implemented per the Ranges TS
  spec and will no longer find the associated unconstrained overload in
  namespace `std::`:
  * `ranges::begin`
  * `ranges::end`
  * `ranges::size`
  * `ranges::swap`
  * `ranges::iter_swap`

  (In practice, this has very little effect but it may effect overloading in
  rare situations.)
- `ranges::is_swappable` now only takes one template parameter. The new
  `ranges::is_swappable_with<T, U>` tests whether `T` and `U` are swappable.
  `ranges::is_swappable<T>` is equivalent to `ranges::is_swappable_with<T &, T &>`.
- The following object concepts have changed to conform with the Ranges TS
  specification, and approved changes (see [P0547](http://wg21.link/p0547)):
  * `Destructible`
  * `Constructible`
  * `DefaultConstructible`
  * `MoveConstructible`
  * `MoveConstructible`
  * `Movable`
  * `Assignable`
- The `View` concept is no longer satisfied by reference types.
- The syntax for defining a concept has changed slightly. See [iterator/concepts.hpp](https://github.com/ericniebler/range-v3/blob/master/include/range/v3/iterator/concepts.hpp) for examples.

\section v0-1-1 Version 0.1.1

- Small tweak to `Writable` concept to fix #537.

\section v0-1-0 Version 0.1.0

- March 8, 2017, Begin semantic versioning
