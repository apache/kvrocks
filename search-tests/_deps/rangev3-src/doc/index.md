User Manual       {#mainpage}
===========

\tableofcontents

\section tutorial-preface Preface

--------------------------------------------
Range library for C++14/17/20. This code is the basis of [the range support in C++20](http://eel.is/c++draft/#ranges).

**Development Status:**

This code is fairly stable, well-tested, and suitable for casual use, although
currently lacking documentation. No promise is made about support or long-term
stability. This code *will* evolve without regard to backwards compatibility.

A notable exception is anything found within the `ranges::cpp20` namespace.
Those components will change rarely or (preferably) never at all.

\subsection tutorial-installation Installation

--------------------------------------------
This library is header-only. You can get the source code from the
[range-v3 repository](https://github.com/ericniebler/range-v3) on github. To
compile with Range-v3, just `#include` the individual headers you want.

This distribution actually contains three separate header-only libraries:

* <strong><tt>include/concepts/...</tt></strong> contains the Concepts Portability Preprocessor, or
  CPP, which is a set of macros for defining and using concept checks,
  regardless of whether your compiler happens to support the C++20 concepts
  language feature or not.
* <strong><tt>include/meta/...</tt></strong> contains the Meta Library, which is a set of
  meta-programming utilities for processing types and lists of types at compile
  time.
* <strong><tt>include/range/...</tt></strong> contains the Range-v3 library, as described below.

The Range-v3 library is physically structured in directories by feature group:

* <strong><tt>include/range/v3/actions/...</tt></strong> contains _actions_, or composable
  components that operate eagerly on containers and return the mutated container
  for further actions.
* <strong><tt>include/range/v3/algorithms/...</tt></strong> contains all the STL _algorithms_ with
  overloads that accept ranges, in addition to the familiar overloads that take iterators.
* <strong><tt>include/range/v3/functional/...</tt></strong> contains many generally useful
  components that would be familiar to functional programmers.
* <strong><tt>include/range/v3/iterator/...</tt></strong> contains the definitions of many useful
  iterators and iterator-related concepts and utilities.
* <strong><tt>include/range/v3/numeric/...</tt></strong> contains numeric algorithms corresponding
  to those found in the standard `<numeric>` header.
* <strong><tt>include/range/v3/range/...</tt></strong> contains range-related utilities, such as
  `begin`, `end`, and `size`, range traits and concepts, and conversions to
  containers.
* <strong><tt>include/range/v3/utility/...</tt></strong> contains a miscellaneous assortment of
  reusable code.
* <strong><tt>include/range/v3/view/...</tt></strong> contains _views_, or composable
  components that operate lazily on ranges and that themselves return ranges
  that can be operated upon with additional view adaptors.

\subsection tutorial-license License

--------------------------------------------
Most of the source code in this project are mine, and those are under the Boost
Software License. Parts are taken from Alex Stepanov's Elements of Programming,
Howard Hinnant's libc++, and from the SGI STL. Please see the attached LICENSE
file and the CREDITS file for the licensing and acknowledgements.

\subsection tutorial-compilers Supported Compilers

--------------------------------------------------------------------------------
The code is known to work on the following compilers:

- clang 5.0
- GCC 6.5
- Clang/LLVM 6 (or later) on Windows
- MSVC VS2019, with `/permissive-` and either `/std:c++latest`, `/std:c++20`, or `/std:c++17`

\section tutorial-quick-start Quick Start

--------------------------------------------------------------------------------
Range-v3 is a generic library that augments the existing standard library with
facilities for working with *ranges*. A range can be loosely thought of a pair
of iterators, although they need not be implemented that way. Bundling begin/end
iterators into a single object brings several benefits: convenience,
composability, and correctness.

**Convenience**

It's more convenient to pass a single range object to an algorithm than separate
begin/end iterators. Compare:

~~~~~~~{.cpp}
    std::vector<int> v{/*...*/};
    std::sort( v.begin(), v.end() );
~~~~~~~

with

~~~~~~~{.cpp}
    std::vector<int> v{/*...*/};
    ranges::sort( v );
~~~~~~~

Range-v3 contains full implementations of all the standard algorithms with
range-based overloads for convenience.

**Composability**

Having a single range object permits *pipelines* of operations. In a pipeline, a
range is lazily adapted or eagerly mutated in some way, with the result
immediately available for further adaptation or mutation. Lazy adaption is
handled by *views*, and eager mutation is handled by *actions*.

For instance, the below uses _views_ to filter a container using a predicate
and transform the resulting range with a function. Note that the underlying
data is `const` and is not mutated by the views.

~~~~~~~{.cpp}
    std::vector<int> const vi{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    using namespace ranges;
    auto rng = vi | views::remove_if([](int i){ return i % 2 == 1; })
                  | views::transform([](int i){ return std::to_string(i); });
    // rng == {"2","4","6","8","10"};
~~~~~~~

In the code above, `rng` simply stores a reference to the underlying data and
the filter and transformation functions. No work is done until `rng` is
iterated.

In contrast, _actions_ do their work eagerly, but they also compose. Consider
the code below, which reads some data into a vector, sorts it, and makes it
unique.

~~~~~~~{.cpp}
    extern std::vector<int> read_data();
    using namespace ranges;
    std::vector<int> vi = read_data() | actions::sort | actions::unique;
~~~~~~~

Unlike views, with actions each step in the pipeline (`actions::sort` and
`actions::unique`) accepts a container _by value_, mutates it in place, and
returns it.

**Correctness**

Whether you are using views or actions, you are operating on data in a pure
functional, declarative style. You rarely need to trouble yourself with
iterators, although they are there under the covers should you need them.

By operating declaratively and functionally instead of imperatively, we reduce
the need for overt state manipulation and branches and loops. This brings down
the number of states your program can be in, which brings down your bug counts.

In short, if you can find a way to express your solution as a composition of
functional transformations on your data, you can make your code _correct by
construction_.

\subsection tutorial-views Views

--------------------------------------------------------------------------------
As described above, the big advantage of ranges over iterators is their
composability. They permit a functional style of programming where data is
manipulated by passing it through a series of combinators. In addition, the
combinators can be *lazy*, only doing work when the answer is requested, and
*purely functional*, without mutating the original data. This makes it easier to
reason about your code.

A _view_ is a lightweight wrapper that presents a view of an underlying sequence
of elements in some custom way without mutating or copying it. Views are cheap
to create and copy and have non-owning reference semantics. Below are some
examples that use views:

Filter a container using a predicate and transform it.

~~~~~~~{.cpp}
    std::vector<int> const vi{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    using namespace ranges;
    auto rng = vi | views::remove_if([](int i){return i % 2 == 1;})
                  | views::transform([](int i){return std::to_string(i);});
    // rng == {"2","4","6","8","10"};
~~~~~~~

Generate an infinite list of integers starting at 1, square them, take the first
10, and sum them:

~~~~~~~{.cpp}
    using namespace ranges;
    int sum = accumulate(views::ints(1)
                       | views::transform([](int i){return i*i;})
                       | views::take(10), 0);
~~~~~~~

Generate a sequence on the fly with a range comprehension and initialize a
vector with it:

~~~~~~~{.cpp}
    using namespace ranges;
    auto vi =
        views::for_each(views::ints(1, 10), [](int i) {
            return yield_from(views::repeat_n(i, i));
        })
      | to<std::vector>();
    // vi == {1,2,2,3,3,3,4,4,4,4,5,5,5,5,5,...}
~~~~~~~

### View const-ness

Logically, a view is a factory for iterators, but in practice a view is often
implemented as a state machine, with the state stored within the view object
itself (to keep iterators small) and mutated as the view is iterated. Because
the view contains mutable state, many views lack a `const`-qualified
`begin()`/`end()`. When `const` versions of `begin()`/`end()` are provided, they
are truly `const`; that is, thread-safe.

Since views present the same interface as containers, the temptation is to think
they behave like containers with regard to `const`-ness. This is not the case.
Their behavior with regards to `const`-ness is similar to iterators and
pointers.

The `const`-ness of a view is not related to the `const`-ness of the underlying
data. A non-`const` view may refer to elements that are themselves `const`, and
_vice versa_. This is analogous to pointers; an `int* const` is a `const`
pointer to a mutable `int`, and a `int const*` is a non-`const` pointer to a
`const` `int`.

Use non-`const` views whenever possible. If you need thread-safety, work with
view copies in threads; don't share.

### View validity

Any operation on the underlying range that invalidates its iterators or
sentinels will also invalidate any view that refers to any part of that range.
Additionally, some views (_e.g._, `views::filter`), are invalidated when the
underlying elements of the range are mutated. It is best to recreate a view
after any operation that may have mutated the underlying range.

### List of range views

Below is a list of the lazy range combinators, or views, that Range-v3
provides, and a blurb about how each is intended to be used.

<DL>
<DT>\link ranges::views::addressof_fn `views::addressof`\endlink</DT>
  <DD>Given a source range of lvalue references, return a new view that is the result of taking `std::addressof` of each.</DD>
<DT>\link ranges::views::adjacent_filter_fn `views::adjacent_filter`\endlink</DT>
  <DD>For each pair of adjacent elements in a source range, evaluate the specified binary predicate. If the predicate evaluates to false, the second element of the pair is removed from the result range; otherwise, it is included. The first element in the source range is always included. (For instance, `adjacent_filter` with `std::not_equal_to` filters out all the non-unique elements.)</DD>
<DT>\link ranges::views::adjacent_remove_if_fn `views::adjacent_remove_if`\endlink</DT>
  <DD>For each pair of adjacent elements in a source range, evaluate the specified binary predicate. If the predicate evaluates to true, the first element of the pair is removed from the result range; otherwise, it is included. The last element in the source range is always included.</DD>
<DT>\link ranges::views::all_fn `views::all`\endlink</DT>
  <DD>Return a range containing all the elements in the source. Useful for converting containers to ranges.</DD>
<DT>\link ranges::any_view `any_view<T>(rng)`\endlink</DT>
  <DD>Type-erased range of elements with value type `T`; can store _any_ range with this value type.</DD>
<DT>\link ranges::views::c_str_fn `views::c_str`\endlink</DT>
  <DD>View a `\0`-terminated C string (e.g. from a `const char*`) as a range.</DD>
<DT>\link ranges::views::cache1_fn `views::cache1`\endlink</DT>
  <DD>Caches the most recent element within the view so that dereferencing the view's iterator multiple times doesn't incur any recomputation. This can be useful in adaptor pipelines that include combinations of `view::filter` and `view::transform`, for instance. `views::cache1` is always single-pass.</DD>
<DT>\link ranges::views::cartesian_product_fn `views::cartesian_product`\endlink</DT>
  <DD>Enumerates the n-ary cartesian product of `n` ranges, i.e., generates all `n`-tuples `(e1, e2, ... , en)` where `e1` is an element of the first range, `e2` is an element of the second range, etc.</DD>
<DT>\link ranges::views::chunk_fn `views::chunk`\endlink</DT>
  <DD>Given a source range and an integer *N*, produce a range of contiguous ranges where each inner range has *N* contiguous elements. The final range may have fewer than *N* elements.</DD>
<DT>\link ranges::views::common_fn `views::common`\endlink</DT>
  <DD>Convert the source range to a *common* range, where the type of the `end` is the same as the `begin`. Useful for calling algorithms in the `std::` namespace.</DD>
<DT>\link ranges::views::concat_fn `views::concat`\endlink</DT>
  <DD>Given *N* source ranges, produce a result range that is the concatenation of all of them.</DD>
<DT>\link ranges::views::const_fn `views::const_`\endlink</DT>
  <DD>Present a `const` view of a source range.</DD>
<DT>\link ranges::views::counted_fn `views::counted`\endlink</DT>
  <DD>Given an iterator `it` and a count `n`, create a range that starts at `it` and includes the next `n` elements.</DD>
<DT>\link ranges::views::cycle_fn `views::cycle`\endlink</DT>
  <DD>Returns an infinite range that endlessly repeats the source range.</DD>
<DT>\link ranges::views::delimit_fn `views::delimit`\endlink</DT>
  <DD>Given a source range and a value, return a new range that ends either at the end of the source or at the first occurrence of the value, whichever comes first. Alternatively, `views::delimit` can be called with an iterator and a value, in which case it returns a range that starts at the specified position and ends at the first occurrence of the value.</DD>
<DT>\link ranges::views::drop_fn `views::drop`\endlink</DT>
  <DD>Given a source range and an integral count, return a range consisting of all but the first *count* elements from the source range, or an empty range if it has fewer elements.</DD>
<DT>\link ranges::views::drop_last_fn `views::drop_last`\endlink</DT>
  <DD>Given a source range and an integral count, return a range consisting of all but the last *count* elements from the source range, or an empty range if it has fewer elements.</DD>
<DT>\link ranges::views::drop_exactly_fn `views::drop_exactly`\endlink</DT>
  <DD>Given a source range and an integral count, return a range consisting of all but the first *count* elements from the source range. The source range must have at least that many elements.</DD>
<DT>\link ranges::views::drop_while_fn `views::drop_while`\endlink</DT>
  <DD>Remove elements from the front of a range that satisfy a unary predicate.</DD>
<DT>\link ranges::views::empty() `views::empty`\endlink</DT>
  <DD>Create an empty range with a given value type.</DD>
<DT>\link ranges::views::enumerate() `views::enumerate`\endlink</DT>
  <DD>Pair each element of a range with its index.</DD>
<DT>\link ranges::views::filter_fn `views::filter`\endlink</DT>
  <DD>Given a source range and a unary predicate, filter the elements that satisfy the predicate. (For users of Boost.Range, this is like the `filter` adaptor.)</DD>
<DT>\link ranges::views::for_each_fn `views::for_each`\endlink</DT>
  <DD>Lazily applies an unary function to each element in the source range that returns another range (possibly empty), flattening the result.</DD>
<DT>\link ranges::views::generate_fn `views::generate`\endlink</DT>
  <DD>Given a nullary function, return an infinite range whose elements are generated with the function.</DD>
<DT>\link ranges::views::generate_n_fn `views::generate_n`\endlink</DT>
  <DD>Given a nullary function and a count, return a range that generates the requested number of elements by calling the function.</DD>
<DT>\link ranges::views::chunk_by_fn `views::chunk_by`\endlink</DT>
  <DD>Given a source range and a binary predicate, return a range of ranges where each range contains contiguous elements from the source range such that the following condition holds: for each element in the range apart from the first, when that element and the previous element are passed to the binary predicate, the result is true. In essence, `views::chunk_by` groups contiguous elements together with a binary predicate.</DD>
<DT>\link ranges::views::indirect_fn `views::indirect`\endlink</DT>
  <DD>Given a source range of readable values (e.g. pointers or iterators), return a new view that is the result of dereferencing each.</DD>
<DT>\link ranges::views::intersperse_fn `views::intersperse`\endlink</DT>
  <DD>Given a source range and a value, return a new range where the value is inserted between contiguous elements from the source.</DD>
<DT>\link ranges::views::ints_fn `views::ints`\endlink</DT>
  <DD>Generate a range of monotonically increasing `int`s. When used without arguments, it generates the quasi-infinite range [0,1,2,3...]. It can also be called with a lower bound, or with a lower and upper bound (exclusive). An inclusive version is provided by `closed_ints`.</DD>
<DT>\link ranges::views::iota_fn `views::iota`\endlink</DT>
  <DD>A generalization of `views::ints` that generates a sequence of monotonically increasing values of any incrementable type. When specified with a single argument, the result is an infinite range beginning at the specified value. With two arguments, the values are assumed to denote a half-open range.</DD>
<DT>\link ranges::views::join_fn `views::join`\endlink</DT>
  <DD>Given a range of ranges, join them into a flattened sequence of elements. Optionally, you can specify a value or a range to be inserted between each source range.</DD>
<DT>\link ranges::views::keys_fn `views::keys`\endlink</DT>
  <DD>Given a range of `pair`s (like a `std::map`), return a new range consisting of just the first element of the `pair`.</DD>
<DT>\link ranges::views::linear_distribute_fn `views::linear_distribute`\endlink</DT>
  <DD>Distributes `n` values linearly in the closed interval `[from, to]` (the end points are always included). If `from == to`, returns `n`-times `to`, and if `n == 1` it returns `to`.</DD>
<DT>\link ranges::views::move_fn `views::move`\endlink</DT>
  <DD>Given a source range, return a new range where each element has been cast to an rvalue reference.</DD>
<DT>\link ranges::views::partial_sum_fn `views::partial_sum`\endlink</DT>
  <DD>Given a range and a binary function, return a new range where the *N*<SUP>th</SUP> element is the result of applying the function to the *N*<SUP>th</SUP> element from the source range and the (N-1)th element from the result range.</DD>
<DT>\link ranges::views::remove_fn `views::remove`\endlink</DT>
  <DD>Given a source range and a value, filter out those elements that do not equal value.</DD>
<DT>\link ranges::views::remove_if_fn `views::remove_if`\endlink</DT>
  <DD>Given a source range and a unary predicate, filter out those elements that do not satisfy the predicate. (For users of Boost.Range, this is like the `filter` adaptor with the predicate negated.)</DD>
<DT>\link ranges::views::repeat_fn `views::repeat`\endlink</DT>
  <DD>Given a value, create a range that is that value repeated infinitely.</DD>
<DT>\link ranges::views::repeat_n_fn `views::repeat_n`\endlink</DT>
  <DD>Given a value and a count, create a range that is that value repeated *count* number of times.</DD>
<DT>\link ranges::views::replace_fn `views::replace`\endlink</DT>
  <DD>Given a source range, a source value and a target value, create a new range where all elements equal to the source value are replaced with the target value.</DD>
<DT>\link ranges::views::replace_if_fn `views::replace_if`\endlink</DT>
  <DD>Given a source range, a unary predicate and a target value, create a new range where all elements that satisfy the predicate are replaced with the target value.</DD>
<DT>\link ranges::views::reverse_fn `views::reverse`\endlink</DT>
  <DD>Create a new range that traverses the source range in reverse order.</DD>
<DT>\link ranges::views::sample_fn `views::sample`\endlink</DT>
  <DD>Returns a random sample of a range of length `size(range)`.</DD>
<DT>\link ranges::views::single_fn `views::single`\endlink</DT>
  <DD>Given a value, create a range with exactly one element.</DD>
<DT>\link ranges::views::slice_fn `views::slice`\endlink</DT>
  <DD>Give a source range a lower bound (inclusive) and an upper bound (exclusive), create a new range that begins and ends at the specified offsets. Both the begin and the end can be integers relative to the front, or relative to the end with "`end-2`" syntax.</DD>
<DT>\link ranges::views::sliding_fn `views::sliding`\endlink</DT>
  <DD>Given a range and a count `n`, place a window over the first `n` elements of the underlying range. Return the contents of that window as the first element of the adapted range, then slide the window forward one element at a time until hitting the end of the underlying range.</DD>
<DT>\link ranges::views::split_fn `views::split`\endlink</DT>
  <DD>Given a source range and a delimiter specifier, split the source range into a range of ranges using the delimiter specifier to find the boundaries. The delimiter specifier can be an element or a range of elements. The elements matching the delimiter are excluded from the resulting range of ranges.</DD>
<DT>\link ranges::views::split_when_fn `views::split_when`\endlink</DT>
  <DD>Given a source range and a delimiter specifier, split the source range into a range of ranges using the delimiter specifier to find the boundaries. The delimiter specifier can be a predicate or a function. The predicate should take a single argument of the range's reference type and return `true` if and only if the element is part of a delimiter. The function should accept an iterator and sentinel indicating the current position and end of the source range and return `std::make_pair(true, iterator_past_the_delimiter)` if the current position is a boundary; otherwise `std::make_pair(false, ignored_iterator_value)`. The elements matching the delimiter are excluded from the resulting range of ranges.</DD>
<DT>\link ranges::views::stride_fn `views::stride`\endlink</DT>
  <DD>Given a source range and an integral stride value, return a range consisting of every *N*<SUP>th</SUP> element, starting with the first.</DD>
<DT>\link ranges::views::tail_fn `views::tail`\endlink</DT>
  <DD>Given a source range, return a new range without the first element. The range must have at least one element.</DD>
<DT>\link ranges::views::take_fn `views::take`\endlink</DT>
  <DD>Given a source range and an integral count, return a range consisting of the first *count* elements from the source range, or the complete range if it has fewer elements. (The result of `views::take` is not a `sized_range`.)</DD>
<DT>\link ranges::views::take_exactly_fn `views::take_exactly`\endlink</DT>
  <DD>Given a source range and an integral count, return a range consisting of the first *count* elements from the source range. The source range must have at least that many elements. (The result of `views::take_exactly` is a `sized_range`.)</DD>
<DT>\link ranges::views::take_last_fn `views::take_last`\endlink</DT>
  <DD>Given a source range and an integral count, return a range consisting of the last *count* elements from the source range. The source range must be a `sized_range`. If the source range does not have at least *count* elements, the full range is returned.</DD>
<DT>\link ranges::views::take_while_fn `views::take_while`\endlink</DT>
  <DD>Given a source range and a unary predicate, return a new range consisting of the  elements from the front that satisfy the predicate.</DD>
<DT>\link ranges::views::tokenize_fn `views::tokenize`\endlink</DT>
  <DD>Given a source range and optionally a submatch specifier and a `std::regex_constants::match_flag_type`, return a `std::regex_token_iterator` to step through the regex submatches of the source range. The submatch specifier may be either a plain `int`, a `std::vector<int>`, or a `std::initializer_list<int>`.</DD>
<DT>\link ranges::views::transform_fn `views::transform`\endlink</DT>
  <DD>Given a source range and a unary function, return a new range where each result element is the result of applying the unary function to a source element.</DD>
<DT>\link ranges::views::trim_fn `views::trim`\endlink</DT>
  <DD>Given a source bidirectional range and a unary predicate, return a new range without the front and back elements that satisfy the predicate.</DD>
<DT>\link ranges::views::unbounded_fn `views::unbounded`\endlink</DT>
  <DD>Given an iterator, return an infinite range that begins at that position.</DD>
<DT>\link ranges::views::unique_fn `views::unique`\endlink</DT>
  <DD>Given a range, return a new range where all consecutive elements that compare equal save the first have been filtered out.</DD>
<DT>\link ranges::views::values_fn `views::values`\endlink</DT>
  <DD>Given a range of `pair`s (like a `std::map`), return a new range consisting of just the second element of the `pair`.</DD>
<DT>\link ranges::views::zip_fn `views::zip`\endlink</DT>
  <DD>Given *N* ranges, return a new range where *M*<SUP>th</SUP> element is the result of calling `make_tuple` on the *M*<SUP>th</SUP> elements of all *N* ranges.</DD>
<DT>\link ranges::views::zip_with_fn `views::zip_with`\endlink</DT>
  <DD>Given *N* ranges and a *N*-ary function, return a new range where *M*<SUP>th</SUP> element is the result of calling the function on the *M*<SUP>th</SUP> elements of all *N* ranges.</DD>
</DL>

\subsection tutorial-actions Actions

--------------------------------------------------------------
When you want to mutate a container in-place, or forward it through a chain of
mutating operations, you can use actions. The following examples should make it
clear.

Read data into a vector, sort it, and make it unique.

~~~~~~~{.cpp}
    extern std::vector<int> read_data();
    using namespace ranges;
    std::vector<int> vi = read_data() | actions::sort | actions::unique;
~~~~~~~

Do the same to a `vector` that already contains some data:

~~~~~~~{.cpp}
    vi = std::move(vi) | actions::sort | actions::unique;
~~~~~~~

Mutate the container in-place:

~~~~~~~{.cpp}
    vi |= actions::sort | actions::unique;
~~~~~~~

Same as above, but with function-call syntax instead of pipe syntax:

~~~~~~~{.cpp}
    actions::unique(actions::sort(vi));
~~~~~~~

### List of range actions

Below is a list of the eager range combinators, or actions, that Range-v3
provides, and a blurb about how each is intended to be used.

<DL>
<DT>\link ranges::actions::drop_fn `actions::drop`\endlink</DT>
  <DD>Removes the first `N` elements of the source range.</DD>
<DT>\link ranges::actions::drop_while_fn `actions::drop_while`\endlink</DT>
  <DD>Removes the first elements of the source range that satisfy the unary predicate.</DD>
<DT>`actions::erase`</DT>
  <DD>Removes all elements in the sub-range of the source (range version) or all elements after position.</DD>
<DT>`actions::insert`</DT>
  <DD>Inserts all elements of the range into the source at position.</DD>
<DT>\link ranges::actions::join_fn `actions::join`\endlink</DT>
  <DD>Flattens a range of ranges.</DD>
<DT> `actions::push_back`</DT>
  <DD>Appends elements to the tail of the source.</DD>
<DT>`actions::push_front`</DT>
  <DD>Appends elements before the head of the source.</DD>
<DT>\link ranges::actions::remove_if_fn `actions::remove_if`\endlink</DT>
  <DD>Removes all elements from the source that satisfy the predicate.</DD>
<DT>\link ranges::actions::remove_fn `actions::remove`\endlink</DT>
  <DD>Removes all elements from the source that are equal to value.</DD>
<DT>\link ranges::actions::reverse_fn `actions::reverse`\endlink</DT>
  <DD>Reverses all the elements in the container.</DD>
<DT>\link ranges::actions::shuffle_fn `actions::shuffle`\endlink</DT>
  <DD>Shuffles the source range.</DD>
<DT>\link ranges::actions::slice_fn `actions::slice`\endlink</DT>
  <DD>Removes all elements from the source that are not part of the sub-range.</DD>
<DT>\link ranges::actions::sort_fn `actions::sort`\endlink</DT>
  <DD>Sorts the source range (unstable).</DD>
<DT>\link ranges::actions::split_fn `actions::split`\endlink</DT>
  <DD>Split a range into a sequence of subranges using a delimiter (a value, a sequence of values, a predicate, or a binary function returning a `pair<bool, N>`).</DD>
<DT>\link ranges::actions::stable_sort_fn `actions::stable_sort`\endlink</DT>
  <DD>Sorts the source range (stable).</DD>
<DT>\link ranges::actions::stride_fn `actions::stride`\endlink</DT>
  <DD>Removes all elements whose position does not match the stride.</DD>
<DT>\link ranges::actions::take_fn `actions::take`\endlink</DT>
  <DD>Keeps the first `N`-th elements of the range, removes the rest.</DD>
<DT>\link ranges::actions::take_while_fn `actions::take_while`\endlink</DT>
  <DD>Keeps the first elements that satisfy the predicate, removes the rest.</DD>
<DT>\link ranges::actions::transform_fn `actions::transform`\endlink</DT>
  <DD>Replaces elements of the source with the result of the unary function.</DD>
<DT>\link ranges::actions::unique_fn `actions::unique`\endlink</DT>
  <DD>Removes adjacent elements of the source that compare equal. If the source is sorted, removes all duplicate elements.</DD>
<DT>\link ranges::actions::unstable_remove_if_fn `actions::unstable_remove_if`\endlink</DT>
  <DD>Much faster (each element remove has constant time complexity), unordered version of `remove_if`. Requires bidirectional container.</DD>
</DL>


\subsection tutorial-utilities Utilities

----------------------------------------------
Below we cover some utilities that range-v3 provides for creating your own
view adaptors and iterators.

#### Create Custom Views with view_facade

Range-v3 provides a utility for easily creating your own range types, called
\link ranges::view_facade `ranges::view_facade`\endlink. The code below uses
`view_facade` to create a range that traverses a null-terminated string:

~~~~~~~{.cpp}
    #include <range/v3/view/facade.hpp>

    // A range that iterates over all the characters in a
    // null-terminated string.
    class c_string_range
      : public ranges::view_facade<c_string_range>
    {
        friend ranges::range_access;
        char const * sz_ = "";
        char const & read() const { return *sz_; }
        bool equal(ranges::default_sentinel_t) const { return *sz_ == '\0'; }
        void next() { ++sz_; }
    public:
        c_string_range() = default;
        explicit c_string_range(char const *sz) : sz_(sz)
        {
            assert(sz != nullptr);
        }
    };
~~~~~~~

The `view_facade` class generates an iterator and begin/end member functions
from the minimal interface provided by `c_string_range`. This is an example of a
very simple range for which it is not necessary to separate the range itself
from the thing that iterates the range. Future examples will show examples of
more sophisticated ranges.

With `c_string_range`, you can now use algorithms to operate on null-terminated
strings, as below:

~~~~~~~{.cpp}
    #include <iostream>
    #include <range/v3/algorithm/for_each.hpp>

    int main()
    {
        c_string_range r("hello world");
        // Iterate over all the characters and print them out
        ranges::for_each(r, [](char ch){
            std::cout << ch << ' ';
        });
        // prints: h e l l o   w o r l d
    }
~~~~~~~

#### Create Custom Views with view_adaptor

Often, a new range type is most easily expressed by adapting an existing range
type. That's the case for many of the range views provided by the Range-v3
library; for example, the `views::remove_if` and `views::transform` views. These
are rich types with many moving parts, but thanks to a helper class called
\link ranges::view_adaptor `ranges::view_adaptor`\endlink, they aren't hard
to write.

Below in roughly 2 dozen lines of code is the `transform` view, which takes one
range and transforms all the elements with a unary function.

~~~~~~~{.cpp}
    #include <range/v3/view/adaptor.hpp>
    #include <range/v3/utility/semiregular_box.hpp>

    // A class that adapts an existing range with a function
    template<class Rng, class Fun>
    class transform_view
      : public ranges::view_adaptor<transform_view<Rng, Fun>, Rng>
    {
        friend ranges::range_access;
        ranges::semiregular_box_t<Fun> fun_; // Make Fun model semiregular if it doesn't
        class adaptor : public ranges::adaptor_base
        {
            ranges::semiregular_box_t<Fun> fun_;
        public:
            adaptor() = default;
            adaptor(ranges::semiregular_box_t<Fun> const &fun) : fun_(fun) {}
            // Here is where we apply Fun to the elements:
            auto read(ranges::iterator_t<Rng> it) const -> decltype(fun_(*it))
            {
                return fun_(*it);
            }
        };
        adaptor begin_adaptor() const { return {fun_}; }
        adaptor end_adaptor() const { return {fun_}; }
    public:
        transform_view() = default;
        transform_view(Rng && rng, Fun fun)
          : transform_view::view_adaptor{std::forward<Rng>(rng)}
          , fun_(std::move(fun))
        {}
    };

    template<class Rng, class Fun>
    transform_view<Rng, Fun> transform(Rng && rng, Fun fun)
    {
        return {std::forward<Rng>(rng), std::move(fun)};
    }
~~~~~~~

Range transformation is achieved by defining a nested `adaptor` class that
handles the transformation, and then defining `begin_adaptor` and `end_adaptor`
members that return adaptors for the begin iterator and the end sentinel,
respectively. The `adaptor` class has a `read` member that performs the
transformation. It is passed an iterator to the current element. Other members
are available for customization: `equal`, `next`, `prev`, `advance`, and
`distance_to`; but the transform adaptor accepts the defaults defined in
\link ranges::adaptor_base `ranges::adaptor_base`\endlink.

With `transform_view`, we can print out the first 20 squares:

~~~~~~~{.cpp}
    int main()
    {
        auto squares = ::transform(views::ints(1), [](int i){return i*i;});
        for(int i : squares | views::take(20))
            std::cout << i << ' ';
        std::cout << '\n';
        // prints 1 4 9 16 25 36 49 64 81 100 121 144 169 196 225 256 289 324 361 400
    }
~~~~~~~

The `transform_view` defined above is an input range when it is wrapping an
input range, a forward range when it's wrapping a forward range, etc. That happens
because of smart defaults defined in the `adaptor_base` class that frees you
from having to deal with a host of niggly detail when implementing iterators.

*(Note: the above `transform_view` always stores a copy of the function in the
sentinel. That is only necessary if the underlying range's sentinel type models
bidirectional_iterator. That's a finer point that you shouldn't worry about right
now.)*

##### view_adaptor in details

Each `view_adaptor` contains `base()` member in view and iterator.
`base()` - allow to access "adapted" range/iterator:

~~~~~~~{.cpp}
    std::vector<int> vec;
    auto list = vec | views::transfom([](int i){ return i+1; });

    assert( vec.begin() == list.begin().base() );
    assert( vec.begin() == list.base().begin() );
~~~~~~~

Like `basic_iterator`'s `cursor`, `view_adaptor`'s `adaptor` can contain mixin class too,
to inject things into the public interface of the iterator:

~~~~~~~{.cpp}
    class adaptor : public ranges::adaptor_base
    {
        template<class BaseMixin>
        struct mixin : BaseMixin
        {
              // everything inside this class will be accessible from iterator
              using BaseMixin::BaseMixin;

              auto& base_value() const
              {
                  return *this->base();
              }

              int get_i() const
              {
                  return this->get().i;
              }
        };

        int i = 100;
    };
~~~~~~~

From within mixin you can call:

* `get()` - to access adaptor internals
* `base()` - to access adaptable iterator

Iterator/sentinel adaptor may "override" the following members:

~~~~~~~{.cpp}
    class adaptor : public ranges::adaptor_base
    {
        // !For begin_adaptor only!
        template<typename Rng>
        constexpr auto begin(Rng &rng)
        {
            return ranges::begin(rng.base());
        }

        // !For end_adaptor only!
        template<typename Rng>
        constexpr auto end(Rng &rng)
        {
            return ranges::end(rng.base());
        }

        template<typename I>
        bool equal(I const &this_iter, I const &that_iter) const
        {
            return this_iter == that_iter;
        }
        // or
        template<typename I>
        bool equal(I const &this_iter, I const &that_iter, adaptor const &that_adapt) const
        {
            return
              *this.some_value == that_adapt.some_value
              && this_iter == that_iter;
        }

        // !For end_adaptor only!
        // Same as equal, but compare iterator with sentinel.
        // Not used, if iterator same as sentinel, and both have the same adaptor.
        template<typename I, typename S>
        constexpr bool empty(I const &it, S const &end) const
        {
            return it == end;
        }
        // or
        template<typename I, typename S, typename SA>
        constexpr bool empty(I const &it, S const &end, SA const &end_adapt) const
        {
            return
              *this.some_value == end_adapt.some_value
              && it == end;
        }

        template<typename I>
        reference_t<I> read(I const &it)
        {
            return *it;
        }

        template<typename I>
        void next(I &it)
        {
            ++it;
        }

        // !For bidirectional iterator only!
        template<typename I>
        void prev(I &it)
        {
            --it;
        }

        // !For random access iterator only!
        template<typename I>
        void advance(I &it, difference_type_t<I> n)
        {
            it += n;
        }

        // !For "sized" iterators only!
        template<typename I>
        difference_type_t<I> distance_to(I const &this_iter, I const &that_iter)
        {
            return that_iter - this_iter;
        }
        // or
        template<typename I>
        difference_type_t<I> distance_to
            (I const &this_iter, I const &that_iter, adaptor const &that_adapt)
        {
            return that_iter - this_iter;
        }
    }
~~~~~~~

As you can see, some "overrides" have effect only for `begin_adaptor` or
`end_adaptor`. In order to use full potential of adaptor, you need to have
separate adaptors for begin and end:

~~~~~~~{.cpp}
    struct adaptor : adaptor_base
    {
        int n = 0;

        void next(iterator_t<Rng>& it)
        {
            ++n;
            ++it;
        }
    };

    struct sentinel_adaptor : adaptor_base
    {
        int stop_at;
        bool empty(const iterator_t<Rng>&, const adaptor& ia, const sentinel_t<Rng>& s) const
        {
            return ia.n == stop_at;
        }
    };

    adaptor begin_adaptor() const { return {}; }
    sentinel_adaptor end_adaptor() const { return {100}; }
~~~~~~~

Sometimes, you can use the same adaptor for both `begin_adaptor` and `end_adaptor`:

~~~~~~~{.cpp}
    struct adaptor : adaptor_base
    {
        int n = 0;
        void next(iterator_t<Rng>& it)
        {
            ++n;
            ++it;
        }

        // pay attention, we use equal, not empty. empty() will never trigger.
        template<typename I>
        bool equal(I const &this_iter, I const &that_iter, adaptor const &that_adapt) const
        {
            return *this.n == that_adapt.n;
        }
    };

    adaptor begin_adaptor() const { return {}; }
    adaptor end_adaptor()   const { return {100}; }
~~~~~~~

Note that all the data you store in the adaptor will become part of the iterator.

If you will not "override" `begin_adaptor()` or/and `end_adaptor()` in your view_adaptor, default ones will be used.

#### Create Custom Iterators with basic_iterator

Here is an example of Range-v3 compatible random access proxy iterator.
The iterator returns a key/value pair, like the `zip` view.

~~~~~~~{.cpp}
    #include <range/v3/iterator/basic_iterator.hpp>
    #include <range/v3/utility/common_tuple.hpp>

    using KeyIter   = typename std::vector<Key>::iterator;
    using ValueIter = typename std::vector<Value>::iterator;

    struct cursor
    {
        // basic_iterator derives from "mixin", if present, so it can be used
        // to inject things into the public interface of the iterator
        struct mixin;

        // This is for dereference operator.
        using value_type = std::pair<Key, Value>;
        ranges::common_pair<Key&, Value&> read() const
        {
            return { *key_iterator, *value_iterator };
        }

        bool equal(const cursor& other) const
        {
            return key_iterator == other.key_iterator;
        }

        void next()
        {
            ++key_iterator;
            ++value_iterator;
        }

        // prev optional. Required for Bidirectional iterator
        void prev()
        {
            --key_iterator;
            --value_iterator;
        }

        // advance and distance_to are optional. Required for random access iterator
        void advance(std::ptrdiff_t n)
        {
            key_iterator   += n;
            value_iterator += n;
        }
        std::ptrdiff_t distance_to(const cursor& other) const
        {
            return other.key_iterator - this->key_iterator;
        }

        cursor() = default;
        cursor(KeyIter key_iterator, ValueIter value_iterator)
          : key_iterator(key_iterator)
          , value_iterator(value_iterator)
        {}

        KeyIter   key_iterator;
        ValueIter value_iterator;
    };

    struct cursor::mixin : ranges::basic_mixin<cursor>
    {
        using ranges::basic_mixin<cursor>::basic_mixin;

        // It is necessary to expose constructor in this way
        mixin(KeyIter key_iterator, ValueIter value_iterator)
          : mixin{ cursor(key_iterator, value_iterator) }
        {}

        KeyIter key_iterator()
        {
            return this->get().key_iterator;
        }
        ValueIter value_iterator()
        {
            return this->get().value_iterator;
        }
    };

    using iterator = ranges::basic_iterator<cursor>;

    void test()
    {
        std::vector<Key>   keys   = {1};
        std::vector<Value> values = {10};

        iterator iter(keys.begin(), values.begin());
        ranges::common_pair<Key&, Value&> pair = *iter;
        Key&   key   = pair.first;
        Value& value = pair.second;

        assert(&key   == &keys[0]);
        assert(&value == &values[0]);

        auto key_iter = iter.key_iterator();
        assert(key_iter == keys.begin());
    }
~~~~~~~

`read()` returns references. But the default for `value_type`, which is
`decay_t<decltype(read())>`, is `common_pair<Key&, Value&>`. That is not correct
in our case. It should be `pair<Key, Value>`, so we explicitly specify
`value_type`.

`ranges::common_pair` has conversions:

> `ranges::common_pair<Key&, Value&>` &harr; `ranges::common_pair<Key, Value>`.

All `ranges::common_pair`s converts to their `std::pair` equivalents, also.

For more information, see [http://wg21.link/P0186#basic-iterators-iterators.basic](http://wg21.link/P0186#basic-iterators-iterators.basic)

\subsection tutorial-concepts Concept Checking

--------------------------------------------------------------------------------
The Range-v3 library makes heavy use of concepts to constrain functions, control
overloading, and check type constraints at compile-time. It achieves this with
the help of a Concepts emulation layer that works on any standard-conforming
C++14 compiler. The library provides many useful concepts, both for the core
language and for iterators and ranges. You can use the concepts framework to
constrain your own code.

For instance, if you would like to write a function that takes an
iterator/sentinel pair, you can write it like this:

~~~~~~~{.cpp}
    CPP_template(class Iter, class Sent, class Comp = /*...some_default..*/)
        (requires sentinel_for<Sent, Iter>)
    void my_algorithm(Iter first, Sent last, Comp comp = Comp{})
    {
        // ...
    }
~~~~~~~

You can then add an overload that take a Range:

~~~~~~~{.cpp}
    CPP_template(class Rng, class Comp = /*...some_default..*/)
        (requires range<Rng>)
    void my_algorithm(Rng && rng, Comp comp = Comp{})
    {
        return my_algorithm(ranges::begin(rng), ranges::end(rng));
    }
~~~~~~~

With the type constraints expressed with the `CPP_template` macro, these
two overloads are guaranteed to not be ambiguous. When compiling with C++20
concepts support, this uses real concept checks. On legacy compilers, it falls
back to using `std::enable_if`.

\subsection tutorial-future Range-v3 and the Future

--------------------------------------------------------------------------------
Range-v3 formed the basis for the
[Technical Specification on Ranges](https://www.iso.org/standard/70910.html),
which has since been merged into the working draft and shipped with C++20 in
the `std::ranges` namespace.

More range adaptors are slated for inclusion in C++23 and beyond.

The actions, as well as various utilities, have not yet been reviewed by the
Committee, although the basic direction has already passed an initial review.

