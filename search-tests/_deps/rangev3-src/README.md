range-v3
========

Range library for C++14/17/20. This code was the basis of [a formal proposal](https://ericniebler.github.io/std/wg21/D4128.html) to add range support to the C++ standard library. That proposal evolved through a Technical Specification, and finally into [P0896R4 "The One Ranges Proposal"](https://wg21.link/p0896r4) which was merged into the C++20 working drafts in November 2018.

About:
------

Ranges are an extension of the Standard Template Library that makes its iterators and algorithms more powerful by making them _composable_. Unlike other range-like solutions which seek to do away with iterators, in range-v3 ranges are an abstration layer _on top_ of iterators.

Range-v3 is built on three pillars: Views, Actions, and Algorithms. The algorithms are the same as those with which you are already familiar in the STL, except that in range-v3 all the algorithms have overloads that take ranges in addition to the overloads that take iterators. Views are composable adaptations of ranges where the adaptation happens lazily as the view is iterated. And an action is an eager application of an algorithm to a container that mutates the container in-place and returns it for further processing.

Views and actions use the pipe syntax (e.g., `rng | adapt1 | adapt2 | ...`) so your code is terse and readable from left to right.

Documentation:
--------------

Check out the (woefully incomplete) documentation [here](https://ericniebler.github.io/range-v3/).

Other resources (mind the dates, the library probably has changed since then):

- Usage:
  - Talk: [CppCon 2015: Eric Niebler "Ranges for the Standard Library"](https://www.youtube.com/watch?v=mFUXNMfaciE), 2015.
  - [A slice of Python in C++](http://ericniebler.com/2014/12/07/a-slice-of-python-in-c/), 07.12.2014.
  - Actions (back then called [Container Algorithms](http://ericniebler.com/2014/11/23/container-algorithms/)), 23.11.2014.
  - [Range comprehensions](http://ericniebler.com/2014/04/27/range-comprehensions/), 27.04.2014.
  - [Input iterators vs input ranges](http://ericniebler.com/2013/11/07/input-iterators-vs-input-ranges/), 07.11.2013.

- Design / Implementation:
  - Rationale behind range-v3: [N4128: Ranges for the standard library Revision 1](http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2014/n4128.html), 2014.
  - Ranges TS: [N4560: C++ Extensions for Ranges](http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2015/n4560.pdf), 2015.
  - Implementation of customization points in range-v3:
    - [N4381: Suggested Design for Customization Points](http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2015/n4381.html), 2015.
    - [P0386: Inline variables](http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2016/p0386r0.pdf), 2016.
    - [Customization Point Design in C++11 and Beyond](http://ericniebler.com/2014/10/21/customization-point-design-in-c11-and-beyond/), 2014.
  - Proxy iterators in range-v3:
    - [D0022: Proxy Iterators for the Ranges Extensions](https://ericniebler.github.io/std/wg21/D0022.html).
    - [To Be or Not to Be (an Iterator)](http://ericniebler.com/2015/01/28/to-be-or-not-to-be-an-iterator/), 2015.
    - [Iterators++: Part1](http://ericniebler.com/2015/02/03/iterators-plus-plus-part-1/), 2015.
    - [Iterators++: Part2](http://ericniebler.com/2015/02/13/iterators-plus-plus-part-2/), 2015.
    - [Iterators++: Part3](http://ericniebler.com/2015/03/03/iterators-plus-plus-part-3/), 2015.
  - Metaprogramming utilities:
    - See the [meta documentation](https://ericniebler.github.io/meta/index.html), the library has changed significantly since the [2014 blog post](http://ericniebler.com/2014/11/13/tiny-metaprogramming-library/).
  - Concept emulation layer: [Concept checking in C++11](http://ericniebler.com/2013/11/23/concept-checking-in-c11/), 2013.
  - [C++Now 2014: Eric Niebler "C++11 Library Design"](https://www.youtube.com/watch?v=zgOF4NrQllo), 2014.

License:
--------

Most of the source code in this project are mine, and those are under the Boost Software License. Parts are taken from Alex Stepanov's Elements of Programming, Howard Hinnant's libc++, and from the SGI STL. Please see the attached LICENSE file and the CREDITS file for the licensing and acknowledgments.

Supported Compilers
-------------------

The code is known to work on the following compilers:

- clang 5.0 (or later)
- GCC 6.5 (or later)
- Clang/LLVM 6 (or later) on Windows (older versions may work - we haven't tested.)
- Visual Studio 2019 (or later) on Windows, with some caveats due to range-v3's strict conformance requirements:
  - range-v3 needs `/permissive-` and either `/std:c++latest`, `/std:c++20`,  or `/std:c++17`

**Development Status:** This code is fairly stable, well-tested, and suitable for casual use, although currently lacking documentation. _In general_, no promise is made about support or long-term stability. This code *will* evolve without regard to backwards compatibility.

A notable exception is anything found within the `ranges::cpp20` namespace. Those components will change rarely or (preferably) never at all.

**Build status**
- on GitHub Actions: [![GitHub Actions Status](https://github.com/ericniebler/range-v3/workflows/range-v3%20CI/badge.svg?branch=master)](https://github.com/ericniebler/range-v3/actions)

Building range-v3 - Using vcpkg
-------------------------------

You can download and install range-v3 using the [vcpkg](https://github.com/Microsoft/vcpkg) dependency manager:

    git clone https://github.com/Microsoft/vcpkg.git
    cd vcpkg
    ./bootstrap-vcpkg.sh
    ./vcpkg integrate install
    ./vcpkg install range-v3

The range-v3 port in vcpkg is kept up to date by Microsoft team members and community contributors. If the version is out of date, please [create an issue or pull request](https://github.com/Microsoft/vcpkg) on the vcpkg repository.

Building range-v3 - Using Conan
-------------------------------

You can download and install range-v3 using the [Conan](https://github.com/conan-io/conan) dependency manager.

Setup your CMakeLists.txt (see [Conan documentation](https://docs.conan.io/en/latest/integrations/build_system.html) on how to use MSBuild, Meson and others):
```
project(myproject CXX)

add_executable(${PROJECT_NAME} main.cpp)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake) # Include Conan-generated file
conan_basic_setup(TARGETS) # Introduce Conan-generated targets

target_link_libraries(${PROJECT_NAME} CONAN_PKG::range-v3)
```
Create `conanfile.txt` in your source dir:
```
[requires]
range-v3/0.12.0

[generators]
cmake
```
Install and run `conan`, then build your project as always:
```
pip install conan
mkdir build
cd build
conan install ../ --build=missing
cmake ../
cmake --build .
```

Building range-v3 - Using `build2`
----------------------------------

You can use [`build2`](https://build2.org), a dependency manager and a build-system combined, to use `range-v3` (or work on it):

Currently this package is available in these package repositories:
 - **https://cppget.org/range-v3/** for released and published versions.
 - [**The git repository with the sources of the `build2` package of `range-v3`**](https://github.com/build2-packaging/range-v3.git) for unreleased or custom revisions of `range-v3`, or for working on it with `build2`.

### Usage:

 - `build2` package name: `range-v3`
 - Library target name : `lib{range-v3}`
 - [Detailed use cases and instructions in this document](https://github.com/build2-packaging/range-v3/NOTES-build2.md).

For example, to make your `build2` project depend on `range-v3`:
  - Add one of the repositories to your configurations, or in your `repositories.manifest`, if not already there; for example:
    ```
    :
    role: prerequisite
    location: https://pkg.cppget.org/1/alpha # v0.11.0 is there.
    ```
  - Add this package as a dependency to your `manifest` file (example for `v0.11.x`):
    ```
    depends: range-v3 ~0.11.0
    ```
  - Import the target and use it as a prerequisite to your own target using `range-v3` in the appropriate `buildfile`:
    ```
    import range_v3 = range-v3%lib{range-v3}

    lib{mylib} : cxx{**} ... $range_v3
    ```

Then just build your project as usual (with `b` or `bdep update`), `build2` will figure out the rest.

For `build2` newcomers or to get more details and use cases, you can read [this document](https://github.com/build2-packaging/range-v3/NOTES-build2.md) and the [`build2` toolchain introduction](https://build2.org/build2-toolchain/doc/build2-toolchain-intro.xhtml).


Say Thanks!
-----------

I do this work because I love it and because I love C++ and want it to be as excellent as I know it can be. If you like my work and are looking for a way to say thank you, you can leave a supportive comment on [my blog](http://ericniebler.com). Or you could leave me some kudos on my Open Hub range-v3 contribution page. Just click the **Give Kudos** button [here](https://www.openhub.net/p/range-v3/contributors/3053743222308608).
