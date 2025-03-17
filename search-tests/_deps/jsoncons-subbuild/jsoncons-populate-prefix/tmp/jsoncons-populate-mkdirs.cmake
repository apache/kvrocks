# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION ${CMAKE_VERSION}) # this file comes with cmake

# If CMAKE_DISABLE_SOURCE_CHANGES is set to true and the source directory is an
# existing directory in our source tree, calling file(MAKE_DIRECTORY) on it
# would cause a fatal error, even though it would be a no-op.
if(NOT EXISTS "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/jsoncons-src")
  file(MAKE_DIRECTORY "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/jsoncons-src")
endif()
file(MAKE_DIRECTORY
  "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/jsoncons-build"
  "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/jsoncons-subbuild/jsoncons-populate-prefix"
  "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/jsoncons-subbuild/jsoncons-populate-prefix/tmp"
  "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/jsoncons-subbuild/jsoncons-populate-prefix/src/jsoncons-populate-stamp"
  "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/jsoncons-subbuild/jsoncons-populate-prefix/src"
  "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/jsoncons-subbuild/jsoncons-populate-prefix/src/jsoncons-populate-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/jsoncons-subbuild/jsoncons-populate-prefix/src/jsoncons-populate-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/Users/vivek-w/Downloads/GSOC/kvrocks/search-tests/_deps/jsoncons-subbuild/jsoncons-populate-prefix/src/jsoncons-populate-stamp${cfgdir}") # cfgdir has leading slash
endif()
