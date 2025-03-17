# Copyright Louis Dionne 2015
# Copyright Gonzalo Brito Gadeschi 2015
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE.md or copy at http://boost.org/LICENSE_1_0.txt)
#
# Setup compiler flags (more can be set on a per-target basis or in
# subdirectories)

# Enable all warnings:
ranges_append_flag(RANGES_HAS_WEVERYTHING -Weverything)
ranges_append_flag(RANGES_HAS_PEDANTIC -pedantic)
ranges_append_flag(RANGES_HAS_PEDANTIC_ERRORS -pedantic-errors)

# Selectively disable those warnings that are not worth it:
ranges_append_flag(RANGES_HAS_WNO_CXX98_COMPAT -Wno-c++98-compat)
ranges_append_flag(RANGES_HAS_WNO_CXX98_COMPAT_PEDANTIC -Wno-c++98-compat-pedantic)
ranges_append_flag(RANGES_HAS_WNO_WEAK_VTABLES -Wno-weak-vtables)
ranges_append_flag(RANGES_HAS_WNO_PADDED -Wno-padded)
ranges_append_flag(RANGES_HAS_WNO_MISSING_VARIABLE_DECLARATIONS -Wno-missing-variable-declarations)
ranges_append_flag(RANGES_HAS_WNO_DOCUMENTATION -Wno-documentation)
ranges_append_flag(RANGES_HAS_WNO_DOCUMENTATION_UNKNOWN_COMMAND -Wno-documentation-unknown-command)
ranges_append_flag(RANGES_HAS_WNO_OLD_STYLE_CAST -Wno-old-style-cast)

if (RANGES_ENV_MACOSX)
  ranges_append_flag(RANGES_HAS_WNO_GLOBAL_CONSTRUCTORS -Wno-global-constructors)
  ranges_append_flag(RANGES_HAS_WNO_EXIT_TIME_DESTRUCTORS -Wno-exit-time-destructors)
endif()

if (RANGES_CXX_COMPILER_CLANG OR RANGES_CXX_COMPILER_CLANGCL)
  ranges_append_flag(RANGES_HAS_WNO_MISSING_PROTOTYPES -Wno-missing-prototypes)
endif()

if (RANGES_CXX_COMPILER_GCC)
  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "6.0")
    ranges_append_flag(RANGES_HAS_WNO_STRICT_OVERFLOW -Wno-strict-overflow)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "5.0")
      ranges_append_flag(RANGES_HAS_WNO_MISSING_FIELD_INITIALIZERS -Wno-missing-field-initializers)
    endif()
  elseif ((CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "7.0") OR (CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL "7.0"))
    ranges_append_flag(RANGES_HAS_WNO_NOEXCEPT_TYPE -Wno-noexcept-type)
  endif()
endif()

if (RANGES_VERBOSE_BUILD)
  message(STATUS "[range-v3]: test C++ flags: ${CMAKE_CXX_FLAGS}")
endif()
