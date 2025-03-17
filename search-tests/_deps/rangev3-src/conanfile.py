# Range v3 library
#
#  Copyright Luis Martinez de Bartolome Izquierdo 2016
#
#  Use, modification and distribution is subject to the
#  Boost Software License, Version 1.0. (See accompanying
#  file LICENSE_1_0.txt or copy at
#  http://www.boost.org/LICENSE_1_0.txt)
#
# Project home: https://github.com/ericniebler/range-v3
#

from conans import ConanFile, CMake

class Rangev3Conan(ConanFile):
    name = "range-v3"
    version = "0.12.0"
    license = "Boost Software License - Version 1.0 - August 17th, 2003"
    url = "https://github.com/ericniebler/range-v3"
    description = """Experimental range library for C++14/17/20"""
    # No settings/options are necessary, this is header only
    exports_sources = "include*", "LICENSE.txt", "CMakeLists.txt", "cmake/*", "Version.cmake", "version.hpp.in"
    no_copy_source = True

    def package(self):
        cmake = CMake(self)
        cmake.definitions["RANGE_V3_TESTS"] = "OFF"
        cmake.definitions["RANGE_V3_EXAMPLES"] = "OFF"
        cmake.definitions["RANGE_V3_PERF"] = "OFF"
        cmake.definitions["RANGE_V3_DOCS"] = "OFF"
        cmake.definitions["RANGE_V3_HEADER_CHECKS"] = "OFF"
        cmake.configure()
        cmake.install()

        self.copy("LICENSE.txt", dst="licenses", ignore_case=True, keep_path=False)
