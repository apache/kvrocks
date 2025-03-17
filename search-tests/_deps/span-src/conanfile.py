from conans import ConanFile, CMake

class SpanLiteConan(ConanFile):
    version = "0.11.0"
    name = "span-lite"
    description = "A C++20-like span for C++98, C++11 and later in a single-file header-only library"
    license = "Boost Software License - Version 1.0. http://www.boost.org/LICENSE_1_0.txt"
    url = "https://github.com/martinmoene/span-lite.git"
    exports_sources = "include/nonstd/*", "CMakeLists.txt", "cmake/*", "LICENSE.txt"
    settings = "compiler", "build_type", "arch"
    build_policy = "missing"
    author = "Martin Moene"

    def build(self):
        """Avoid warning on build step"""
        pass

    def package(self):
        """Run CMake install"""
        cmake = CMake(self)
        cmake.definitions["SPAN_LITE_OPT_BUILD_TESTS"] = "OFF"
        cmake.definitions["SPAN_LITE_OPT_BUILD_EXAMPLES"] = "OFF"
        cmake.configure()
        cmake.install()

    def package_info(self):
        self.info.header_only()
