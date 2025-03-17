// Copyright (c) 2018-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <iostream>
#include <utility>

int main()
{
#ifdef __GNUC__
   std::cout << "__GNUC__: " << __GNUC__ << std::endl;
   std::cout << "__GNUC_MINOR__: " << __GNUC_MINOR__ << std::endl;
   std::cout << "__GNUC_PATCHLEVEL__: " << __GNUC_PATCHLEVEL__ << std::endl;
#endif
#ifdef __GLIBCPP__
   std::cout << "__GLIBCPP__: " << __GLIBCPP__ << std::endl;
#endif
#ifdef __GLIBCXX__
   std::cout << "__GLIBCXX__: " << __GLIBCXX__ << std::endl;
#endif

#ifdef __clang__
   std::cout << "__clang__: " << __clang__ << std::endl;
   std::cout << "__clang_major__: " << __clang_major__ << std::endl;
   std::cout << "__clang_minor__: " << __clang_minor__ << std::endl;
   std::cout << "__clang_patchlevel__: " << __clang_patchlevel__ << std::endl;
   std::cout << "__clang_version__: " << __clang_version__ << std::endl;
#endif
#ifdef __apple_build_version__
   std::cout << "__apple_build_version__: " << __apple_build_version__ << std::endl;
#endif
#ifdef _LIBCPP_VERSION
   std::cout << "_LIBCPP_VERSION: " << _LIBCPP_VERSION << std::endl;
#endif

#ifdef _MSC_VER
   std::cout << "_MSC_VER: " << _MSC_VER << std::endl;
   std::cout << "_MSC_FULL_VER: " << _MSC_FULL_VER << std::endl;
#endif
   return 0;
}
