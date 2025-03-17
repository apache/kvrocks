#define CATCH_CONFIG_MAIN

#include <jsoncons/config/jsoncons_config.hpp>
#include <catch/catch.hpp>

TEST_CASE("configuration")
{
    #if defined(__clang__) 
        std::cout << "clang" << "\n";
    #elif defined(__GNUC__)
        std::cout << "__GNUC__: " << __GNUC__ << "\n";
        #if defined(__GNUC_MINOR__)
            std::cout << "__GNUC_MINOR__" << __GNUC_MINOR__ << "\n";
        #endif
    #endif
    #if defined(JSONCONS_HAS_STD_REGEX)
        std::cout << "JSONCONS_HAS_STD_REGEX\n";
    #endif
}
