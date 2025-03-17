cmake_minimum_required(VERSION 3.5)

project(tsl_hat_trie_tests)

add_executable(tsl_hat_trie_tests "main.cpp" 
                                  "trie_map_tests.cpp" 
                                  "trie_set_tests.cpp")

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    target_compile_options(tsl_hat_trie_tests PRIVATE -std=c++11 -Werror -Wall -Wextra -Wold-style-cast -O3 -DTSL_DEBUG)
elseif(CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
    target_compile_options(tsl_hat_trie_tests PRIVATE /bigobj /WX /W3 /DTSL_DEBUG)
endif()

# Boost::unit_test_framework
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.54.0 REQUIRED COMPONENTS unit_test_framework)
target_link_libraries(tsl_hat_trie_tests PRIVATE Boost::unit_test_framework)   

# tsl::hat_trie
add_subdirectory(../ ${CMAKE_CURRENT_BINARY_DIR}/tsl)
target_link_libraries(tsl_hat_trie_tests PRIVATE tsl::hat_trie)  
