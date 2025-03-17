if (CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    project(jsoncons-test)

    find_package(jsoncons REQUIRED CONFIG)
    set(JSONCONS_INCLUDE_DIR ${jsoncons_INCLUDE_DIRS})
endif ()

if(NOT CMAKE_BUILD_TYPE)
message(STATUS "Forcing tests build type to Release")
set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
endif()

include(CheckCXXCompilerFlag)

string(TOUPPER "${CMAKE_BUILD_TYPE}" U_CMAKE_BUILD_TYPE)

OPTION(JSONCONS_SANITIZE "sanitize" OFF)
option(JSONCONS_VALGRIND "Execute tests with valgrind" OFF)

if(JSONCONS_VALGRIND)
    find_program(CMAKE_MEMORYCHECK_COMMAND valgrind)
    message(STATUS "Executing test suite with Valgrind (${CMAKE_MEMORYCHECK_COMMAND})")
    set(memcheck_command "${CMAKE_MEMORYCHECK_COMMAND} ${CMAKE_MEMORYCHECK_COMMAND_OPTIONS} --error-exitcode=1 --leak-check=full")
    separate_arguments(memcheck_command)
endif()
 
include (CTest)

# load per-platform configuration
# include (${JSONCONS_PROJECT_DIR}/cmake/${CMAKE_SYSTEM_NAME}.cmake)

# -fsanitize=undefined clang issue
if (JSONCONS_SANITIZE)
    if(NOT MSVC)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fsanitize=address -fno-omit-frame-pointer")
        message(STATUS "SANITIZE ON")
    endif()
endif()

if(MSVC)
    set(CMAKE_EXE_LINKER_FLAGS /MANIFEST:NO)
endif()

set(CTEST_OUTPUT_ON_FAILURE ON)

set(JSONCONS_TESTS_DIR ${JSONCONS_PROJECT_DIR}/test)
set(JSONCONS_INCLUDE_DIR ${JSONCONS_PROJECT_DIR}/include)
set(JSONCONS_THIRD_PARTY_INCLUDE_DIR ${JSONCONS_TESTS_DIR}/thirdparty)

set(CATCH_INCLUDE_DIR ${JSONCONS_THIRD_PARTY_INCLUDE_DIR}/catch)
add_library(catch INTERFACE)

if (${CMAKE_VERSION} VERSION_LESS "3.8.0")
    target_compile_features(catch INTERFACE cxx_range_for)  # for C++11 - flags
else()
    target_compile_features(catch INTERFACE cxx_std_11)
endif()

target_include_directories (catch INTERFACE ${CATCH_INCLUDE_DIR})

add_executable(unit_tests 
               bson/src/bson_cursor_tests.cpp
               bson/src/bson_encoder_tests.cpp
               bson/src/bson_reader_tests.cpp
               bson/src/bson_decimal128_tests.cpp
               bson/src/bson_oid_tests.cpp
               bson/src/bson_test_suite.cpp
               bson/src/encode_decode_bson_tests.cpp
               cbor/src/cbor_bitset_traits_tests.cpp
               cbor/src/cbor_cursor_tests.cpp
               cbor/src/cbor_event_reader_tests.cpp
               cbor/src/cbor_encoder_tests.cpp
               cbor/src/cbor_event_visitor_tests.cpp
               cbor/src/cbor_reader_tests.cpp
               cbor/src/cbor_tests.cpp
               cbor/src/cbor_typed_array_tests.cpp
               cbor/src/decode_cbor_tests.cpp
               cbor/src/encode_cbor_tests.cpp
               csv/src/csv_reader_tests.cpp
               csv/src/csv_cursor_tests.cpp
               csv/src/csv_subfield_tests.cpp
               csv/src/csv_tests.cpp
               csv/src/encode_decode_csv_tests.cpp
               jmespath/src/jmespath_custom_function_tests.cpp
               jmespath/src/jmespath_expression_tests.cpp
               jmespath/src/jmespath_let_tests.cpp
               jmespath/src/jmespath_tests.cpp
               mergepatch/src/mergepatch_test_suite.cpp
               jsonpatch/src/jsonpatch_test_suite.cpp
               jsonpatch/src/jsonpatch_tests.cpp
               jsonpath/src/jsonpath_flatten_tests.cpp
               jsonpath/src/jsonpath_function_tests.cpp
               jsonpath/src/path_node_tests.cpp
               jsonpath/src/json_location_tests.cpp
               jsonpath/src/json_location_parser_tests.cpp
               jsonpath/src/jsonpath_custom_function_tests.cpp
               jsonpath/src/jsonpath_json_query_tests.cpp
               jsonpath/src/jsonpath_expression_tests.cpp
               jsonpath/src/jsonpath_json_replace_tests.cpp
               jsonpath/src/jsonpath_select_paths_tests.cpp
               jsonpath/src/jsonpath_test_suite.cpp
               jsonpath/src/jsonpath_stateful_allocator_tests.cpp
               jsonpointer/src/jsonpointer_flatten_tests.cpp
               jsonpointer/src/jsonpointer_tests.cpp
               jsonschema/src/abort_early_tests.cpp
               jsonschema/src/custom_message_tests.cpp
               jsonschema/src/json_schema_walk_tests.cpp
               jsonschema/src/format_validator_tests.cpp
               jsonschema/src/validation_report_tests.cpp
               jsonschema/src/dynamic_ref_tests.cpp
               jsonschema/src/jsonschema_defaults_tests.cpp
               jsonschema/src/jsonschema_draft4_tests.cpp
               jsonschema/src/jsonschema_draft6_tests.cpp
               jsonschema/src/jsonschema_draft7_tests.cpp
               jsonschema/src/jsonschema_draft201909_tests.cpp
               jsonschema/src/jsonschema_draft202012_tests.cpp
               jsonschema/src/schema_version_tests.cpp
               msgpack/src/decode_msgpack_tests.cpp
               msgpack/src/encode_msgpack_tests.cpp
               msgpack/src/msgpack_bitset_traits_tests.cpp
               msgpack/src/msgpack_cursor_tests.cpp
               msgpack/src/msgpack_event_reader_tests.cpp
               msgpack/src/msgpack_encoder_tests.cpp
               msgpack/src/msgpack_tests.cpp
               msgpack/src/msgpack_timestamp_tests.cpp
               corelib/src/source_adaptor_tests.cpp
               corelib/src/value_converter_tests.cpp
               corelib/src/decode_traits_tests.cpp
               corelib/src/detail/optional_tests.cpp
               corelib/src/detail/span_tests.cpp
               corelib/src/detail/string_view_tests.cpp
               corelib/src/utility/byte_string_tests.cpp
               corelib/src/utility/heap_string_tests.cpp
               corelib/src/utility/bigint_tests.cpp
               corelib/src/utility/uri_tests.cpp
               corelib/src/utility/extension_traits_tests.cpp
               corelib/src/utility/unicode_conv_tests.cpp
               corelib/src/detail/to_integer_tests.cpp
               corelib/src/double_round_trip_tests.cpp
               corelib/src/double_to_string_tests.cpp
               corelib/src/dtoa_tests.cpp
               corelib/src/encode_decode_json_tests.cpp
               corelib/src/encode_traits_tests.cpp
               corelib/src/json_parser_recovery_tests.cpp
               corelib/src/json_array_tests.cpp
               corelib/src/json_as_tests.cpp
               corelib/src/json_bitset_traits_tests.cpp
               corelib/src/json_checker_tests.cpp
               corelib/src/json_compare_tests.cpp
               corelib/src/json_const_pointer_arg_tests.cpp
               corelib/src/json_pointer_arg_tests.cpp
               corelib/src/json_assignment_tests.cpp
               corelib/src/json_constructor_tests.cpp
               corelib/src/json_cursor_tests.cpp
               corelib/src/json_encoder_tests.cpp
               corelib/src/json_exception_tests.cpp
               corelib/src/json_filter_tests.cpp
               corelib/src/json_in_place_update_tests.cpp
               corelib/src/json_integer_tests.cpp
               corelib/src/json_less_tests.cpp
               corelib/src/json_line_split_tests.cpp
               corelib/src/json_literal_operator_tests.cpp
               corelib/src/json_object_tests.cpp
               corelib/src/ojson_object_tests.cpp
               corelib/src/json_options_tests.cpp
               corelib/src/json_parser_error_tests.cpp
               corelib/src/json_parser_position_tests.cpp
               corelib/src/json_parser_tests.cpp
               corelib/src/json_push_back_tests.cpp
               corelib/src/json_reader_exception_tests.cpp
               corelib/src/json_reader_tests.cpp
               corelib/src/json_storage_tests.cpp
               corelib/src/json_swap_tests.cpp
               corelib/src/json_traits_macro_functional_tests.cpp
               corelib/src/json_traits_macro_tests.cpp
               corelib/src/json_traits_macro_limit_tests.cpp
               corelib/src/json_traits_name_macro_tests.cpp
               corelib/src/json_type_traits_chrono_tests.cpp
               corelib/src/json_type_traits_container_tests.cpp
               corelib/src/json_type_traits_tests.cpp
               corelib/src/json_validation_tests.cpp 
               corelib/src/jsoncons_tests.cpp
               corelib/src/JSONTestSuite_tests.cpp
               corelib/src/ojson_tests.cpp
               corelib/src/parse_string_tests.cpp
               corelib/src/short_string_tests.cpp
               corelib/src/source_tests.cpp
               corelib/src/staj_iterator_tests.cpp
               corelib/src/polymorphic_allocator_tests.cpp
               corelib/src/scoped_allocator_adaptor_tests.cpp
               corelib/src/string_to_double_tests.cpp
               corelib/src/wjson_tests.cpp
               ubjson/src/decode_ubjson_tests.cpp
               ubjson/src/encode_ubjson_tests.cpp
               ubjson/src/ubjson_cursor_tests.cpp
               ubjson/src/ubjson_encoder_tests.cpp
               corelib/src/testmain.cpp
)              

if (${CMAKE_VERSION} VERSION_LESS "3.8.0")
    target_compile_features(unit_tests INTERFACE cxx_range_for)  # for C++11 - flags
else()
    target_compile_features(unit_tests INTERFACE cxx_std_11)
endif()

target_compile_options(unit_tests PRIVATE
    $<$<CXX_COMPILER_ID:MSVC>: /EHsc /MP /bigobj /W4>
)

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "8.0")
    message(STATUS Version " ${CMAKE_CXX_COMPILER_VERSION}")
    # older GCC versions don't support 
   target_compile_options(unit_tests PRIVATE
       $<$<CXX_COMPILER_ID:GNU>:-Werror=maybe-uninitialized -Wnon-virtual-dtor -Werror=stringop-overflow -Werror -Wall -Wextra -Wcast-align -Wcast-qual -Wimplicit-fallthrough -Wsign-compare -pedantic -Wnonnull -Wnull-dereference -Werror=nonnull>
   )
endif()

if (NOT CMAKE_SYSTEM_NAME STREQUAL Windows)
target_compile_options(unit_tests PRIVATE
    $<$<CXX_COMPILER_ID:Clang>:-Werror=uninitialized -Werror -Wall -Wextra -Wimplicit-fallthrough -Wcast-align -Wcast-qual -Wsign-compare -pedantic>
)
endif()

add_test(NAME unit_tests COMMAND unit_tests WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test)

target_include_directories (unit_tests 
                            PUBLIC ${JSONCONS_INCLUDE_DIR} 
                            PRIVATE ${JSONCONS_TESTS_DIR}
                            PRIVATE ${JSONCONS_THIRD_PARTY_INCLUDE_DIR})

target_link_libraries(unit_tests catch)

