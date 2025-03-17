set(CMAKE_FOLDER "example")

add_subdirectory(view)

# examples use a less draconian set of compiler warnings
ranges_append_flag(RANGES_HAS_WNO_MISSING_BRACES -Wno-missing-braces)
ranges_append_flag(RANGES_HAS_WNO_SHORTEN_64_TO_32 -Wno-shorten-64-to-32)
ranges_append_flag(RANGES_HAS_WNO_GLOBAL_CONSTRUCTORS -Wno-global-constructors)

rv3_add_test(example.comprehensions comprehensions comprehensions.cpp)
rv3_add_test(example.hello hello hello.cpp)
rv3_add_test(example.count count count.cpp)
rv3_add_test(example.count_if count_if count_if.cpp)
rv3_add_test(example.any_all_none_of any_all_none_of any_all_none_of.cpp)
rv3_add_test(example.for_each_sequence for_each_sequence for_each_sequence.cpp)
rv3_add_test(example.for_each_assoc for_each_assoc for_each_assoc.cpp)
rv3_add_test(example.is_sorted is_sorted is_sorted.cpp)
rv3_add_test(example.find find find.cpp)
rv3_add_test(example.filter_transform filter_transform filter_transform.cpp)
rv3_add_test(example.accumulate_ints accumulate_ints accumulate_ints.cpp)
rv3_add_test(example.comprehension_conversion comprehension_conversion comprehension_conversion.cpp)
rv3_add_test(example.sort_unique sort_unique sort_unique.cpp)

# Guarded with a variable because the calendar example causes gcc to puke.
if(RANGES_BUILD_CALENDAR_EXAMPLE)
    set(Boost_USE_STATIC_LIBS        ON)
    set(Boost_USE_MULTITHREADED     OFF)
    set(Boost_USE_STATIC_RUNTIME    OFF)
    find_package(Boost 1.59.0 COMPONENTS date_time program_options)

    if (Boost_FOUND)
        add_executable(range.v3.calendar calendar.cpp)
        target_include_directories(range.v3.calendar SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})
        target_link_libraries(range.v3.calendar PRIVATE range-v3 Boost::date_time Boost::program_options)
        message(STATUS "boost: ${Boost_LIBRARY_DIRS}")
        target_compile_definitions(range.v3.calendar PRIVATE BOOST_NO_AUTO_PTR)
        target_compile_options(range.v3.calendar PRIVATE -std=gnu++14)
    endif()
endif()
