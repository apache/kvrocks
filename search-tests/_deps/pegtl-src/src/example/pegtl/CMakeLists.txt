cmake_minimum_required(VERSION 3.8...3.19)

set(example_sources
  abnf2pegtl.cpp
  analyze.cpp
  calculator.cpp
  chomsky_hierarchy.cpp
  csv1.cpp
  csv2.cpp
  dynamic_match.cpp
  expression.cpp
  hello_world.cpp
  indent_aware.cpp
  iri.cpp
  json_analyze.cpp
  json_ast.cpp
  json_build.cpp
  json_count.cpp
  json_coverage.cpp
  json_parse.cpp
  json_print_debug.cpp
  json_print_names.cpp
  json_trace.cpp
  lua53_analyze.cpp
  lua53_parse.cpp
  modulus_match.cpp
  parse_tree.cpp
  parse_tree_user_state.cpp
  peg2pegtl.cpp
  proto3.cpp
  recover.cpp
  s_expression.cpp
  sum.cpp
  symbol_table.cpp
  token_input.cpp
  unescape.cpp
  uri.cpp
  uri_print_debug.cpp
  uri_print_names.cpp
  uri_trace.cpp
)

# file(GLOB ...) is used to validate the above list of test_sources
file(GLOB glob_example_sources RELATIVE ${CMAKE_CURRENT_LIST_DIR} *.cpp)

foreach(examplesourcefile ${example_sources})
  if(${examplesourcefile} IN_LIST glob_example_sources)
    list(REMOVE_ITEM glob_example_sources ${examplesourcefile})
  else()
    message(SEND_ERROR "File ${examplesourcefile} is missing from src/example/pegtl")
  endif()

  get_filename_component(exename pegtl-example-${examplesourcefile} NAME_WE)
  add_executable(${exename} ${examplesourcefile})
  target_link_libraries(${exename} PRIVATE taocpp::pegtl)
  set_target_properties(${exename} PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
  )
  if(MSVC)
    target_compile_options(${exename} PRIVATE /W4 /WX /utf-8)
  else()
    target_compile_options(${exename} PRIVATE -pedantic -Wall -Wextra -Wshadow -Werror)
  endif()
endforeach()

if(glob_example_sources)
  foreach(ignored_source_file ${glob_example_sources})
    message(SEND_ERROR "File ${ignored_source_file} in src/example/pegtl is ignored")
  endforeach()
endif()
