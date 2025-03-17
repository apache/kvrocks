cmake_minimum_required(VERSION 3.8...3.19)

set(test_sources
  action_enable.cpp
  action_match.cpp
  actions_one.cpp
  actions_three.cpp
  actions_two.cpp
  argv_input.cpp
  ascii_classes.cpp
  ascii_eol.cpp
  ascii_eolf.cpp
  ascii_forty_two.cpp
  ascii_identifier.cpp
  ascii_istring.cpp
  ascii_keyword.cpp
  ascii_shebang.cpp
  ascii_string.cpp
  ascii_three.cpp
  ascii_two.cpp
  buffer_input.cpp
  change_action_and_state.cpp
  change_action_and_states.cpp
  change_state.cpp
  change_states.cpp
  check_bytes.cpp
  contains.cpp
  contrib_alphabet.cpp
  contrib_analyze.cpp
  contrib_control_action.cpp
  contrib_coverage.cpp
  contrib_function.cpp
  contrib_http.cpp
  contrib_if_then.cpp
  contrib_instantiate.cpp
  contrib_integer.cpp
  contrib_iri.cpp
  contrib_json.cpp
  contrib_parse_tree.cpp
  contrib_parse_tree_to_dot.cpp
  contrib_partial_trace.cpp
  contrib_predicates.cpp
  contrib_print.cpp
  contrib_raw_string.cpp
  contrib_remove_first_state.cpp
  contrib_remove_last_states.cpp
  contrib_rep_one_min_max.cpp
  contrib_rep_string.cpp
  contrib_separated_seq.cpp
  contrib_state_control.cpp
  contrib_to_string.cpp
  contrib_trace1.cpp
  contrib_trace2.cpp
  contrib_unescape.cpp
  contrib_uri.cpp
  control_unwind.cpp
  data_cstring.cpp
  demangle.cpp
  discard_input.cpp
  enable_control.cpp
  error_message.cpp
  file_cstream.cpp
  file_file.cpp
  file_istream.cpp
  file_mmap.cpp
  file_read.cpp
  icu_general.cpp
  internal_endian.cpp
  internal_file_mapper.cpp
  internal_file_opener.cpp
  limit_bytes.cpp
  limit_depth.cpp
  parse_error.cpp
  pegtl_string_t.cpp
  position.cpp
  restart_input.cpp
  rule_action.cpp
  rule_apply0.cpp
  rule_apply.cpp
  rule_at.cpp
  rule_bof.cpp
  rule_bol.cpp
  rule_bytes.cpp
  rule_control.cpp
  rule_disable.cpp
  rule_discard.cpp
  rule_enable.cpp
  rule_eof.cpp
  rule_failure.cpp
  rule_if_apply.cpp
  rule_if_must.cpp
  rule_if_must_else.cpp
  rule_if_then_else.cpp
  rule_list.cpp
  rule_list_must.cpp
  rule_list_tail.cpp
  rule_minus.cpp
  rule_must.cpp
  rule_not_at.cpp
  rule_opt.cpp
  rule_opt_must.cpp
  rule_pad.cpp
  rule_pad_opt.cpp
  rule_plus.cpp
  rule_raise.cpp
  rule_rematch.cpp
  rule_rep.cpp
  rule_rep_max.cpp
  rule_rep_min.cpp
  rule_rep_min_max.cpp
  rule_rep_opt.cpp
  rule_require.cpp
  rule_seq.cpp
  rule_sor.cpp
  rule_star.cpp
  rule_star_must.cpp
  rule_state.cpp
  rule_success.cpp
  rule_try_catch.cpp
  rule_until.cpp
  test_empty.cpp
  test_result.cpp
  test_setup.cpp
  uint16_general.cpp
  uint32_general.cpp
  uint64_general.cpp
  uint8_general.cpp
  utf16_general.cpp
  utf32_general.cpp
  utf8_general.cpp
  visit.cpp
)

# file(GLOB ...) is used to validate the above list of test_sources
file(GLOB glob_test_sources RELATIVE ${CMAKE_CURRENT_LIST_DIR} *.cpp)

foreach(testsourcefile ${test_sources})
  if(${testsourcefile} IN_LIST glob_test_sources)
    list(REMOVE_ITEM glob_test_sources ${testsourcefile})
  else()
    message(SEND_ERROR "File ${testsourcefile} is missing from src/test/pegtl")
  endif()

  get_filename_component(exename pegtl-test-${testsourcefile} NAME_WE)
  add_executable(${exename} ${testsourcefile})
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
  if(ANDROID)
    add_test(NAME ${exename} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} COMMAND ${CMAKE_COMMAND} "-DANDROID_NDK=${ANDROID_NDK}" "-DTEST_RESOURCES_DIR=${CMAKE_SOURCE_DIR}" "-DTEST_RESOURCES=src/test/pegtl/data;src/test/pegtl/file_data.txt;Makefile" "-DUNITTEST=${exename}" -P ${CMAKE_CURRENT_SOURCE_DIR}/ExecuteOnAndroid.cmake)
  else()
    add_test(NAME ${exename} WORKING_DIRECTORY ${CMAKE_SOURCE_DIR} COMMAND ${exename})
  endif()
endforeach()

if(glob_test_sources)
  foreach(ignored_source_file ${glob_test_sources})
    message(SEND_ERROR "File ${ignored_source_file} in src/test/pegtl is ignored")
  endforeach()
endif()
