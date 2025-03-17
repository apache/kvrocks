if (TARGET range-v3::meta)
  return()
endif()

include("${CMAKE_CURRENT_LIST_DIR}/range-v3-targets.cmake")

add_library(range-v3::meta INTERFACE IMPORTED)
add_library(range-v3::concepts INTERFACE IMPORTED)
add_library(range-v3::range-v3 INTERFACE IMPORTED)

# workaround for target_link_libraries on lower cmake versions (< 3.11)
# see https://cmake.org/cmake/help/latest/release/3.11.html#commands
if(CMAKE_VERSION VERSION_LESS 3.11)
set_target_properties(range-v3::meta PROPERTIES INTERFACE_LINK_LIBRARIES "range-v3-meta")
set_target_properties(range-v3::concepts PROPERTIES INTERFACE_LINK_LIBRARIES "range-v3-concepts")
set_target_properties(range-v3::range-v3 PROPERTIES INTERFACE_LINK_LIBRARIES "range-v3")
else()
target_link_libraries(range-v3::meta INTERFACE range-v3-meta)
target_link_libraries(range-v3::concepts INTERFACE range-v3-concepts)
target_link_libraries(range-v3::range-v3 INTERFACE range-v3)
endif()
