#=============================================================================
# Setup the documentation
#=============================================================================
if (NOT DOXYGEN_FOUND)
    message(STATUS
    "Doxygen not found; the 'range-v3-doc' and 'range-v3-gh-pages.{clean,copy,update}' targets "
    "will be unavailable.")
    return()
endif()

set(CMAKE_FOLDER "doc")

configure_file(Doxyfile.in Doxyfile @ONLY)
configure_file(preprocess.sh.in preprocess.sh @ONLY)
add_custom_target(range.v3.doc.check
    COMMAND ${DOXYGEN_EXECUTABLE} Doxyfile
    COMMENT "Running Doxygen to validate the documentation"
    VERBATIM
)
set_target_properties(range.v3.doc.check
    PROPERTIES FOLDER ${CMAKE_FOLDER}
)

# if (NOT TARGET benchmarks)
#     message(STATUS
#         "The 'benchmarks' target is not available; the 'doc' and "
#         "'gh-pages.{clean,copy,update}' targets will be unavailable. "
#         "The 'doc.check' target can still be used to generate the "
#         "documentation to check for errors/warnings.")
#     return()
# endif()

add_custom_target(range-v3-doc
    COMMAND ${DOXYGEN_EXECUTABLE} Doxyfile
    COMMENT "Generating API documentation with Doxygen"
#     DEPENDS range-v3-benchmarks
    VERBATIM
)
set_target_properties(range-v3-doc
    PROPERTIES FOLDER ${CMAKE_FOLDER}
)

if (NOT GIT_FOUND)
    message(STATUS
    "Git was not found; the 'range-v3-gh-pages.{clean,copy,update}' targets "
    "will be unavailable.")
    return()
endif()

add_custom_target(range-v3-gh-pages.clean
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_LIST_DIR}/clean-gh-pages.cmake
    COMMAND ${CMAKE_COMMAND} -E remove_directory search
    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/gh-pages
    COMMENT "Cleaning up doc/gh-pages"
    VERBATIM
)
set_target_properties(range-v3-gh-pages.clean
    PROPERTIES FOLDER ${CMAKE_FOLDER}
)

add_custom_target(range-v3-gh-pages.copy
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_BINARY_DIR}/html ${CMAKE_CURRENT_LIST_DIR}/gh-pages
    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/gh-pages
    COMMENT "Copying the documentation from ${CMAKE_CURRENT_BINARY_DIR}/html to doc/gh-pages"
    DEPENDS range-v3-doc range-v3-gh-pages.clean
    VERBATIM
)
set_target_properties(range-v3-gh-pages.copy
    PROPERTIES FOLDER ${CMAKE_FOLDER}
)

execute_process(
    COMMAND ${GIT_EXECUTABLE} -C ${CMAKE_SOURCE_DIR} rev-parse --short HEAD
    OUTPUT_VARIABLE RANGE_V3_GIT_SHORT_SHA
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

add_custom_target(range-v3-gh-pages.update
    COMMAND ${GIT_EXECUTABLE} add --all .
    COMMAND ${GIT_EXECUTABLE} commit -m "Update to ${RANGE_V3_GIT_SHORT_SHA}"
    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/gh-pages
    COMMENT "Updating the gh-pages branch with freshly built documentation"
    DEPENDS range-v3-gh-pages.copy
    VERBATIM
)
set_target_properties(range-v3-gh-pages.update
    PROPERTIES FOLDER ${CMAKE_FOLDER}
)
