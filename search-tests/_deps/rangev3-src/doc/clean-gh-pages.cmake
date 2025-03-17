FILE(GLOB gh_files "*.html" "*.js" "*.css" "*.png")
IF( gh_files )
    execute_process( COMMAND ${CMAKE_COMMAND} -E remove ${gh_files} )
ENDIF()
