# it must be a macro cause policies have scopes
# http://www.cmake.org/cmake/help/v3.0/command/cmake_policy.html

macro (kvrocks_policy)

	# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
	if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
		cmake_policy(SET CMP0135 NEW)
	endif()
	
endmacro()
