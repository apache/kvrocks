if (CMAKE_VERSION VERSION_LESS 3.0)
  message (FATAL_ERROR "CMake >= 3.0 required")
endif (CMAKE_VERSION VERSION_LESS 3.0)


####### Expanded from @PACKAGE_INIT@ by configure_package_config_file() #######
####### Any changes to this file will be overwritten by the next CMake run ####
####### The input file was glog-config.cmake.in                            ########

get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../" ABSOLUTE)

macro(set_and_check _var _file)
  set(${_var} "${_file}")
  if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "File or directory ${_file} referenced by variable ${_var} does not exist !")
  endif()
endmacro()

####################################################################################

include (CMakeFindDependencyMacro)
include (${CMAKE_CURRENT_LIST_DIR}/glog-modules.cmake)

find_dependency (Threads)




include (${CMAKE_CURRENT_LIST_DIR}/glog-targets.cmake)
