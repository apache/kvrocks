# jsoncons cmake module
# This module sets the following variables in your project::
#
#   jsoncons_FOUND - true if jsoncons found on the system
#   jsoncons_INCLUDE_DIRS - the directory containing jsoncons headers
#   jsoncons_LIBRARY - empty


####### Expanded from @PACKAGE_INIT@ by configure_package_config_file() #######
####### Any changes to this file will be overwritten by the next CMake run ####
####### The input file was Config.cmake                            ########

get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../" ABSOLUTE)

macro(set_and_check _var _file)
  set(${_var} "${_file}")
  if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "File or directory ${_file} referenced by variable ${_var} does not exist !")
  endif()
endmacro()

macro(check_required_components _NAME)
  foreach(comp ${${_NAME}_FIND_COMPONENTS})
    if(NOT ${_NAME}_${comp}_FOUND)
      if(${_NAME}_FIND_REQUIRED_${comp})
        set(${_NAME}_FOUND FALSE)
      endif()
    endif()
  endforeach()
endmacro()

####################################################################################

if(NOT TARGET jsoncons)
  include("${CMAKE_CURRENT_LIST_DIR}/jsonconsTargets.cmake")
  get_target_property(jsoncons_INCLUDE_DIRS jsoncons INTERFACE_INCLUDE_DIRECTORIES)
endif()
