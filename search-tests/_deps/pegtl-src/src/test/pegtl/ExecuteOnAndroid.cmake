
if("${ANDROID_NDK}" STREQUAL "")
    message(FATAL_ERROR "cmake parameter ANDROID_NDK is not set!")
endif()

if(NOT EXISTS "${ANDROID_NDK}/../platform-tools/adb")
    message(FATAL_ERROR "adb is not found in ${ANDROID_NDK}/../platform-tools")
endif()

get_filename_component(UNITTEST_FILENAME ${UNITTEST} NAME)
message(STATUS "Cleanup /data/local/tmp ...")
execute_process(COMMAND ${ANDROID_NDK}/../platform-tools/adb shell "rm -r /data/local/tmp/*" OUTPUT_QUIET)

foreach(_TEST_DATA IN ITEMS ${TEST_RESOURCES})
    get_filename_component(_DIR ${_TEST_DATA} DIRECTORY)
    message(STATUS "Create /data/local/tmp/${_DIR} directory structure on android ...")
    execute_process(COMMAND ${ANDROID_NDK}/../platform-tools/adb shell "mkdir -p /data/local/tmp/${_DIR}" OUTPUT_QUIET)
    message(STATUS "Push ${_TEST_DATA} from ${TEST_RESOURCES_DIR} to android ...")
    execute_process(COMMAND ${ANDROID_NDK}/../platform-tools/adb push ${_TEST_DATA} /data/local/tmp/${_DIR} OUTPUT_QUIET WORKING_DIRECTORY ${TEST_RESOURCES_DIR})
endforeach()
if(LIBRARY_DIR)
    message(STATUS "Push ${LIBRARY_DIR} to android ...")
    execute_process(COMMAND ${ANDROID_NDK}/../platform-tools/adb push ${LIBRARY_DIR} /data/local/tmp/ OUTPUT_QUIET)
endif()
message(STATUS "Push ${UNITTEST} to android ...")
if(NOT EXISTS "${UNITTEST}")
    message(FATAL_ERROR "${UNITTEST} is not found!")
endif()
execute_process(COMMAND ${ANDROID_NDK}/../platform-tools/adb push ${UNITTEST} /data/local/tmp/ OUTPUT_QUIET)
message(STATUS "Execute ${UNITTEST_FILENAME} on android ...")

execute_process(
   COMMAND ${ANDROID_NDK}/../platform-tools/adb shell "cd /data/local/tmp;su root sh -c 'LD_LIBRARY_PATH=/data/local/tmp/lib TMPDIR=/data/local/tmp HOME=/data/local/tmp ./${UNITTEST_FILENAME};echo exit code $?'"
   RESULT_VARIABLE _RESULT
   OUTPUT_VARIABLE _OUT
   ERROR_VARIABLE _ERR
)

if(_RESULT)
    message(FATAL_ERROR "Execution of ${UNITTEST_FILENAME} failed")
else()
    string(REGEX MATCH "exit code ([0-9]+)" _EXIT_CODE ${_OUT})
    if(NOT "${CMAKE_MATCH_1}" EQUAL 0)
        string(REGEX REPLACE "exit code [0-9]+" "" _PRINT_OUT ${_OUT})
        message(FATAL_ERROR "${UNITTEST_FILENAME} execution error: ${_PRINT_OUT} ${_ERR}")
    endif()
endif()
