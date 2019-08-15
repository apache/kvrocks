#!/bin/bash
CHECK_TYPES="warning,performance,portability,information,missingInclude"
STANDARD=c++11
ERROR_EXITCODE=1
LANG=c++
cppcheck --enable=${CHECK_TYPES} -U__GNUC__ -x ${LANG}  src --std=${STANDARD} --error-exitcode=${ERROR_EXITCODE}
