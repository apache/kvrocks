#!/bin/bash
CHECK_TYPES="warning,portability,information,missingInclude"
STANDARD=c++11
ERROR_EXITCODE=1
LANG=c++
cppcheck --force --enable=${CHECK_TYPES} -U__GNUC__ -x ${LANG}  src --std=${STANDARD} --error-exitcode=${ERROR_EXITCODE}
