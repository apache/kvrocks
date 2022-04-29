#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

GIT_SHA1=`(git show-ref --head --hash=8 2> /dev/null || echo 00000000) | head -n1`
GIT_DIRTY=`git diff --no-ext-diff 2> /dev/null | wc -l`
VERSION=`grep -i "version" -m1 ../Changelog|awk '{printf $3}'`
BUILD_ID=`uname -n`"-"`date +%s`
if [ -n "$SOURCE_DATE_EPOCH" ]; then
  BUILD_ID=$(date -u -d "@$SOURCE_DATE_EPOCH" +%s 2>/dev/null || date -u -r "$SOURCE_DATE_EPOCH" +%s 2>/dev/null || date -u %s)
fi
test -f version.h|| touch version.h 
(cat version.h | grep SHA1 | grep $GIT_SHA1) && \
(cat version.h | grep DIRTY | grep $GIT_DIRTY) && exit 0 # Already up-to-date
echo "#pragma once" > version.h 
echo "#define GIT_COMMIT \"$GIT_SHA1\"" >> version.h 
echo "#define VERSION \"$VERSION\"" >> version.h 
echo "#define BUILD_ID \"$BUILD_ID\"" >> version.h 

touch main.cc # Force recompile of main.cc
