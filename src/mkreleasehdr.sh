#!/bin/sh
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
