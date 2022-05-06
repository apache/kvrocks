#!/usr/bin/env bash

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

NAME="kvrocks"
VERSION=`_build/bin/kvrocks -v|awk '{printf $2;}'`
STAGE=${STAGE:-release}
fpm -f -s dir -t rpm --prefix '/www/kvrocks'  -n ${NAME} --epoch 7 \
    --config-files /www/kvrocks/conf/kvrocks.conf \
    -v ${VERSION} --iteration ${CI_PIPELINE_ID}.${STAGE} -C ./_build \
    --verbose --category 'kvrockslabs/projects' --description 'kvrocks' \
    --url 'https://github.com/apache/incubator-kvrocks' --license 'Apache2.0'

