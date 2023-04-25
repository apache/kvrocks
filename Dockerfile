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

FROM ubuntu:focal as build

ARG MORE_BUILD_ARGS

# workaround tzdata install hanging
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt update && apt install -y git gcc g++ make cmake autoconf automake libtool python3 libssl-dev curl
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && apt-get install -y nodejs
WORKDIR /kvrocks

COPY . .
RUN ./x.py build -DENABLE_OPENSSL=ON -DPORTABLE=ON $MORE_BUILD_ARGS
RUN cd ./web/api && npm install && cd ../../
RUN cd ./web/ui && npm install && npm run build && cd ../..

FROM ubuntu:focal

RUN apt update && apt install -y libssl-dev

WORKDIR /kvrocks

COPY --from=build /kvrocks/web ./web/
COPY --from=build /kvrocks/build/kvrocks ./bin/

VOLUME /var/lib/kvrocks

COPY ./LICENSE ./NOTICE ./DISCLAIMER ./
COPY ./licenses ./licenses
COPY ./kvrocks.conf  /var/lib/kvrocks/

EXPOSE 6666:6666
ENTRYPOINT ["./bin/kvrocks", "-c", "/var/lib/kvrocks/kvrocks.conf", "--dir", "/var/lib/kvrocks"]
