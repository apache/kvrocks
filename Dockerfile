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

FROM debian:bookworm-slim AS build

ARG MORE_BUILD_ARGS

RUN DEBIAN_FRONTEND=noninteractive && apt-get update && apt-get upgrade -y && apt-get -y --no-install-recommends install git build-essential autoconf cmake libtool python3 libssl-dev && apt-get autoremove && apt-get clean

WORKDIR /kvrocks

COPY . .
RUN ./x.py build -DENABLE_OPENSSL=ON -DPORTABLE=1 -DCMAKE_BUILD_TYPE=Release -j $(nproc) $MORE_BUILD_ARGS

FROM debian:bookworm-slim

RUN DEBIAN_FRONTEND=noninteractive && apt-get update && apt-get upgrade -y && apt-get -y install openssl ca-certificates redis-tools binutils && apt-get clean

RUN mkdir /var/run/kvrocks

VOLUME /var/lib/kvrocks

COPY --from=build /kvrocks/build/kvrocks /bin/

HEALTHCHECK --interval=10s --timeout=1s --start-period=30s --retries=3 \
    CMD redis-cli -p 6666 PING | grep -E '(PONG|NOAUTH)' || exit 1

COPY ./LICENSE ./NOTICE ./licenses /kvrocks/
COPY ./kvrocks.conf /var/lib/kvrocks/

EXPOSE 6666:6666

ENTRYPOINT ["kvrocks", "-c", "/var/lib/kvrocks/kvrocks.conf", "--dir", "/var/lib/kvrocks", "--pidfile", "/var/run/kvrocks/kvrocks.pid", "--bind", "0.0.0.0"]
