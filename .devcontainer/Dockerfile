FROM golang:1.25-bookworm AS go-toolchain

FROM ubuntu:noble

COPY --from=go-toolchain /usr/local/go /usr/local/go
ENV PATH="/usr/local/go/bin:${PATH}"

RUN apt-get -o Acquire::Retries=5 update \
    && for attempt in 1 2 3; do \
        apt-get -o Acquire::Retries=5 install -y \
            git build-essential cmake libtool python3 libssl-dev python3-pip \
            wget curl clang-format clang-tidy ninja-build \
            redis-tools vim python3-redis redis-server clang lld mold gdb fish \
        && break; \
        if [ "$attempt" -eq 3 ]; then exit 1; fi; \
        sleep 5; \
    done
RUN BUILD_DIR=$(pwd) && git clone https://github.com/jsha/minica /opt/minica \
    && cd /opt/minica && git checkout 96a5c93723cf3d34b50b3e723a9f05cd3765bc67 && go build && cd $BUILD_DIR \
    && echo 'export PATH=/opt/minica:$PATH' >> $HOME/.bashrc
