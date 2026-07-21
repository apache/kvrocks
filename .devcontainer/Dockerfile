FROM ubuntu:resolute
RUN apt update \
    && apt install -y \
        git build-essential cmake libtool python3 libssl-dev python3-pip \
        wget curl clang-format-18 clang-tidy-18 golang-go ninja-build \
        redis-tools vim python3-redis redis-server clang lld mold gdb fish
RUN ln -s /usr/bin/clang-format-18 /usr/local/bin/clang-format \
    && ln -s /usr/bin/clang-tidy-18 /usr/local/bin/clang-tidy \
    && ln -s /usr/bin/run-clang-tidy-18 /usr/local/bin/run-clang-tidy
RUN BUILD_DIR=$(pwd) && git clone https://github.com/jsha/minica /opt/minica \
    && cd /opt/minica && git checkout 96a5c93723cf3d34b50b3e723a9f05cd3765bc67 && go build && cd $BUILD_DIR \
    && echo 'export PATH=/opt/minica:$PATH' >> $HOME/.bashrc
