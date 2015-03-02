FROM mercury/proxygen

RUN apt-get install git gcc-4.9-base

ADD . /docker/treeserve
WORKDIR /docker/treeserve

RUN git submodule init && git submodule update
RUN make -j

ENTRYPOINT ["./bin/treeserve", "-logtostderr", "-dump", "/dev/null", "-lstat", "/docker/input.gz", "-ip", "0.0.0.0", "-port", "80"]
