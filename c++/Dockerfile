FROM mercury/proxygen

RUN apt-get install git

ADD . /docker/treeserve
WORKDIR /docker/treeserve


RUN mkdir bin && make -j


ENTRYPOINT ["./bin/treeserve", "-logtostderr", "-dump", "/dev/null", "-lstat", "/docker/input.gz", "-ip", "0.0.0.0", "-port", "80"]
