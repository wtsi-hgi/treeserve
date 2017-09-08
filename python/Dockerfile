FROM ubuntu:16.04
MAINTAINER jcrandall@alum.mit.edu

USER root

ENV DEBIAN_FRONTEND noninteractive

RUN \
  apt-get -q=2 update && \
  apt-get -q=2 -y upgrade && \
  apt-get -q=2 -y --no-install-recommends install \
    software-properties-common \
    python-software-properties && \
  apt-get autoremove && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

ADD . /docker/treeserve
WORKDIR /docker/treeserve


ENTRYPOINT ["./bin/treeserve", "-logtostderr", "-dump", "/dev/null", "-lstat", "/docker/input.gz", "-ip", "0.0.0.0", "-port", "80"]
