# build with
#    docker build -t simdcsv .
# run with
#    docker run -it --privileged -v $(pwd):/project:Z simdcsv /bin/bash

FROM ubuntu:20.10

RUN apt-get update -qq
RUN DEBIAN_FRONTEND="noninteractive" apt-get -y install tzdata g++ git make

RUN mkdir project

WORKDIR /project
