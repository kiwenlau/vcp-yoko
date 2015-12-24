FROM ubuntu:14.04

MAINTAINER kiwenlau@gmail.com

WORKDIR /root

ADD install-mesos.sh /tmp/install-mesos.sh

RUN /tmp/install-mesos.sh

RUN apt-get -y install vim

RUN wget http://www.apache.org/dist/mesos/0.26.0/mesos-0.26.0.tar.gz
RUN tar -zxf mesos-0.26.0.tar.gz

ADD build-mesos.sh /tmp/build-mesos.sh

RUN /tmp/build-mesos.sh

ADD test.py /root/test.py


