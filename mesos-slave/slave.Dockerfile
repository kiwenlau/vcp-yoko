FROM nasuno/mesos-aurora
MAINTAINER kiwenlau@gmail.com

RUN apt-get -y install build-essential python-dev python-boto libcurl4-nss-dev libsasl2-dev libsasl2-modules maven libapr1-dev libsvn-dev

#RUN cp /usr/lib/x86_64-linux-gnu/libcurl-nss.so.4.3.0 /usr/lib
#RUN ln -s /usr/lib/libcurl-nss.so.4.3.0 /usr/lib/libcurl-nss.so.4

ENV LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/
RUN ldconfig

ADD init.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/init.sh

CMD ["/usr/local/bin/init.sh"]