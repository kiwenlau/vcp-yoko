FROM nasuno/pitagora-galaxy:latest
MAINTAINER kiwenlau@gmail.com

ADD config/aurora /usr/local/bin/
ADD config/clusters.json /etc/aurora/clusters.json
ADD config/nginx.aurora /tmp/nginx.aurora

ADD config/job_conf.xml /home/galaxy/galaxy/config/job_conf.xml
ADD config/tool_conf.xml /home/galaxy/galaxy/config/tool_conf.xml

ADD config/tools/* /home/galaxy/galaxy/tools/aurora/

ADD config/aurora.py /home/galaxy/galaxy/lib/galaxy/jobs/runners/aurora.py

ADD config/job2.aurora /tmp/job2.aurora

CMD ["/usr/local/bin/init.sh"]