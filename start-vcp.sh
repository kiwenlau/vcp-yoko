#!/bin/bash

docker rm -f mesosmaster mesosslave galaxy

# start scheduler container
docker run -d --name mesosmaster --net=host -e MY_HOST_IF=eth1 -v /opt:/opt nasuno/mesos-aurora

#sleep 5

# start mesosslave container
docker run -d --name mesosslave --privileged -v /var/lib/docker:/var/lib/docker -v /opt:/opt --net=host -e MY_HOST_IF=eth1 -e ZK_HOST=127.0.0.1 -e MESOS_CPUS=8 -e MESOS_MEM=16000 -e MESOS_DISK=4000 kiwenlau/mesos-aurora:0.1


rm /opt/workdir/galaxy_files/000/*
cp ./galaxy/dataset/* /opt/workdir/galaxy_files/000/ 

# start galaxy container
docker run -d --name galaxy -v /opt:/opt --net=host kiwenlau/pitagora-galaxy:0.2

echo -e "\nGalaxy is starting, please wait for a few seconds"

for (( i = 0; i < 120; i++ )); do
	galaxy_logs=`docker logs galaxy | grep PID`
	if [[ $galaxy_logs ]]; then
		echo -e "\nGalaxy is running"
		break
	fi
	sleep 1
done