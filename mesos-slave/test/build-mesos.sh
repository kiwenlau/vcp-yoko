# Change working directory.
cd /root/mesos-0.26.0

# Configure and build.
mkdir build
cd build
../configure
make

# Install 
make install