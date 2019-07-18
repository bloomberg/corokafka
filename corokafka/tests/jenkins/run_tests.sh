#!/bin/bash
set -e

# get current directory
ROOTDIR=$(pwd)

# set selinux to permissive for directory
# (allow docker containers to modify files)
chcon -Rt svirt_sandbox_file_t $ROOTDIR || echo "chcon error"

# build and run builder docker container
docker build -t corokafka-console ./corokafka/tests/jenkins/kafka
docker run --rm -v $ROOTDIR/:/corokafka:rw corokafka-console
#docker pull artprod.dev.bloomberg.com/bb-etn/nano/builder:latest
#docker run --rm -v $tdir/:/nano:rw artprod.dev.bloomberg.com/bb-etn/nano/builder:latest

exit 0
