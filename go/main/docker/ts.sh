#!/bin/bash

set -euf -o pipefail

datfile=$1
port=$2

if [[ ! -r "${datfile}" ]];
then
    echo "Cannot read from datfile ${datfile}" >&2
    exit 1
fi

if [[ ! ( ( ${port} -ge 1 ) && ( ${port} -le 65535 ) ) ]];
then
    echo "Port not an integer between 1-65535: ${port}" >&2
    exit 2
fi

if [[ -z "${TMPDIR+set}" ]];
then
    TMPDIR="/tmp"
fi

pid=$$

groupfile="${TMPDIR}/group.${pid}"
passwdfile="${TMPDIR}/passwd.${pid}"

# lookup users and groups on real system to be bind-mounted into docker
getent group > ${groupfile}
getent passwd > ${passwdfile}

# all bind-mounted paths must be absolute. make sure they are. 
datfile="$(readlink -f ${datfile})"
groupfile="$(readlink -f ${groupfile})"
passwdfile="$(readlink -f ${passwdfile})"

# make sure image is up to date
sudo docker build -t go_treeserve-${pid}  .

# launch docker container 
#sudo docker run -i -t --rm -v ${groupfile}:/etc/groups -v ${passwdfile}:/etc/users -v ${datfile}:/tmp/input.dat.gz -p ${port}:${port} local/treeserve-${pid} 

# on Opensatck with a mounted volume on /data1 (and check Dockerfile matches)
sudo docker run -i -t --rm -v /data1:/data1 -v ${groupfile}:/etc/groups -v ${passwdfile}:/etc/users -v ${datfile}:/tmp/input.dat.gz -p 9001:9001 go_treeserve-${pid} 
