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

export NO_PROXY=/var/run/docker.sock

# make sure image is up to date
docker pull mercury/treeserve

# launch docker container
container=$(docker run -d -v ${groupfile}:/etc/group -v ${passwdfile}:/etc/passwd -v ${datfile}:/docker/input.gz -p ${port}:80 mercury/treeserve)
echo "launched docker container ${container} for ${datfile} on ${port}"

trap "docker kill ${container}" EXIT
docker logs -f ${container}

