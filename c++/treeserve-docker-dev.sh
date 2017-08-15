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
sudo docker build -t local/treeserve-${pid} .

# launch docker container 
sudo docker run -v ${groupfile}:/etc/group -v ${passwdfile}:/etc/passwd -v ${datfile}:/docker/input.gz -p ${port}:80 local/treeserve-${pid}
