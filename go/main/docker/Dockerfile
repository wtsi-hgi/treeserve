FROM snormore/lmdb

WORKDIR /go/src/app
# if data file here, the copy may be a problem
COPY . .

#EXPOSE 9000
EXPOSE 9001

#CMD ./treeserveApp -port=9999

# for openstack if volume mapped to /data1
ENTRYPOINT ["./treeserveApp",   "-port", "9001", "-lmdbPath", "/data1/treeserve_lmdb"]


