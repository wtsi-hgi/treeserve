FROM snormore/lmdb

WORKDIR /go/src/app
COPY . .

EXPOSE 9000
EXPOSE 9001

#CMD ./treeserveApp -port=9999

ENTRYPOINT ["./treeserveApp",   "-port", "9001"]


