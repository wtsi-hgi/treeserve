CFLAGS=-std=gnu++11 -O3
LIBS=-lboost_iostreams -lmicrohttpd

all : bin/lstatTree

bin/lstatTree : src/lstatTree.o src/base64.o
	g++ -o bin/lstatTree src/lstatTree.o src/base64.o $(LIBS)

src/lstatTree.o :
	g++ -c $(CFLAGS) -o src/lstatTree.o src/lstatTree.cpp

src/base64.o : src/base64.cpp src/base64.h
	g++ -c $(CFLAGS) -o src/base64.o src/base64.cpp

clean :
	touch src/tmp.o
	rm src/*.o
	touch bin/tmp
	rm bin/*
