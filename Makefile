CFLAGS=-std=gnu++11 -O3
LIBS=-lboost_iostreams

all : bin/lstatTree

bin/lstatTree : src/lstatTree.o src/base64.o
	g++ -o bin/lstatTree src/lstatTree.o src/base64.o $(LIBS)

bin/testHttpd: src/testHttpd.o
	g++ -o bin/testHttpd src/testHttpd.o $(LIBS)

src/lstatTree.o : src/lstatTree.cpp
	g++ -c $(CFLAGS) -o src/lstatTree.o src/lstatTree.cpp

src/base64.o : src/base64.cpp src/base64.h
	g++ -c $(CFLAGS) -o src/base64.o src/base64.cpp

src/testHttpd.o : src/testHttpd.cpp
	g++ -c $(CFLAGS) -o src/testHttpd.o src/testHttpd.cpp

clean :
	touch src/tmp.o
	rm src/*.o
	rm bin/lstatTree
