#CFLAGS=-std=gnu++11 -O0 -g 
#CFLAGS=-std=gnu++11 -O0 -ggdb -pg
CFLAGS=-std=gnu++11 -Wall -Weffc++ -O2 -Ijson/src
LIBS=-lboost_iostreams -lboost_regex -lgflags -lglog

all : bin/treeserve

bin/treeserve : src/treeserve.o src/TreeBuilder.o src/base64.o src/fossa.o src/IndexedMap.o
	g++ $(CFLAGS) -o bin/treeserve src/treeserve.o src/TreeBuilder.o src/base64.o src/fossa.o src/IndexedMap.o $(LIBS)

src/treeserve.o : src/treeserve.cpp src/TreeNode.hpp src/Tree.hpp src/IndexedMap.hpp src/Datum.hpp
	g++ -c $(CFLAGS) -o src/treeserve.o src/treeserve.cpp

src/TreeBuilder.o : src/TreeBuilder.cpp src/TreeBuilder.hpp
	g++ -c $(CFLAGS) -o src/TreeBuilder.o src/TreeBuilder.cpp

src/base64.o : src/base64.cpp src/base64.h
	g++ -c $(CFLAGS) -o src/base64.o src/base64.cpp

src/fossa.o : src/fossa.c src/fossa.h
	g++ -c $(CFLAGS) -o src/fossa.o src/fossa.c

src/IndexedMap.o : src/IndexedMap.hpp src/IndexedMap.cpp src/Datum.hpp
	g++ -c $(CFLAGS)  -o src/IndexedMap.o src/IndexedMap.cpp

clean :
	touch src/tmp.o
	touch bin/treeserve
	rm src/*.o
	rm bin/treeserve
