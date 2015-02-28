CXXFLAGS = -std=gnu++11 -Wall -Werror -Wextra
CFLAGS = -Wall -Werror -Wextra
LIBS = -lboost_iostreams -lboost_regex

.PHONY: all profile debug test clean

all: CXXFLAGS += -O2 -DNDEBUG
all: bin/lstatTree

profile: CXXFLAGS += -O2 -DNDEBUG -g -fno-omit-frame-pointer -pg -fno-inline-functions -fno-inline-functions-called-once -fno-optimize-sibling-calls 
profile: bin/lstatTree

debug: CXXFLAGS += -std=gnu++11 -O0 -DDEBUG -ggdb -fno-omit-frame-pointer
debug: bin/lstatTree

test: bin/testTree

bin/lstatTree: src/lstatTree.o src/base64.o src/fossa.o src/IndexedMap.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

bin/testHttpd: src/testHttpd.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

src/lstatTree.o: src/lstatTree.cpp src/TreeNode.hpp src/Tree.hpp src/IndexedMap.hpp src/Datum.hpp
	$(CXX) $(CXXFLAGS) -c -Ijson/src -o src/lstatTree.o src/lstatTree.cpp

src/base64.o: src/base64.cpp src/base64.h
	$(CXX) $(CXXFLAGS) -c -o src/base64.o src/base64.cpp

src/testHttpd.o: src/testHttpd.cpp
	$(CXX) $(CXXFLAGS) -c -o src/testHttpd.o src/testHttpd.cpp

src/fossa.o: src/fossa.c
	$(CC) -c -o src/fossa.o src/fossa.c

src/IndexedMap.o: src/IndexedMap.hpp src/IndexedMap.cpp src/Datum.hpp
	$(CXX) $(CXXFLAGS) -c -Ijson/src -o src/IndexedMap.o src/IndexedMap.cpp

clean:
	touch src/tmp.o
	rm src/*.o
	rm bin/lstatTree
