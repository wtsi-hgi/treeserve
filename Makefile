
CXXFLAGS = -std=gnu++11 -Wall -Werror -Wextra -Weffc++
CFLAGS = -Wall -Werror -Wextra
LIBS = -lboost_iostreams -lboost_regex -lgflags -lglog -lproxygenhttpserver -lfolly -pthread

CLASS_OBJECTS=src/TreeBuilder.o src/IndexedMap.o src/TreeserveRouter.o src/TreeserveHandler.o src/base64.o src/globals.o
PROGRAM_OBJECTS=src/treeserve.o src/testProxygen.o src/testTreeNode.o src/testTree.o src/testIndexedMap.o src/testDatum.o src/testTreeBuilder.o

.PHONY: all profile debug test clean

all: CXXFLAGS += -O2 -DNDEBUG
all: bin/treeserve

profile: CXXFLAGS += -O2 -DNDEBUG -g -fno-omit-frame-pointer -pg -fno-inline-functions -fno-inline-functions-called-once -fno-optimize-sibling-calls 
profile: bin/treeserve

debug: CXXFLAGS += -std=gnu++11 -O0 -DDEBUG -ggdb -fno-omit-frame-pointer
debug: bin/treeserve

test: bin/testDatum bin/testIndexedMap bin/testTreeNode bin/testTree bin/testTreeBuilder bin/testProxygen

bin/treeserve : $(PROGRAM_OBJECTS) $(CLASS_OBJECTS)
	g++ -o bin/treeserve  src/treeserve.o $(CLASS_OBJECTS) $(LIBS)

bin/testDatum : $(PROGRAM_OBJECTS) $(CLASS_OBJECTS)
	g++ -o bin/testDatum  src/testDatum.o

bin/testIndexedMap : $(PROGRAM_OBJECTS) $(CLASS_OBJECTS)
	g++ -o bin/testIndexedMap  src/testIndexedMap.o src/IndexedMap.o

bin/testTreeNode : $(PROGRAM_OBJECTS) $(CLASS_OBJECTS)
	g++ -o bin/testTreeNode  src/testTreeNode.o src/IndexedMap.o

bin/testTree : $(PROGRAM_OBJECTS) $(CLASS_OBJECTS)
	g++ -o bin/testTree src/testTree.o src/IndexedMap.o

bin/testTreeBuilder : $(PROGRAM_OBJECTS) $(CLASS_OBJECTS)
	g++ -o bin/testTreeBuilder src/testTreeBuilder.o src/TreeBuilder.o src/base64.o src/IndexedMap.o -lboost_iostreams -lboost_regex -lgflags -lglog

$(CLASS_OBJECTS): %.o: %.cpp %.hpp
	g++ $(CFLAGS) -c -o $@  $<

$(PROGRAM_OBJECTS): %.o: %.cpp
	g++ $(CFLAGS) -c -o $@  $<

clean:
	touch src/tmp.o
	touch bin/treeserve
	rm src/*.o
	rm bin/treeserve
