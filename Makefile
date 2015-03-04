
CXXFLAGS = -std=gnu++11 -Wall -Wextra -Weffc++ -Ijson/src
LIBS = -lboost_serialization -lboost_iostreams -lboost_regex -lgflags -lglog -lproxygenhttpserver -lfolly -pthread -lz

CLASS_OBJECTS=src/TreeBuilder.o src/IndexedMap.o src/TreeserveRouter.o src/TreeserveHandler.o src/base64.o src/globals.o src/compress.o
TEST_OBJECTS=src/testProxygen.o src/testTreeNode.o src/testTree.o src/testIndexedMap.o src/testDatum.o src/testTreeBuilder.o
TREESERVE_OBJECT=src/treeserve.o

.PHONY: all profile debug test clean

all: CXXFLAGS += -O2 -DNDEBUG
all: bin/treeserve

profile: CXXFLAGS += -O2 -DNDEBUG -g -fno-omit-frame-pointer -pg -fno-inline-functions -fno-inline-functions-called-once -fno-optimize-sibling-calls 
profile: bin/treeserve

debug: CXXFLAGS += -std=gnu++11 -O0 -DDEBUG -ggdb -fno-omit-frame-pointer
debug: bin/treeserve

test: bin/testDatum bin/testIndexedMap bin/testTreeNode bin/testTree bin/testTreeBuilder bin/testProxygen

bin/treeserve : $(TREESERVE_OBJECT) $(CLASS_OBJECTS)
	$(CXX) $(CXXFLAGS) -o bin/treeserve $(TREESERVE_OBJECT) $(CLASS_OBJECTS) $(LIBS)

bin/testDatum : $(TEST_OBJECTS) $(CLASS_OBJECTS)
	$(CXX) $(CXXFLAGS) -o bin/testDatum  src/testDatum.o -lboost_serialization

bin/testIndexedMap : $(TEST_OBJECTS) $(CLASS_OBJECTS)
	$(CXX) $(CXXFLAGS) -o bin/testIndexedMap  src/testIndexedMap.o src/IndexedMap.o -lboost_serialization

bin/testTreeNode :$(TEST_OBJECTS) $(CLASS_OBJECTS)
	$(CXX) $(CXXFLAGS) -o bin/testTreeNode  src/testTreeNode.o src/IndexedMap.o -lboost_serialization

bin/testTree : $(TEST_OBJECTS) $(CLASS_OBJECTS)
	$(CXX) $(CXXFLAGS) -o bin/testTree src/testTree.o src/IndexedMap.o -lboost_serialization

bin/testTreeBuilder : $(TEST_OBJECTS) $(CLASS_OBJECTS)
	$(CXX) $(CXXFLAGS) -o bin/testTreeBuilder src/testTreeBuilder.o src/TreeBuilder.o src/base64.o src/IndexedMap.o -lboost_serialization -lboost_iostreams -lboost_regex -lgflags -lglog

bin/testProxygen : $(TEST_OBJECTS) $(CLASS_OBJECTS) src/TestHandler.o src/TestRouter.o
	$(CXX) $(CXXFLAGS) -o bin/testProxygen src/testProxygen.o src/IndexedMap.o src/TestHandler.o src/TestRouter.o -lboost_serialization -lgflags -lglog -lproxygenhttpserver -lfolly -pthread

$(CLASS_OBJECTS): %.o: %.cpp %.hpp
	$(CXX) $(CXXFLAGS) -c -o $@  $<

$(TEST_OBJECTS): %.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@  $<

$(TREESERVE_OBJECT): %.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@  $<

src/TestHandler.o: src/TestHandler.cpp src/TestHandler.hpp
	$(CXX) $(CXXFLAGS) -c -o src/TestHandler.o src/TestHandler.cpp

src/TestRouter.o: src/TestRouter.cpp src/TestRouter.hpp
	$(CXX) $(CXXFLAGS) -c -o src/TestRouter.o src/TestRouter.cpp

clean:
	touch src/tmp.o
	touch bin/treeserve
	rm src/*.o
	rm bin/treeserve
