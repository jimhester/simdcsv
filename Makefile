CXXFLAGS := -O3 -g
CXXFLAGS += -std=c++17
LDFLAGS += -pthread

simdcsv: src/main.o src/io_util.o
	$(CXX) $(CXXSTD) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

src/main.o: src/two_pass.h

.PHONY: clean
clean:
	rm -f src/*.o simdcsv
