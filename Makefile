CXXFLAGS ?= -O3
override CXXFLAGS += -g -std=c++17 -Iinclude -march=native -MMD
LDFLAGS += -pthread

all: simdcsv

SRCS := src/main.cpp src/io_util.cpp

OBJS := $(patsubst %.cpp,%.o,$(SRCS))

-include $(OBJS:.o=.d)

.PHONY: debug
debug: CXXFLAGS += -O0
debug: simdcsv

simdcsv: $(OBJS)
	$(CXX) $(CXXSTD) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

.PHONY: clean
clean:
	rm -f $(OBJS) simdcsv
