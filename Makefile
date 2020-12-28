CXXFLAGS ?= -O3
override CXXFLAGS += -g -std=c++17 -Iinclude -march=native -MMD
LDFLAGS += -pthread

SRCS := src/main.cpp src/io_util.cpp

OBJS := $(patsubst %.cpp,%.o,$(SRCS))

-include $(OBJS:.o=.d)

simdcsv: $(OBJS)
	$(CXX) $(CXXSTD) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

.PHONY: clean
clean:
	rm -f $(OBJS) simdcsv
