CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -Wpedantic -pedantic -O3 \
-fno-exceptions -flto -fno-permissive -DNDEBUG -DFF_BOUNDED_BUFFER \
-DDEFAULT_BUFFER_CAPACITY=32786

INCLUDE_FLAGS = -I$(HOME)/.local/include -I$(HOME)/fastflow \
-I$(HOME)/.local/include/wf

LIBS = -pthread

MO := mo
MO_SRCS := $(MO:=.cpp)
MO_OBJS := $(MO_SRCS:.cpp=.o)

ifneq (, $(shell which clang++))
	DEBUG_CXX = clang++
else
	DEBUG_CXX = g++
endif

all: $(MO)

debug: CXXFLAGS := $(CXXFLAGS) -fno-lto -Og -ggdb3 -fno-inline -UNDEBUG
debug: CXX := $(DEBUG_CXX)
debug: all

debug-optimized: CXXFLAGS := $(CXXFLAGS) -fno-lto -g3 -UNDEBUG
debug-optimized: all

profile: CXXFLAGS := $(CXXFLAGS) -fno-lto -g3
profile: all

clean:
	rm -rf $(MO) $(MO_OBJS)

$(MO_OBJS): %.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDE_FLAGS) -c $< -o $@

$(MO) : %: %.o
	$(CXX) $< $(LIBS) -o $@
