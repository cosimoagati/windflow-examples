CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -Wpedantic -pedantic -O3 \
-fno-exceptions -flto -fno-permissive -DNDEBUG -DFF_BOUNDED_BUFFER \
-DDEFAULT_BUFFER_CAPACITY=32786

INCLUDE_FLAGS = -I$(HOME)/.local/include -I$(HOME)/fastflow \
-I$(HOME)/.local/include/wf

LIBS = -pthread

TT := tt
TT_SRCS := $(TT:=.cpp)
TT_OBJS := $(TT_SRCS:.cpp=.o)

ifneq (, $(shell which clang++))
	DEBUG_CXX = clang++
else
	DEBUG_CXX = g++
endif

all: $(TT)

debug: CXXFLAGS := $(CXXFLAGS) -fno-lto -Og -ggdb3 -fno-inline -UNDEBUG
debug: CXX := $(DEBUG_CXX)
debug: all

debug-optimized: CXXFLAGS := $(CXXFLAGS) -fno-lto -g3 -UNDEBUG
debug-optimized: all

profile: CXXFLAGS := $(CXXFLAGS) -fno-lto -g3
profile: all

clean:
	rm -rf $(TT) $(TT_OBJS)

$(TT_OBJS): %.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDE_FLAGS) -c $< -o $@

$(TT) : %: %.o
	$(CXX) $< $(LIBS) -o $@
