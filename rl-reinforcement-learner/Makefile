CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -Wpedantic -pedantic -O3 \
-fno-exceptions -flto -fno-permissive -DNDEBUG -DFF_BOUNDED_BUFFER \
-DDEFAULT_BUFFER_CAPACITY=32786

INCLUDE_FLAGS = -I$(HOME)/.local/include -I$(HOME)/.local/include/gsl \
-I$(HOME)/fastflow -I$(HOME)/.local/include/wf

LIBS = -pthread

RL := rl
RL_SRCS := $(RL:=.cpp)
RL_OBJS := $(RL_SRCS:.cpp=.o)

ifneq (, $(shell which clang++))
	DEBUG_CXX = clang++
else
	DEBUG_CXX = g++
endif

all: $(RL)

debug: CXXFLAGS := $(CXXFLAGS) -fno-lto -Og -ggdb3 -fno-inline -UNDEBUG
debug: CXX := $(DEBUG_CXX)
debug: all

debug-optimized: CXXFLAGS := $(CXXFLAGS) -fno-lto -g3 -UNDEBUG
debug-optimized: all

clean:
	rm -rf $(RL) $(RL_OBJS)

$(RL_OBJS): %.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDE_FLAGS) -c $< -o $@

$(RL) : %: %.o
	$(CXX) $< $(LIBS) -o $@
