CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -Wpedantic -pedantic -O3 \
-fno-exceptions -flto -fno-permissive -DNDEBUG -DFF_BOUNDED_BUFFER \
-DDEFAULT_BUFFER_CAPACITY=32768

INCLUDE_FLAGS = -I$(HOME)/.local/include -I$(HOME)/fastflow \
-I$(HOME)/.local/include/wf
LIB_PATH_FLAGS = -L$(HOME)/.local/lib

LIBS = -pthread -lmaxminddb

LP := lp
LP_SRCS := $(LP:=.cpp)
LP_OBJS := $(LP_SRCS:.cpp=.o)

ifneq (, $(shell which clang++))
	DEBUG_CXX = clang++
else
	DEBUG_CXX = g++
endif

all: $(LP)

debug: CXXFLAGS := $(CXXFLAGS) -fno-lto -Og -ggdb3 -fno-inline -UNDEBUG
debug: CXX := $(DEBUG_CXX)
debug: all

debug-optimized: CXXFLAGS := $(CXXFLAGS) -fno-lto -g3 -UNDEBUG
debug-optimized: all

profile: CXXFLAGS := $(CXXFLAGS) -fno-lto -g3 -DPROFILE
profile: all

tracing: CXXFLAGS := $(CXXFLAGS) -DWF_TRACING_ENABLED
tracing: LIBS := $(LIBS) -ltbb -lgvc -lcgraph
tracing: all

clean:
	rm -rf $(LP) $(LP_OBJS)

$(LP_OBJS): %.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDE_FLAGS) -c $< -o $@

$(LP) : %: %.o
	$(CXX) $< $(LIB_PATH_FLAGS) $(LIBS) -o $@
