CPU_EXAMPLES:= example1 example2
GPU_EXAMPLES:= example3
CXX = g++
CXXFLAGS = -std=c++17
INCLUDE_FLAGS = -I$(HOME)/.local/include -I$(HOME)/fastflow	\
-I$(HOME)/.local/include/wf
LIBS = -pthread
CPU_SRCS:=$(CPU_EXAMPLES:=.cpp)
GPU_SRCS:=$(GPU_EXAMPLES:=.cu)
CPU_OBJS:=$(CPU_SRCS:.cpp=.o)
GPU_OBJS:=$(GPU_SRCS:.cu=.o)

all: $(CPU_EXAMPLES)

clean:
	rm -f $(CPU_EXAMPLES) *.o

$(CPU_OBJS): %.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDE_FLAGS) -o $@ -c $<

$(CPU_EXAMPLES): %: %.o
	$(CXX) $(LIBS) -o $@ $^
