CPU_EXAMPLES:= example1 example2
GPU_EXAMPLES:= example3
CXX = g++
CXXFLAGS = -std=c++17 -g -O0
INCLUDE_FLAGS = -I$(HOME)/.local/include -I$(HOME)/fastflow	\
-I$(HOME)/.local/include/wf
LIBS = -pthread

NVXX = /usr/local/cuda/bin/nvcc
NVXXFLAGS = -std=c++17 -x cu --compiler-options "-Wall -Wextra -Wpedantic"
NVOPTFLAGS = -w --expt-extended-lambda -g -G -O0 -Wno-deprecated-gpu-targets	\
--expt-relaxed-constexpr

ARCH = $(shell arch)
ifeq ($(ARCH), x86_64)
	NVOPTFLAGS := $(NVOPTFLAGS) -gencode arch=compute_35,code=sm_35
endif
ifeq ($(ARCH), aarch64)
	NVOPTFLAGS := $(NVOPTFLAGS) -gencode arch=compute_53,code=sm_53
endif

CPU_SRCS:=$(CPU_EXAMPLES:=.cpp)
GPU_SRCS:=$(GPU_EXAMPLES:=.cu)
CPU_OBJS:=$(CPU_SRCS:.cpp=.o)
GPU_OBJS:=$(GPU_SRCS:.cu=.o)

all: $(CPU_EXAMPLES) $(GPU_EXAMPLES)

clean:
	rm -f $(CPU_EXAMPLES) $(GPU_EXAMPLES) *.o

$(CPU_OBJS): %.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDE_FLAGS) -c $< -o $@

$(GPU_OBJS): %.o: %.cu
	$(NVXX) $(NVXXFLAGS) $(INCLUDE_FLAGS) $(MACRO) $(NVOPTFLAGS) \
	$(USER_DEFINES) -c $< -o $@

$(CPU_EXAMPLES): %: %.o
	$(CXX) $< $(LIBS) -o $@

$(GPU_EXAMPLES): %: %.o
	$(NVXX) -lcuda -lcudart -lcublas $< -o $@
