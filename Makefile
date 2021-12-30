CPU_EXAMPLES:= example1 example2 sa-sentiment-analysis/sa mo-machine-outlier/mo
GPU_EXAMPLES:= example3 example4 example5
CXXFLAGS = -std=c++17 -O3 -fno-exceptions
INCLUDE_FLAGS = -I$(HOME)/.local/include -I$(HOME)/fastflow	\
-I$(HOME)/.local/include/wf
LIBS = -pthread
GPULIBS = -ltbb

ifneq (, $(shell which clang++))
	CXX = clang++
else
	CXX = g++
endif

NVXX = /usr/local/cuda/bin/nvcc
NVXXFLAGS = -std=c++17 -x cu --compiler-options \
	"-Wall -Wextra -Wpedantic -pedantic"
NVOPTFLAGS = -w --expt-extended-lambda -O3 -Wno-deprecated-gpu-targets	\
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

all: cpu gpu

debug-cpu: CXXFLAGS := $(CXXFLAGS) -fno-lto -Og -g -fno-inline
debug-cpu: cpu

debug-gpu: NVOPTFLAGS := $(NVOPTFLAGS) -g -G -O0
debug-gpu: gpu

debug: debug-cpu debug-gpu

cpu: $(CPU_EXAMPLES)
gpu: $(GPU_EXAMPLES)

clean:
	rm -f $(CPU_EXAMPLES) $(GPU_EXAMPLES) $(CPU_OBJS) $(GPU_OBJS)

$(CPU_OBJS): %.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDE_FLAGS) -c $< -o $@

$(GPU_OBJS): %.o: %.cu
	$(NVXX) $(NVXXFLAGS) $(INCLUDE_FLAGS) $(MACRO) $(NVOPTFLAGS) \
	$(USER_DEFINES) -c $< -o $@

$(CPU_EXAMPLES): %: %.o
	$(CXX) $< $(LIBS) -o $@

$(GPU_EXAMPLES): %: %.o
	$(NVXX) $(GPULIBS) $< -o $@
