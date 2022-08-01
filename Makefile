GPU_EXAMPLES:= example3 example4 example5

GPULIBS = -ltbb
NVXX = /usr/local/cuda/bin/nvcc
NVXXFLAGS = -std=c++17 -x cu --compiler-options \
"-Wall -Wextra -Wpedantic -pedantic"

NVOPTFLAGS = -w --expt-extended-lambda -O3 -Wno-deprecated-gpu-targets \
--expt-relaxed-constexpr

ARCH = $(shell arch)
ifeq ($(ARCH), x86_64)
	NVOPTFLAGS := $(NVOPTFLAGS) -gencode arch=compute_35,code=sm_35
endif
ifeq ($(ARCH), aarch64)
	NVOPTFLAGS := $(NVOPTFLAGS) -gencode arch=compute_53,code=sm_53
endif

GPU_SRCS:=$(GPU_EXAMPLES:=.cu)
CPU_OBJS:=$(CPU_SRCS:.cpp=.o)
GPU_OBJS:=$(GPU_SRCS:.cu=.o)

all: cpu gpu

debug-gpu: NVOPTFLAGS := $(NVOPTFLAGS) -g -G -O0
debug-gpu: gpu

debug: debug-cpu debug-gpu

sa:
	$(MAKE) -C sa-sentiment-analysis
mo:
	$(MAKE) -C mo-machine-outlier
tt:
	$(MAKE) -C tt-trending-topics
rl:
	$(MAKE) -C rl-reinforcement-learner
lp:
	$(MAKE) -C lp-log-processing

sa-debug:
	$(MAKE) debug -C sa-sentiment-analysis
mo-debug:
	$(MAKE) debug -C mo-machine-outlier
tt-debug:
	$(MAKE) debug -C tt-trending-topics
rl-debug:
	$(MAKE) debug -C rl-reinforcement-learner
lp-debug:
	$(MAKE) debug -C lp-log-processing

sa-debug-optimized:
	$(MAKE) debug-optimized -C sa-sentiment-analysis
mo-debug-optimized:
	$(MAKE) debug-optimized -C mo-machine-outlier
tt-debug-optimized:
	$(MAKE) debug-optimized -C tt-trending-topics
rl-debug-optimized:
	$(MAKE) debug-optimized -C rl-reinforcement-learner
lp-debug-optimized:
	$(MAKE) debug-optimized -C lp-log-processing

sa-clean:
	$(MAKE) clean -C sa-sentiment-analysis
mo-clean:
	$(MAKE) clean -C mo-machine-outlier
tt-clean:
	$(MAKE) clean -C tt-trending-topics
rl-clean:
	$(MAKE) clean -C rl-reinforcement-learner
lp-clean:
	$(MAKE) clean -C lp-log-processing

cpu: sa mo tt rl lp
debug-cpu: sa-debug mo-debug tt-debug rl-debug lp-debug
debug-cpu-optimized: sa-debug-optimized mo-debug-optimized tt-debug-optimized \
rl-debug-optimized lp-debug-optimized

gpu: $(GPU_EXAMPLES)

clean: sa-clean mo-clean tt-clean rl-clean lp-clean
	rm -f $(GPU_EXAMPLES) $(CPU_OBJS) $(GPU_OBJS)

$(GPU_OBJS): %.o: %.cu
	$(NVXX) $(NVXXFLAGS) $(INCLUDE_FLAGS) $(MACRO) $(NVOPTFLAGS) \
	$(USER_DEFINES) -c $< -o $@

$(GPU_EXAMPLES): %: %.o
	$(NVXX) $(GPULIBS) $< -o $@
