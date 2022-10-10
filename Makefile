CPU_OBJS:=$(CPU_SRCS:.cpp=.o)

all: cpu
debug: debug-cpu
debug-optimized: debug-cpu-optimized
profile: profile-cpu
tracing: tracing-cpu

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

sa-profile:
	$(MAKE) profile -C sa-sentiment-analysis
mo-profile:
	$(MAKE) profile -C mo-machine-outlier
tt-profile:
	$(MAKE) profile -C tt-trending-topics
rl-profile:
	$(MAKE) profile -C rl-reinforcement-learner
lp-profile:
	$(MAKE) profile -C lp-log-processing

sa-tracing:
	$(MAKE) tracing -C sa-sentiment-analysis
mo-tracing:
	$(MAKE) tracing -C mo-machine-outlier
tt-tracing:
	$(MAKE) tracing -C tt-trending-topics
rl-tracing:
	$(MAKE) tracing -C rl-reinforcement-learner
lp-tracing:
	$(MAKE) tracing -C lp-log-processing

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
profile-cpu: sa-profile mo-profile tt-profile rl-profile lp-profile
tracing-cpu: sa-tracing mo-tracing tt-tracing rl-tracing lp-tracing

clean: sa-clean mo-clean tt-clean rl-clean lp-clean
