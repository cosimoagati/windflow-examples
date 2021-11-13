EXAMPLES:= example1
CXX = g++
CXXFLAGS = -std=c++17
INCLUDE_FLAGS = -I$(HOME)/.local/include -I$(HOME)/fastflow	\
-I$(HOME)/.local/include/wf
LIBS = -pthread
SRCS:=$(EXAMPLES).c
OBJS:=$(SRCS:.c=.o)

all: $(EXAMPLES)

clean:
	rm -f $(EXAMPLES) *.o

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDE_FLAGS) -o $@ -c $<

$(EXAMPLES): $(OBJS)
	$(CXX) $(LIBS) -o $@ $<
