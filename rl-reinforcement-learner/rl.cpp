#include <stduuid/uuid.h>
#include <utility>
#include <wf/windflow.hpp>

using namespace std;
using namespace wf;

struct Parameters {
    unsigned ctr_generator_parallelism {1};
    unsigned reward_source_parallelism {1};
    unsigned reinforcement_learner_parallelism {1};
    unsigned sink_parallelism {1};
    unsigned batch_size {0};
    unsigned duration {60};
    unsigned tuple_rate {1000};
    unsigned sampling_rate {100};
    bool     use_chaining {false};
};

enum class TupleType { Event, Reward };

struct Tuple {
    TupleType     tuple_type;
    unsigned long id;
    unsigned long timestamp;
};

class CTRGeneratorFunctor {
    static constexpr unsigned long default_max_rounds {10000};

    unsigned long round_num {1};
    unsigned long event_count {0};
    unsigned long max_rounds;

public:
    CTRGeneratorFunctor(unsigned long max_rounds = default_max_rounds)
        : max_rounds {max_rounds} {}

    bool has_next() {
        return round_num <= max_rounds;
    }

    void operator()(Source_Shipper<Tuple> &shipper) {}
};

class RewardSourceFunctor {
public:
    void operator()(Source_Shipper<Tuple> &shipper) {}
};

class ReinforcementLearnerFunctor {
public:
    void operator()(const Tuple &tuple, Shipper<Tuple> &shipper) {}
};

class SinkFunctor {
public:
    void operator()(optional<Tuple> &input);
};

static inline PipeGraph &build_graph(const Parameters &parameters,
                                     PipeGraph &       graph) {
    CTRGeneratorFunctor ctr_generator_functor;

    auto ctr_generator_node =
        Source_Builder {ctr_generator_functor}
            .withParallelism(parameters.ctr_generator_parallelism)
            .withName("ctr generator")
            .withOutputBatchSize(parameters.batch_size)
            .build();

    RewardSourceFunctor reward_source_functor;
    auto                reward_source_node =
        Source_Builder {reward_source_functor}
            .withParallelism(parameters.reward_source_parallelism)
            .withName("reward source")
            .withOutputBatchSize(parameters.batch_size)
            .build();

    ReinforcementLearnerFunctor reinforcement_learner_functor;
    auto                        reinforcement_learner_node =
        FlatMap_Builder {reinforcement_learner_functor}
            .withParallelism(parameters.reinforcement_learner_parallelism)
            .withName("reinforcement learner")
            .withOutputBatchSize(parameters.batch_size)
            .build();

    SinkFunctor sink_functor;
    auto        sink = Sink_Builder {sink_functor}
                    .withParallelism(parameters.sink_parallelism)
                    .withName("sink")
                    .build();

    MultiPipe &ctr_generator_pipe = graph.add_source(ctr_generator_node);
    MultiPipe &reward_source_pipe = graph.add_source(reward_source_node);
    MultiPipe &reinforcement_learner_pipe =
        ctr_generator_pipe.merge(reward_source_pipe);

    if (parameters.use_chaining) {
        reinforcement_learner_pipe.chain(reinforcement_learner_node);
    } else {
        reinforcement_learner_pipe.add(reinforcement_learner_node);
    }
    return graph;
}

int main(int argc, char *argv[]) {
    // TODO
    return 0;
}
