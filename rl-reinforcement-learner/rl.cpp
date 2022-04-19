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


int main(int argc, char *argv[]) {
    // TODO
    return 0;
}
