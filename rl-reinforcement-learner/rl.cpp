#include <stduuid/uuid.h>
#include <utility>
#include <wf/windflow.hpp>

using namespace std;
using namespace wf;

struct TupleMetadata {
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
    unsigned long id;
    unsigned long timestamp;
};

struct Event {
    TupleMetadata metadata;
};

struct Reward {
    TupleMetadata metadata;
};

class CTRGeneratorFunctor {};

class RewardSourceFunctor {};

class ReinforcementLearnerFunctor {};

int main(int argc, char *argv[]) {
    // TODO
    return 0;
}
