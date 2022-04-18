#include <wf/windflow.hpp>

using namespace std;
using namespace wf;

struct TupleMetadata {
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
