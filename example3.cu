#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <wf/windflow.hpp>
#include <wf/windflow_gpu.hpp>

using namespace std;
using namespace wf;

constexpr auto MAX_LEN_RECORD = 256;

struct Record {
    int  customer_id;
    char record[MAX_LEN_RECORD];
    bool is_fraud;
};

class PredictionModel {
public:
    __device__      PredictionModel() {}
    __device__ void add_record(Record &record) {}
    __device__ bool update_classification() {
        // dummy body
        return true;
    }
};

class Source_Functor {
    vector<Record> records;

public:
    Source_Functor(const vector<Record> &records) : records {records} {}

    void operator()(Source_Shipper<Record> &shipper) {
        for (const auto &record : records) {
            shipper.push(record);
            cout << "Sent record containing string " << record.record << '\n';
        }
    }
};

struct Filter_Functor {
    __device__ bool operator()(Record &input) {
        return !(input.record[0] == '\0');
    }
};

struct Map_Functor {
    __device__ void operator()(Record &input, PredictionModel &model) {
        model.add_record(input);
        input.is_fraud = model.update_classification();
    }
};

class Sink_Functor {
    unsigned counter;

public:
    Sink_Functor() : counter {0} {}

    void operator()(optional<Record> &input) {
        if (input) {
            ++counter;
            cout << "Received word number " << counter << " containing "
                 << input->record << "\n";
        } else {
            cout << "End of stream!" << endl;
        }
    }
};

static vector<Record> get_record_vector(const int          argc,
                                        const char **const argv) {
    vector<Record> dataset;

    for (auto i = 2; i < argc; ++i) {
        dataset.emplace_back();
        dataset.back().customer_id = i;
        dataset.back().is_fraud    = false;
        strncpy(dataset.back().record, argv[i], MAX_LEN_RECORD);
    }
    return dataset;
}

static bool get_chaining_option(const char *const arg) {
    if (string {arg} == "true") {
        return true;
    } else if (string {arg} == "false") {
        return false;
    } else {
        cerr << "Use as " << arg << " true|false <strings...>\n";
        exit(EXIT_FAILURE);
    }
}

int main(const int argc, const char *argv[]) {
    if (argc < 2) {
        cerr << "Use as " << argv[0] << " true|false <strings...>\n";
        return -1;
    }

    const auto use_chaining = get_chaining_option(argv[1]);
    const auto records      = get_record_vector(argc, argv);
    for (const auto &record : records) {
        cout << record.record << endl;
    }

    Source_Functor source_functor {records};
    auto           source = Source_Builder {source_functor}
                      .withParallelism(1)
                      .withName("source")
                      .withOutputBatchSize(1000)
                      .build();

    Filter_Functor filter_functor;
    auto           filter = FilterGPU_Builder {filter_functor}
                      .withParallelism(2)
                      .withName("filter")
                      .build();

    Map_Functor map_functor;
    auto        map =
        MapGPU_Builder {map_functor}
            .withParallelism(2)
            .withName("map")
            .withKeyBy([] __host__ __device__(const Record &record) -> int {
                return record.customer_id;
            })
            .build();

    Sink_Functor sink_functor;
    auto         sink =
        Sink_Builder {sink_functor}.withParallelism(3).withName("sink").build();

    PipeGraph graph {"graph", Execution_Mode_t::DEFAULT,
                     Time_Policy_t::INGRESS_TIME};
    if (use_chaining) {
        graph.add_source(source).chain(filter).add(map).chain_sink(sink);
    } else {
        graph.add_source(source).add(filter).add(map).add_sink(sink);
    }
    graph.run();

    return 0;
}
