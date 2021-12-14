/*
 * The dataset is formed by the strings passed as input to the program.  Run as
 * ./example1 <string1> <string2> ...
 */
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

struct record_t {
    int  customer_id;
    char record[MAX_LEN_RECORD];
    bool is_fraud;

    record_t() : customer_id {0}, is_fraud {false} {
        record[0] = '\0';
    }

    record_t(int customer_id, const char *record, bool is_fraud)
        : customer_id {customer_id}, is_fraud {is_fraud} {
        strncpy(this->record, record, MAX_LEN_RECORD);
    }
};

class PredictionModel {
public:
    __device__      PredictionModel() {}
    __device__ void add_record(record_t &record) {}
    __device__ bool update_classification() {
        // dummy body
        return true;
    }
};

class Source_Functor {
    vector<record_t> records;

public:
    Source_Functor(const vector<record_t> &records) : records {records} {}

    void operator()(Source_Shipper<record_t> &shipper) {
        for (auto record : records) {
            shipper.push(record);
            cout << "Sent record containing string " << record.record << '\n';
        }
    }
};

struct Filter_Functor {
    __device__ bool operator()(record_t &input) {
        return !(input.record[0] == '\0');
    }
};

struct Map_Functor {
    __device__ void operator()(record_t &input, PredictionModel &model) {
        model.add_record(input);
        input.is_fraud = model.update_classification();
    }
};

class Sink_Functor {
    unsigned counter;

public:
    Sink_Functor() : counter {0} {}

    void operator()(optional<record_t> &input) {
        if (input) {
            ++counter;
            cout << "Received word number " << counter << "\n";
        } else {
            cout << "End of stream!" << endl;
        }
    }
};

vector<record_t> get_record_vector(const int argc, const char **const argv) {
    vector<record_t> dataset;

    for (auto i = 1; i < argc; ++i) {
        record_t record {i, argv[i], false};
        dataset.push_back(record);
    }
    return dataset;
}

int main(const int argc, const char *argv[]) {
    if (argc < 2) {
        cerr << "Use as " << argv[0] << "<strings...> \n";
        return -1;
    }

    auto records = get_record_vector(argc, argv);
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
            .withKeyBy([] __host__ __device__(const record_t &record) -> int {
                return record.customer_id;
            })
            .build();

    Sink_Functor sink_functor;
    auto         sink =
        Sink_Builder {sink_functor}.withParallelism(3).withName("sink").build();

    PipeGraph graph {"graph", Execution_Mode_t::DEFAULT,
                     Time_Policy_t::INGRESS_TIME};
    graph.add_source(source).add(filter).add(map).add_sink(sink);
    graph.run();
    return 0;
}
