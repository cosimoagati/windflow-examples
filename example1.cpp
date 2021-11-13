/*
 * The dataset is formed by the strings passed as input to the program.  Run as
 * ./example1 <string1> <string2> ...
 */

#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <wf/windflow.hpp>

using namespace std;

class Source_Functor {
    vector<string> dataset;

public:
    Source_Functor(const vector<string> &dataset) : dataset {dataset} {}

    void operator()(wf::Source_Shipper<string> &shipper) {
        for (const auto &line : dataset) {
            shipper.push(line);
        }
    }
};

class Splitter_Functor {
public:
    void operator()(const string &input, wf::Shipper<string> &shipper) {
        istringstream line {input};
        string        word;

        while (getline(line, word, ' ')) {
            shipper.push(std::move(word));
        }
    }
};

class Counter_Functor {
    unordered_map<string, int> table;

public:
    pair<string, int> operator()(const string &word) {
        if (table.find(word) == table.end()) {
            table.insert(make_pair(word, 0));
        } else {
            table.at(word)++;
        }
        return {word, table.at(word)};
    }
};

class Sink_Functor {
public:
    void operator()(optional<pair<string, int>> &input) {
        if (input) {
            cout << "Received word " << (*input).first << " with counter "
                 << (*input).second << "\n";
        } else {
            cout << "End of stream\n" << endl;
        }
    }
};

int main(const int argc, const char *const argv[]) {
    const auto chaining = false;

    vector<string> dataset;
    for (auto i = 1; i < argc; ++i) {
        dataset.push_back(argv[i]);
    }

    Source_Functor source_functor {dataset};
    auto           source = wf::Source_Builder(source_functor)
                      .withParallelism(2)
                      .withName("wc_source")
                      .build();

    Splitter_Functor splitter_functor;
    auto             splitter = wf::FlatMap_Builder(splitter_functor)
                        .withParallelism(2)
                        .withName("wc_splitter")
                        .withOutputBatchSize(10)
                        .build();

    Counter_Functor counter_functor;
    auto            counter = wf::Map_Builder(counter_functor)
                       .withParallelism(3)
                       .withName("wc_counter")
                       .withKeyBy([](const std::string &word) -> std::string {
                           return word;
                       })
                       .build();

    Sink_Functor sink_functor;
    auto         sink = wf::Sink_Builder(sink_functor)
                    .withParallelism(3)
                    .withName("wc_sink")
                    .build();

    wf::PipeGraph topology {"wc", wf::Execution_Mode_t::DEFAULT,
                            wf::Time_Policy_t::INGRESS_TIME};
    if (chaining) {
        topology.add_source(source).chain(splitter).add(counter).chain_sink(
            sink);
    } else {
        topology.add_source(source).add(splitter).add(counter).add_sink(sink);
    }
    topology.run(); // synchronous execution of the dataflow

    return 0;
}
