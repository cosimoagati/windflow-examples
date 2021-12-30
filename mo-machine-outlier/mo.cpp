#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <regex>
#include <string>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>
#include <wf/windflow.hpp>

using namespace std;
using namespace std::chrono;
using namespace wf;

atomic_ulong g_sent_tuples;
atomic_ulong g_average_latency;

template<typename TimeUnit>
constexpr auto timeunit_to_string = "time unit";
template<>
constexpr auto timeunit_to_string<milliseconds> = "millisecond";
template<>
constexpr auto timeunit_to_string<seconds> = "second";
template<>
constexpr auto timeunit_to_string<microseconds> = "microsecond";
template<>
constexpr auto timeunit_to_string<nanoseconds> = "nanosecond";

struct SourceTuple {
    string                   tweet;
    time_point<steady_clock> timestamp;
};

template<typename TimeUnit>
struct MapOutputTuple {
    string   tweet;
    TimeUnit latency;
};

class SourceFunctor {
    static constexpr auto default_path = "example-dataset.txt";

    vector<string> dataset;
    unsigned long  total_tuples;

public:
    SourceFunctor(const string &path, unsigned long t)
        : dataset {}, total_tuples {t} {}

    SourceFunctor(unsigned long t) : SourceFunctor {default_path, t} {}

    SourceFunctor() : SourceFunctor {default_path, 1000} {}

    void operator()(Source_Shipper<SourceTuple> &shipper) {
        size_t        index {0};
        unsigned long sent_tuples {0};

        while (sent_tuples < total_tuples) {
            shipper.push({dataset[index], steady_clock::now()});
            ++sent_tuples;
            index = (index + 1) % dataset.size();
        }
        g_sent_tuples.store(sent_tuples);
    }
};

template<typename TimeUnit>
class MapFunctor {

public:
    MapFunctor() = default;

    MapOutputTuple<TimeUnit> operator()(const SourceTuple &tuple) {
        return {};
    }
};

template<typename TimeUnit>
class SinkFunctor {
    static constexpr auto  verbose_output = false;
    unsigned long          tuples_received {0};
    typename TimeUnit::rep total_average {0};

public:
    void operator()(optional<MapOutputTuple<TimeUnit>> &input) {
        if (input) {
            ++tuples_received;
            total_average += input->latency.count();

            if constexpr (verbose_output) {
                cout << "Received tweet \"" << input->tweet << "\" with score "
                     << input->result.second << " and classification "
                     << sentiment_to_string(input->result.first) << "\n";
            }
        } else {
            g_average_latency.store(total_average / tuples_received);
        }
    }
};

static inline void parse_and_validate_args(int argc, char **argv,
                                           unsigned long &total_tuples,
                                           unsigned int & map_parallelism,
                                           bool &         use_chaining) {
    int option;
    while ((option = getopt(argc, argv, "t:m:c:")) != -1) {
        switch (option) {
        case 't':
            total_tuples = atol(optarg);
            break;
        case 'm':
            map_parallelism = atoi(optarg);
            break;
        case 'c': {
            const auto opt_string = string {optarg};
            if (opt_string == string {"true"}) {
                use_chaining = true;
            } else if (opt_string == string {"false"}) {
                use_chaining = false;
            } else {
                cerr << "Error: chaining option must be either \"true\" or "
                        "\"false\"\n";
                exit(EXIT_FAILURE);
            }
            break;
        }
        default:
            cerr << "Use as " << argv[0]
                 << " [-c true|false] -t <tuples> -m <parallelism>\n";
            exit(EXIT_FAILURE);
        }
    }

    if (total_tuples <= 0) {
        cerr << "Error: number of tuples is not positive\n";
        exit(EXIT_FAILURE);
    }

    if (map_parallelism <= 0) {
        cerr << "Error: Map parallelism degree is not positive\n";
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char *argv[]) {
    using TimeUnit = microseconds;

    const auto start_time      = steady_clock::now();
    auto       use_chaining    = false;
    auto       map_parallelism = 0u;
    auto       total_tuples    = 0ul;

    parse_and_validate_args(argc, argv, total_tuples, map_parallelism,
                            use_chaining);
    SourceFunctor source_functor {total_tuples};
    auto          source = Source_Builder {source_functor}
                      .withParallelism(1)
                      .withName("source")
                      .build();

    MapFunctor<TimeUnit> map_functor;
    auto                 classifier_node = Map_Builder {map_functor}
                               .withParallelism(map_parallelism)
                               .withName("counter")
                               .build();

    SinkFunctor<TimeUnit> sink_functor;
    auto                  sink =
        Sink_Builder {sink_functor}.withParallelism(1).withName("sink").build();

    PipeGraph graph {"sa-sentiment-analysis", Execution_Mode_t::DEFAULT,
                     Time_Policy_t::INGRESS_TIME};
    if (use_chaining) {
        graph.add_source(source).chain(classifier_node).chain_sink(sink);
    } else {
        graph.add_source(source).add(classifier_node).add_sink(sink);
    }
    graph.run();

    const auto elapsed_time =
        duration_cast<TimeUnit>(steady_clock::now() - start_time);
    const auto throughput =
        elapsed_time.count() > 0
            ? g_sent_tuples.load() / (double) elapsed_time.count()
            : g_sent_tuples.load();
    const auto service_time = 1 / throughput;

    cout << "Elapsed time: " << elapsed_time.count() << ' '
         << timeunit_to_string<TimeUnit> << "s\n";
    cout << "Processed about " << throughput << " tuples per "
         << timeunit_to_string<TimeUnit> << '\n';
    cout << "Service time: " << service_time << ' '
         << timeunit_to_string<TimeUnit> << "s\n";
    cout << "Average latency is " << g_average_latency << ' '
         << timeunit_to_string<TimeUnit> << "s\n";
    return 0;
}