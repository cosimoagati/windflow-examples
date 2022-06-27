/*
 * Copyright (C) 2021-2022 Cosimo Agati
 *
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * You should have received a copy of the GNU AGPLv3 with this software,
 * if not, please visit <https://www.gnu.org/licenses/>
 */

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <deque>
#include <fstream>
#include <getopt.h>
#include <iostream>
#include <limits>
#include <mutex>
#include <nlohmann/json.hpp>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <valarray>
#include <vector>

#include "../util.hpp"

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wreorder"
#pragma GCC diagnostic ignored "-Wextra"
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Woverloaded-virtual"
#endif

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wall"
#pragma clang diagnostic ignored "-Wunused-private-field"
#endif

#include <wf/windflow.hpp>

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

#ifdef __clang__
#pragma clang diagnostic pop
#endif

using namespace std;
using namespace wf;

enum NodeId : unsigned {
    source_id          = 0,
    observer_id        = 1,
    anomaly_scorer_id  = 2,
    alert_triggerer_id = 3,
    sink_id            = 4,
    num_nodes          = 5
};

struct Parameters {
    const char *     metric_output_directory = ".";
    Execution_Mode_t execution_mode          = Execution_Mode_t::DETERMINISTIC;
    Time_Policy_t    time_policy             = Time_Policy_t::INGRESS_TIME;
    unsigned         parallelism[num_nodes]  = {1, 1, 1, 1, 1};
    unsigned         batch_size[num_nodes - 1] = {0, 0, 0, 0};
    unsigned         duration                  = 60;
    unsigned         tuple_rate                = 1000;
    unsigned         sampling_rate             = 100;
    bool             use_chaining              = false;
};

struct MachineMetadata {
    string        machine_ip;
    double        cpu_usage;
    double        memory_usage;
    double        score;
    unsigned long timestamp;
};

#ifndef NDEBUG
ostream &operator<<(ostream &stream, const MachineMetadata &metadata) {
    stream << "Machine ip: " << metadata.machine_ip
           << ", CPU usage: " << metadata.cpu_usage
           << ", memory usage: " << metadata.memory_usage
           << ", observation timestamp: " << metadata.timestamp;
    return stream;
}
#endif

template<typename T>
struct ScorePackage {
    string id;
    double score;
    T      data;
};

struct SourceTuple {
    MachineMetadata observation;
    unsigned long   execution_timestamp;
};

struct ObservationResultTuple {
    string          id;
    double          score;
    unsigned long   observation_timestamp;
    unsigned long   execution_timestamp;
    MachineMetadata observation;
};

struct AnomalyResultTuple {
    string          id;
    double          anomaly_score;
    unsigned long   observation_timestamp;
    unsigned long   execution_timestamp;
    MachineMetadata observation;
    double          individual_score;
};

struct AlertTriggererResultTuple {
    string          id;
    double          anomaly_score;
    unsigned long   observation_timestamp;
    unsigned long   execution_timestamp;
    bool            is_abnormal;
    MachineMetadata observation; // XXX This field may not be correct!
};

static const struct option long_opts[] = {{"help", 0, 0, 'h'},
                                          {"rate", 1, 0, 'r'},
                                          {"sampling", 1, 0, 's'},
                                          {"parallelism", 1, 0, 'p'},
                                          {"batch", 1, 0, 'b'},
                                          {"chaining", 1, 0, 'c'},
                                          {"duration", 1, 0, 'd'},
                                          {"execmode", 1, 0, 'e'},
                                          {"timepolicy", 1, 0, 't'},
                                          {"outputdir", 1, 0, 'o'},
                                          {0, 0, 0, 0}};

static inline optional<MachineMetadata>
parse_google_trace(const string &trace) {
    const auto      values             = string_split(trace, ',');
    const auto      timestamp_index    = 0;
    const auto      machine_id_index   = 4;
    const auto      cpu_usage_index    = 5;
    const auto      memory_usage_index = 6;
    MachineMetadata metadata;

    if (values.size() != 19) {
        // cerr << "Ill-formed line!\n";
        // cerr << "Offending line looks like: " << trace << '\n';
        return {};
    }

    metadata.machine_ip   = values[machine_id_index];
    metadata.timestamp    = stoul(values[timestamp_index].data());
    metadata.cpu_usage    = stod(values[cpu_usage_index].data()) * 10;
    metadata.memory_usage = stod(values[memory_usage_index].data()) * 10;
    return metadata;
}

static inline optional<MachineMetadata>
parse_alibaba_trace(const string &trace) {
    const auto values = string_split(trace, ',');
    if (values.size() != 7) {
        return {};
    }

    const unsigned  timestamp_index    = 1;
    const unsigned  machine_id_index   = 0;
    const unsigned  cpu_usage_index    = 2;
    const unsigned  memory_usage_index = 3;
    MachineMetadata metadata;

    metadata.machine_ip   = values[machine_id_index];
    metadata.timestamp    = stoul(values[timestamp_index].data()) * 1000;
    metadata.cpu_usage    = stod(values[cpu_usage_index].data());
    metadata.memory_usage = stod(values[memory_usage_index].data());
    return metadata;
}

static inline double eucledean_norm(const valarray<double> &elements) {
    double result = 0.0;
    for (const auto &x : elements) {
        result += pow(x, 2.0);
    }
    return sqrt(result);
}

template<optional<MachineMetadata> parse_trace(const string &)>
static inline vector<MachineMetadata> parse_metadata(const char *filename) {
    ifstream                metadata_stream {filename};
    vector<MachineMetadata> metadata_info;

    for (string line; getline(metadata_stream, line);) {
        auto metadata = parse_trace(line);
        if (metadata) {
            metadata_info.push_back(move(*metadata));
        }
    }
    return metadata_info;
}

static inline void parse_args(int argc, char **argv, Parameters &parameters) {
    int option;
    int index;

    while ((option = getopt_long(argc, argv, "r:s:p:b:c:d:o:e:t:h", long_opts,
                                 &index))
           != -1) {
        switch (option) {
        case 'r':
            parameters.tuple_rate = atoi(optarg);
            break;
        case 's':
            parameters.sampling_rate = atoi(optarg);
            break;
        case 'b': {
            const auto batches = get_nums_split_by_commas(optarg);
            if (batches.size() != num_nodes - 1) {
                cerr << "Error in parsing the input arguments.  Batch sizes "
                        "string requires exactly "
                     << (num_nodes - 1) << " elements\n";
                exit(EXIT_FAILURE);
            } else {
                for (unsigned i = 0; i < num_nodes - 1; ++i) {
                    parameters.batch_size[i] = batches[i];
                }
            }
        } break;
        case 'p': {
            const auto degrees = get_nums_split_by_commas(optarg);
            if (degrees.size() != num_nodes) {
                cerr << "Error in parsing the input arguments.  Parallelism "
                        "degree string requires exactly "
                     << num_nodes << " elements.\n";
                exit(EXIT_FAILURE);
            } else {
                for (unsigned i = 0; i < num_nodes; ++i) {
                    parameters.parallelism[i] = degrees[i];
                }
            }
        } break;
        case 'c':
            parameters.use_chaining = get_chaining_value_from_string(optarg);
            break;
        case 'd':
            parameters.duration = atoi(optarg);
            break;
        case 'o':
            parameters.metric_output_directory = optarg;
            break;
        case 'e':
            parameters.execution_mode = get_execution_mode_from_string(optarg);
            break;
        case 't':
            parameters.time_policy = get_time_policy_from_string(optarg);
            break;
        case 'h':
            cout << "Parameters: --rate <value> --sampling "
                    "<value> --batch <size> --parallelism "
                    "<nSource,nObserver,nAnomalyScorer,nAlertTriggerer,nSink> "
                    "[--duration <seconds>] [--chaining <value>]\n";
            exit(EXIT_SUCCESS);
            break;
        default:
            cerr << "Error in parsing the input arguments.  Use the --help "
                    "(-h) option for usage information.\n";
            exit(EXIT_FAILURE);
            break;
        }
    }
}

static inline void validate_args(const Parameters &parameters) {
    if (parameters.duration == 0) {
        cerr << "Error: duration must be positive\n";
        exit(EXIT_FAILURE);
    }

    for (unsigned i = 0; i < num_nodes; ++i) {
        if (parameters.parallelism[i] == 0) {
            cerr << "Error: parallelism degree for node " << i
                 << " must be positive\n";
            exit(EXIT_FAILURE);
        }
    }

    const auto max_threads = thread::hardware_concurrency();

    for (unsigned i = 0; i < num_nodes; ++i) {
        if (parameters.parallelism[i] > max_threads) {
            cerr << "Error:  parallelism degree for node " << i
                 << " is too large\n"
                    "Maximum available number of threads is: "
                 << max_threads << '\n';
        }
    }

    if (accumulate(cbegin(parameters.parallelism),
                   cend(parameters.parallelism), 0u)
            >= max_threads
        && !parameters.use_chaining) {
        cerr << "Error: the total number of hardware threads specified is "
                "too high to be used without chaining.\n"
                "Maximum available number of threads is: "
             << max_threads << '\n';
        exit(EXIT_FAILURE);
    }
}

static inline void print_initial_parameters(const Parameters &parameters) {
    cout << "Running graph with the following parameters:\n"
         << "Source parallelism:\t" << parameters.parallelism[source_id]
         << '\n'
         << "Observer parallelism:\t" << parameters.parallelism[observer_id]
         << '\n'
         << "Anomaly scorer parallelism:\t"
         << parameters.parallelism[anomaly_scorer_id] << '\n'
         << "Alert triggerer parallelism:\t"
         << parameters.parallelism[alert_triggerer_id] << '\n'
         << "Sink parallelism:\t" << parameters.parallelism[sink_id] << '\n'
         << "Batching:\n";

    for (unsigned i = 0; i < num_nodes - 1; ++i) {
        cout << "\tNode " << i << ": ";
        if (parameters.batch_size[i] != 0) {
            cout << parameters.batch_size[i] << '\n';
        } else {
            cout << "none\n";
        }
    }

    cout << "Execution mode:\t"
         << get_string_from_execution_mode(parameters.execution_mode) << '\n';
    cout << "Time policy:\t"
         << get_string_from_time_policy(parameters.time_policy) << '\n';

    cout << "Duration:\t" << parameters.duration << " second"
         << (parameters.duration == 1 ? "" : "s") << '\n'
         << "Tuple generation rate: ";
    if (parameters.tuple_rate > 0) {
        cout << parameters.tuple_rate << " tuple"
             << (parameters.tuple_rate == 1 ? "" : "s") << " per second\n";
    } else {
        cout << "unlimited (BEWARE OF QUEUE CONGESTION)\n";
    }

    cout << "Sampling rate:\t";
    if (parameters.sampling_rate > 0) {
        cout << parameters.sampling_rate << " measurement"
             << (parameters.sampling_rate == 1 ? "" : "s") << " per second\n";
    } else {
        cout << "unlimited (sample every incoming tuple)\n";
    }

    cout << "Chaining:\t" << (parameters.use_chaining ? "enabled" : "disabled")
         << '\n';
}

/*
 * Global variables
 */
static atomic_ulong          global_sent_tuples {0};
static atomic_ulong          global_received_tuples {0};
static Metric<unsigned long> global_latency_metric {"mo-latency"};
#ifndef NDEBUG
static mutex print_mutex;
#endif

class SourceFunctor {
    static constexpr auto   default_path = "machine-usage.csv";
    vector<MachineMetadata> machine_metadata;
    unsigned long           measurement_timestamp_additional_amount = 0;
    unsigned long           measurement_timestamp_increase_step;
    unsigned long           duration;
    unsigned                tuple_rate_per_second;

public:
    SourceFunctor(unsigned d, unsigned rate, const char *path = default_path)
        : machine_metadata {parse_metadata<parse_alibaba_trace>(path)},
          duration {d * timeunit_scale_factor}, tuple_rate_per_second {rate} {
        if (machine_metadata.empty()) {
            cerr << "Error: empty machine reading stream.  Check whether "
                    "dataset file exists and is readable\n";
            exit(EXIT_FAILURE);
        }
    }

    void operator()(Source_Shipper<SourceTuple> &shipper) {
        const auto    end_time    = current_time() + duration;
        unsigned long sent_tuples = 0;
        size_t        index       = 0;

        while (current_time() < end_time) {
            auto current_observation = machine_metadata[index];
            current_observation.timestamp +=
                measurement_timestamp_additional_amount;
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[SOURCE] Sending out tuple with the following "
                        "observation: "
                     << current_observation << '\n';
            }
#endif
            index = (index + 1) % machine_metadata.size();
            if (index == 0) {
                if (measurement_timestamp_additional_amount == 0) {
                    measurement_timestamp_increase_step =
                        current_observation.timestamp;
                }
                measurement_timestamp_additional_amount +=
                    measurement_timestamp_increase_step;
            }

            const auto execution_timestamp = current_time();
            shipper.push({current_observation, execution_timestamp});
            ++sent_tuples;

            if (tuple_rate_per_second > 0) {
                const unsigned long delay =
                    (1.0 / tuple_rate_per_second) * timeunit_scale_factor;
                busy_wait(delay);
            }
        }
        global_sent_tuples.fetch_add(sent_tuples);
    }
};

class MachineMetadataScorer {
    static constexpr size_t cpu_idx    = 0;
    static constexpr size_t memory_idx = 1;

    valarray<double> calculate_distance(valarray<valarray<double>> &matrix) {
        assert(matrix.size() > 0);
#ifndef NDEBUG
        for (const auto &row : matrix) {
            assert(row.size() > 0);
        }
#endif
        valarray<double> mins(matrix[0].size());
        valarray<double> maxs(matrix[0].size());
        const auto       column_number = matrix[0].size();

        for (size_t col = 0; col < column_number; ++col) {
            auto min = numeric_limits<double>::min();
            auto max = numeric_limits<double>::max();

            for (size_t row {0}; row < matrix.size(); ++row) {
                if (matrix[row][col] < min) {
                    min = matrix[row][col];
                }
                if (matrix[row][col] > min) {
                    max = matrix[row][col];
                }
            }
            mins[col] = min;
            maxs[col] = max;
        }
        mins[cpu_idx] = 0.0;
        maxs[cpu_idx] = 1.0;

        mins[memory_idx] = 0.0;
        maxs[memory_idx] = 100.0;

        valarray<double> centers(0.0, column_number);
        for (size_t col = 0; col < column_number; ++col) {
            if (mins[col] == 0 && maxs[col] == 0) {
                continue;
            }
            for (size_t row = 0; row < matrix.size(); ++row) {
                matrix[row][col] =
                    (matrix[row][col] - mins[col]) / (maxs[col] - mins[col]);
                centers[col] += matrix[row][col];
            }
            centers[col] /= matrix.size();
        }

        valarray<valarray<double>> distances(
            valarray<double>(0.0, matrix[0].size()), matrix.size());

        for (size_t row = 0; row < matrix.size(); ++row) {
            for (size_t col {0}; col < matrix[row].size(); ++col) {
                distances[row][col] = abs(matrix[row][col] - centers[col]);
            }
        }

        valarray<double> l2distances(matrix.size());
        for (size_t row = 0; row < l2distances.size(); ++row) {
            l2distances[row] = eucledean_norm(distances[row]);
        }
        return l2distances;
    }

public:
    vector<ScorePackage<MachineMetadata>>
    get_scores(const vector<MachineMetadata> &observation_list) {
        vector<ScorePackage<MachineMetadata>> score_package_list;

        valarray<valarray<double>> matrix(valarray<double>(0.0, 2),
                                          observation_list.size());

        for (size_t i = 0; i < observation_list.size(); ++i) {
            const auto &metadata  = observation_list[i];
            matrix[i][cpu_idx]    = metadata.cpu_usage;
            matrix[i][memory_idx] = metadata.memory_usage;
        }

        const auto l2distances = calculate_distance(matrix);
        for (size_t i = 0; i < observation_list.size(); ++i) {
            const auto &                  metadata = observation_list[i];
            ScorePackage<MachineMetadata> package {
                metadata.machine_ip, 1.0 + l2distances[i], metadata};
            score_package_list.push_back(move(package));
        }

        return score_package_list;
    }
};

template<typename Scorer>
class ObserverScorerFunctor {
    Scorer                  scorer;
    vector<MachineMetadata> observation_list;
    unsigned long           last_measurement_timestamp = 0;
    unsigned long           last_execution_timestamp   = 0;

public:
    void operator()(const SourceTuple &              tuple,
                    Shipper<ObservationResultTuple> &shipper) {
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[OBSERVER SCORER] Received tuple with observation "
                    "timestamp: "
                 << tuple.observation.timestamp << '\n';
        }
#endif
        if (tuple.observation.timestamp > last_measurement_timestamp) {
            if (!observation_list.empty()) {
                const auto score_package_list =
                    scorer.get_scores(observation_list);
                for (const auto &package : score_package_list) {
#ifndef NDEBUG
                    {
                        lock_guard lock {print_mutex};
                        clog << "[OBSERVER SCORER] Sending tuple with id: "
                             << package.id << ", score: " << package.score
                             << '\n';
                    }
#endif
                    shipper.push({package.id, package.score,
                                  last_measurement_timestamp,
                                  last_execution_timestamp, package.data});
                }
                observation_list.clear();
            }
            last_measurement_timestamp = tuple.observation.timestamp;
        }

        if (observation_list.empty()) {
            last_execution_timestamp = tuple.execution_timestamp;
        }
        observation_list.push_back(tuple.observation);
    }
};

class SlidingWindowStreamAnomalyScoreFunctor {
    unordered_map<string, deque<double>> sliding_window_map;
    size_t                               window_length;
    unsigned long                        previous_timestamp = 0;

public:
    SlidingWindowStreamAnomalyScoreFunctor(size_t length = 10)
        : window_length {length} {}

    AnomalyResultTuple operator()(const ObservationResultTuple &tuple) {
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[SLIDING WINDOW STREAM ANOMALY SCORER] Received tuple "
                    "with observation ID: "
                 << tuple.id << '\n';
        }
#endif
        auto &sliding_window = sliding_window_map[tuple.id];
        sliding_window.push_back(tuple.score);
        if (sliding_window.size() > window_length) {
            sliding_window.pop_front();
        }

        double score_sum = 0;
        for (const double score : sliding_window) {
            score_sum += score;
        }
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[SLIDING WINDOW STREAM ANOMALY SCORER] Sending out tuple "
                    "with observation: ("
                 << tuple.observation << "), ID: " << tuple.id
                 << ", score sum: " << score_sum
                 << ", individual score: " << tuple.score << '\n';
        }
#endif
        return {tuple.id,
                score_sum,
                tuple.observation_timestamp,
                tuple.execution_timestamp,
                tuple.observation,
                tuple.score};
    }
};

template<typename T>
struct TupleWrapper {
    T      tuple;
    double score;

    TupleWrapper(const T &tuple, double score)
        : tuple {tuple}, score {score} {}

    int compare_to(const TupleWrapper &other) const {
        if (score == other.score) {
            return 0;
        } else if (score > other.score) {
            return 1;
        } else {
            return -1;
        }
    }
};

template<typename T>
static inline void tuple_swap(vector<TupleWrapper<T>> &tuple_wrapper_list,
                              size_t left, size_t right) {
    assert(left <= right);
    assert(left < tuple_wrapper_list.size());
    assert(right < tuple_wrapper_list.size());

    if (left != right) {
        const auto tmp            = move(tuple_wrapper_list[left]);
        tuple_wrapper_list[left]  = move(tuple_wrapper_list[right]);
        tuple_wrapper_list[right] = move(tmp);
    }
}

template<typename T>
static inline size_t
partition_single_side(vector<TupleWrapper<T>> &tuple_wrapper_list, size_t left,
                      size_t right) {
    assert(!tuple_wrapper_list.empty());
    assert(left < right);
    assert(left < tuple_wrapper_list.size());
    assert(right < tuple_wrapper_list.size());

    const auto &pivot = tuple_wrapper_list[right];
    auto        bar   = left;

    for (auto i = left; i < right; ++i) {
        if (tuple_wrapper_list[i].compare_to(pivot) < 0) {
            tuple_swap(tuple_wrapper_list, bar, i);
            ++bar;
        }
    }
    tuple_swap(tuple_wrapper_list, bar, right);
    return bar;
}

template<typename T>
static inline TupleWrapper<T>
bfprt_wrapper(vector<TupleWrapper<T>> &tuple_wrapper_list, size_t i,
              size_t left, size_t right) {
    assert(!tuple_wrapper_list.empty());
    assert(left <= right);
    assert(left <= i);
    assert(i <= right);
    assert(right < tuple_wrapper_list.size());

    if (left == right) {
        return tuple_wrapper_list[right];
    }

    const auto p = partition_single_side(tuple_wrapper_list, left, right);

    if (p == i) {
        return tuple_wrapper_list[p];
    } else if (p < i) {
        return bfprt_wrapper(tuple_wrapper_list, i, p + 1, right);
    } else {
        assert(p >= 1);
        return bfprt_wrapper(tuple_wrapper_list, i, left, p - 1);
    }
}

static inline AnomalyResultTuple bfprt(vector<AnomalyResultTuple> &tuple_list,
                                       size_t                      i) {
    assert(!tuple_list.empty());
    assert(i < tuple_list.size());

    vector<TupleWrapper<AnomalyResultTuple>> tuple_wrapper_list;
    for (const auto &tuple : tuple_list) {
        tuple_wrapper_list.emplace_back(tuple, tuple.individual_score);
    }

    const auto median_tuple =
        bfprt_wrapper(tuple_wrapper_list, i, 0, tuple_wrapper_list.size() - 1)
            .tuple;
    tuple_list.clear();

    for (const auto &wrapper : tuple_wrapper_list) {
        tuple_list.push_back(wrapper.tuple);
    }

    return median_tuple;
}

static inline vector<AnomalyResultTuple>
identify_abnormal_streams(vector<AnomalyResultTuple> &stream_list) {
    const size_t median_idx {stream_list.size() / 2};
    bfprt(stream_list, median_idx);
    const auto abnormal_stream_list = stream_list;
    return abnormal_stream_list; // XXX: Is this correct?  Why the copy?
}

class AlertTriggererFunctor {
    inline static const double dupper = sqrt(2);

    unsigned long              previous_observation_timestamp = 0;
    vector<AnomalyResultTuple> stream_list;
    double min_data_instance_score = numeric_limits<double>::max();
    double max_data_instance_score = 0.0;

public:
    void operator()(const AnomalyResultTuple &          input,
                    Shipper<AlertTriggererResultTuple> &shipper) {
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[ALERT TRIGGERER] Received input with observation "
                    "timestamp: "
                 << input.observation_timestamp << '\n';
        }
#endif
        if (input.observation_timestamp > previous_observation_timestamp) {
            if (!stream_list.empty()) {
                const auto abnormal_streams =
                    identify_abnormal_streams(stream_list);
                const size_t median_idx = stream_list.size() / 2;
                const double min_score  = abnormal_streams[0].anomaly_score;

                assert(median_idx < abnormal_streams.size());
                const auto median_score =
                    abnormal_streams[median_idx].anomaly_score;
#ifndef NDEBUG
                {
                    lock_guard lock {print_mutex};
                    clog << "[ALERT TRIGGERER] Median index: " << median_idx
                         << ", minimum score: " << min_score
                         << ", median score: " << median_score << '\n';
                }
#endif
                for (size_t i = 0; i < abnormal_streams.size(); ++i) {
                    const auto &stream_profile = abnormal_streams[i];
                    const auto  stream_score   = stream_profile.anomaly_score;
                    const auto  cur_data_inst_score =
                        stream_profile.anomaly_score;
                    const auto is_abnormal =
                        stream_score > 2 * median_score - min_score
                        && stream_score > min_score + 2 * dupper
                        && cur_data_inst_score > 0.1 + min_data_instance_score;

                    if (is_abnormal) {
#ifndef NDEBUG
                        {
                            lock_guard lock {print_mutex};
                            clog << "[ALERT TRIGGERER] Sending out tuple with "
                                    "stream ID: "
                                 << stream_profile.id
                                 << ", stream score: " << stream_score
                                 << ", stream profile timestamp: "
                                 << stream_profile.observation_timestamp
                                 << ", is_abnormal: "
                                 << (is_abnormal ? "true" : "false")
                                 << ", with observation (" << input.observation
                                 << ")\n";
                        }
#endif
                        shipper.push({stream_profile.id, stream_score,
                                      stream_profile.observation_timestamp,
                                      input.execution_timestamp, is_abnormal,
                                      input.observation});
                    }
                }
                stream_list.clear();
                min_data_instance_score = numeric_limits<double>::max();
                max_data_instance_score = 0.0;
            }
            previous_observation_timestamp = input.observation_timestamp;
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[ALERT TRIGGERER] Previous timestamp is now: "
                     << previous_observation_timestamp << '\n';
            }
#endif
        }

        if (input.individual_score > max_data_instance_score) {
            max_data_instance_score = input.individual_score;
        }
        if (input.individual_score < min_data_instance_score) {
            min_data_instance_score = input.individual_score;
        }
        stream_list.push_back(input);
    }
};

class SinkFunctor {
    vector<unsigned long> latency_samples;
    unsigned long         tuples_received    = 0;
    unsigned long         last_sampling_time = current_time();
    unsigned long         last_arrival_time  = last_sampling_time;
    unsigned              sampling_rate;

    bool is_time_to_sample(unsigned long arrival_time) {
        if (sampling_rate == 0) {
            return true;
        }
        const auto time_since_last_sampling =
            difference(arrival_time, last_sampling_time);
        const auto time_between_samples =
            (1.0 / sampling_rate) * timeunit_scale_factor;
        return time_since_last_sampling >= time_between_samples;
    }

public:
    SinkFunctor(unsigned rate) : sampling_rate {rate} {}

    void operator()(optional<AlertTriggererResultTuple> &input,
                    RuntimeContext &                     context) {
        if (input) {
            const auto arrival_time = current_time();
            const auto latency =
                difference(arrival_time, input->execution_timestamp);

            ++tuples_received;
            last_arrival_time = arrival_time;
            if (is_time_to_sample(arrival_time)) {
                latency_samples.push_back(latency);
                last_sampling_time = arrival_time;
            }
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[SINK] id: " << input->id << " "
                     << "anomaly score: " << input->anomaly_score
                     << " is_abnormal: " << input->is_abnormal
                     << " arrival time: " << arrival_time
                     << " observation ts: " << input->observation_timestamp
                     << " execution ts: " << input->execution_timestamp
                     << " latency: " << latency << ' ' << timeunit_string
                     << "s\n";
            }
#endif
        } else {
            global_received_tuples.fetch_add(tuples_received);
            global_latency_metric.merge(latency_samples);
        }
    }
};

static inline PipeGraph &build_graph(const Parameters &parameters,
                                     PipeGraph &       graph) {
    SourceFunctor source_functor {parameters.duration, parameters.tuple_rate};
    auto          source = Source_Builder {source_functor}
                      .withParallelism(parameters.parallelism[source_id])
                      .withName("source")
                      .withOutputBatchSize(parameters.batch_size[source_id])
                      .build();

    ObserverScorerFunctor<MachineMetadataScorer> observer_functor;
    auto                                         observer_scorer_node =
        FlatMap_Builder {observer_functor}
            .withParallelism(parameters.parallelism[observer_id])
            .withName("observation scorer")
            .withOutputBatchSize(parameters.batch_size[observer_id])
            .build();

    SlidingWindowStreamAnomalyScoreFunctor anomaly_scorer_functor;
    auto                                   anomaly_scorer_node =
        Map_Builder {anomaly_scorer_functor}
            .withParallelism(parameters.parallelism[anomaly_scorer_id])
            .withName("anomaly scorer")
            .withKeyBy([](const ObservationResultTuple &tuple) -> string {
                return tuple.id;
            })
            .withOutputBatchSize(parameters.batch_size[anomaly_scorer_id])
            .build();

    AlertTriggererFunctor alert_triggerer_functor;
    auto                  alert_triggerer_node =
        FlatMap_Builder {alert_triggerer_functor}
            .withParallelism(parameters.parallelism[alert_triggerer_id])
            .withName("alert triggerer")
            .withOutputBatchSize(parameters.batch_size[alert_triggerer_id])
            .build();

    SinkFunctor sink_functor {parameters.sampling_rate};
    auto        sink = Sink_Builder {sink_functor}
                    .withParallelism(parameters.parallelism[sink_id])
                    .withName("sink")
                    .build();

    if (parameters.use_chaining) {
        graph.add_source(source)
            .chain(observer_scorer_node)
            .chain(anomaly_scorer_node)
            .chain(alert_triggerer_node)
            .chain_sink(sink);
    } else {
        graph.add_source(source)
            .add(observer_scorer_node)
            .add(anomaly_scorer_node)
            .add(alert_triggerer_node)
            .add_sink(sink);
    }
    return graph;
}

int main(int argc, char *argv[]) {
    Parameters parameters;
    parse_args(argc, argv, parameters);
    validate_args(parameters);
    print_initial_parameters(parameters);

    PipeGraph graph {"mo-machine-outlier", parameters.execution_mode,
                     parameters.time_policy};
    build_graph(parameters, graph);

    const auto start_time = current_time();
    graph.run();
    const auto   elapsed_time = difference(current_time(), start_time);
    const double throughput =
        elapsed_time > 0
            ? (global_sent_tuples.load() / static_cast<double>(elapsed_time))
            : global_sent_tuples.load();
    const double service_time = 1 / throughput;

    const auto latency_stats = get_distribution_stats(
        global_latency_metric, parameters, global_received_tuples);
    serialize_json(latency_stats, "mo-latency",
                   parameters.metric_output_directory);

    const auto throughput_stats = get_single_value_stats(
        throughput, "throughput", parameters, global_sent_tuples.load());
    serialize_json(throughput_stats, "mo-throughput",
                   parameters.metric_output_directory);

    const auto service_time_stats = get_single_value_stats(
        service_time, "service time", parameters, global_sent_tuples.load());
    serialize_json(service_time_stats, "mo-service-time",
                   parameters.metric_output_directory);

    const auto average_latency =
        accumulate(global_latency_metric.begin(), global_latency_metric.end(),
                   0.0)
        / (!global_latency_metric.empty() ? global_latency_metric.size()
                                          : 1.0);
    print_statistics(elapsed_time, parameters.duration, global_sent_tuples,
                     average_latency, global_received_tuples);
    return 0;
}
