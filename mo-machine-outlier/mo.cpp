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

struct Parameters {
    unsigned source_parallelism          = 1;
    unsigned observer_parallelism        = 1;
    unsigned anomaly_scorer_parallelism  = 1;
    unsigned alert_triggerer_parallelism = 1;
    unsigned sink_parallelism            = 1;
    unsigned batch_size                  = 0;
    unsigned duration                    = 60;
    unsigned tuple_rate                  = 1000;
    unsigned sampling_rate               = 100;
    bool     use_chaining                = false;
};

struct MachineMetadata {
    string        machine_ip;
    double        cpu_usage;
    double        memory_usage;
    double        score;
    unsigned long measurement_timestamp;
};

#ifndef NDEBUG
ostream &operator<<(ostream &stream, const MachineMetadata &metadata) {
    stream << "Machine ip: " << metadata.machine_ip
           << ", CPU usage: " << metadata.cpu_usage
           << ", memory usage: " << metadata.memory_usage
           << ", observation timestamp: " << metadata.measurement_timestamp;
    return stream;
}
#endif

template<typename T>
struct ScorePackage {
    string id;
    double score;
    T      data;
};

struct TupleMetadata {
    unsigned long id;
    unsigned long timestamp;
};

struct SourceTuple {
    TupleMetadata   metadata;
    MachineMetadata observation;
};

struct ObservationResultTuple {
    TupleMetadata   metadata;
    string          id;
    double          score;
    unsigned long   timestamp;
    MachineMetadata observation;
};

struct AnomalyResultTuple {
    TupleMetadata   metadata;
    string          id;
    double          anomaly_score;
    unsigned long   timestamp;
    MachineMetadata observation;
    double          individual_score;
};

struct AlertTriggererResultTuple {
    TupleMetadata   metadata;
    string          id;
    double          anomaly_score;
    unsigned long   timestamp;
    bool            is_abnormal;
    MachineMetadata observation; // XXX This field may not be correct!
};

template<typename T>
class Metric {
    vector<T> sorted_samples;
    string    metric_name;
    mutex     metric_mutex;

public:
    Metric(const char *name = "name") : metric_name {name} {}

    Metric &merge(const vector<T> &new_samples) {
        lock_guard guard {metric_mutex};
        sorted_samples.insert(sorted_samples.begin(), new_samples.begin(),
                              new_samples.end());
        sort(sorted_samples.begin(), sorted_samples.end());
        return *this;
    }

    size_t size() const {
        return sorted_samples.size();
    }

    size_t length() const {
        return sorted_samples.length();
    }

    bool empty() const {
        return sorted_samples.empty();
    }

    typename vector<T>::const_iterator begin() const {
        return sorted_samples.begin();
    }

    typename vector<T>::const_iterator end() const {
        return sorted_samples.end();
    }

    const char *name() const {
        return metric_name.c_str();
    }
};

static constexpr auto current_time = current_time_nsecs;

static const auto timeunit_string =
    current_time == current_time_usecs   ? "microsecond"
    : current_time == current_time_nsecs ? "nanosecond"
                                         : "time unit";

static const unsigned long timeunit_scale_factor =
    current_time == current_time_usecs   ? 1000000
    : current_time == current_time_nsecs ? 1000000000
                                         : 1;

static const struct option long_opts[] = {
    {"help", 0, 0, 'h'},        {"rate", 1, 0, 'r'},  {"sampling", 1, 0, 's'},
    {"parallelism", 1, 0, 'p'}, {"batch", 1, 0, 'b'}, {"chaining", 1, 0, 'c'},
    {"duration", 1, 0, 'd'},    {0, 0, 0, 0}};

/*
 * Return difference between a and b, accounting for unsigned arithmetic
 * wraparound.
 */
static inline unsigned long difference(unsigned long a, unsigned long b) {
    return max(a, b) - min(a, b);
}

/*
 * Return a std::vector of std::string_views, obtained from splitting the
 * original string_view. by the delim character.
 */
static inline vector<string_view> string_split(const string_view &s,
                                               char               delim) {
    const auto          is_delim   = [=](char c) { return c == delim; };
    auto                word_begin = find_if_not(s.begin(), s.end(), is_delim);
    vector<string_view> words;

    while (word_begin < s.end()) {
        const auto word_end = find_if(word_begin + 1, s.end(), is_delim);
        words.emplace_back(word_begin, word_end - word_begin);
        word_begin = find_if_not(word_end, s.end(), is_delim);
    }
    return words;
}

static inline vector<size_t> get_parallelism_degrees(const char *degrees) {
    vector<size_t> parallelism_degrees;
    for (const auto &s : string_split(degrees, ',')) {
        parallelism_degrees.push_back(atoi(s.data()));
    }
    return parallelism_degrees;
}

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

    metadata.machine_ip            = values[machine_id_index];
    metadata.measurement_timestamp = stoul(values[timestamp_index].data());
    metadata.cpu_usage             = stod(values[cpu_usage_index].data()) * 10;
    metadata.memory_usage = stod(values[memory_usage_index].data()) * 10;
    return metadata;
}

static inline optional<MachineMetadata>
parse_alibaba_trace(const string &trace) {
    const auto      values             = string_split(trace, ',');
    const auto      timestamp_index    = 1;
    const auto      machine_id_index   = 0;
    const auto      cpu_usage_index    = 2;
    const auto      memory_usage_index = 3;
    MachineMetadata metadata;

    if (values.size() != 7) {
        return {};
    }

    metadata.machine_ip = values[machine_id_index];
    metadata.measurement_timestamp =
        stoul(values[timestamp_index].data()) * 1000;
    metadata.cpu_usage    = stod(values[cpu_usage_index].data());
    metadata.memory_usage = stod(values[memory_usage_index].data());
    return metadata;
}

static inline double eucledean_norm(const valarray<double> &elements) {
    double result {0.0};
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

    while (
        (option = getopt_long(argc, argv, "r:s:p:b:c:d:h", long_opts, &index))
        != -1) {
        switch (option) {
        case 'r':
            parameters.tuple_rate = atoi(optarg);
            break;
        case 's':
            parameters.sampling_rate = atoi(optarg);
            break;
        case 'b':
            parameters.batch_size = atoi(optarg);
            break;
        case 'p': {
            const auto degrees = get_parallelism_degrees(optarg);
            if (degrees.size() != 5) {
                cerr << "Error in parsing the input arguments.  Parallelism "
                        "degree string requires exactly 5 elements.\n";
                exit(EXIT_FAILURE);
            } else {
                parameters.source_parallelism          = degrees[0];
                parameters.observer_parallelism        = degrees[1];
                parameters.anomaly_scorer_parallelism  = degrees[2];
                parameters.alert_triggerer_parallelism = degrees[3];
                parameters.sink_parallelism            = degrees[4];
            }
        } break;
        case 'c':
            parameters.use_chaining = atoi(optarg) > 0 ? true : false;
            break;
        case 'd':
            parameters.duration = atoi(optarg);
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

    const auto max_threads = thread::hardware_concurrency();

    if (parameters.source_parallelism == 0
        || parameters.observer_parallelism == 0
        || parameters.anomaly_scorer_parallelism == 0
        || parameters.alert_triggerer_parallelism == 0
        || parameters.sink_parallelism == 0) {
        cerr << "Error: parallelism degree must be positive\n";
        exit(EXIT_FAILURE);
    }

    if (parameters.source_parallelism > max_threads) {
        cerr << "Error: source parallelism degree is too large\n"
                "Maximum available number of threads is: "
             << max_threads << '\n';
        exit(EXIT_FAILURE);
    }

    if (parameters.observer_parallelism > max_threads) {
        cerr << "Error: observer parallelism degree is too large\n"
                "Maximum available number of threads is: "
             << max_threads << '\n';
        exit(EXIT_FAILURE);
    }

    if (parameters.anomaly_scorer_parallelism > max_threads) {
        cerr << "Error: anomaly scorer parallelism degree is too large\n"
                "Maximum available number of threads is: "
             << max_threads << '\n';
        exit(EXIT_FAILURE);
    }

    if (parameters.alert_triggerer_parallelism > max_threads) {
        cerr << "Error: alert triggerer parallelism degree is too large\n"
                "Maximum available number of threads is: "
             << max_threads << '\n';
        exit(EXIT_FAILURE);
    }

    if (parameters.sink_parallelism > max_threads) {
        cerr << "Error: sink parallelism degree is too large\n"
                "Maximum available number of threads is: "
             << max_threads << '\n';
        exit(EXIT_FAILURE);
    }

    if (parameters.source_parallelism + parameters.observer_parallelism
                + parameters.anomaly_scorer_parallelism
                + parameters.alert_triggerer_parallelism
                + parameters.sink_parallelism
            >= max_threads
        && !parameters.use_chaining) {
        cerr << "Error: the total number of hardware threads specified is too "
                "high to be used without chaining.\n"
                "Maximum available number of threads is: "
             << max_threads << '\n';
        exit(EXIT_FAILURE);
    }
}

static inline void print_initial_parameters(const Parameters &parameters) {
    cout << "Running graph with the following parameters:\n"
         << "Source parallelism: " << parameters.source_parallelism << '\n'
         << "Observer parallelism: " << parameters.observer_parallelism << '\n'
         << "Anomaly scorer parallelism: "
         << parameters.anomaly_scorer_parallelism << '\n'
         << "Alert triggerer parallelism: "
         << parameters.alert_triggerer_parallelism << '\n'
         << "Sink parallelism: " << parameters.sink_parallelism << '\n'
         << "Batching: ";
    if (parameters.batch_size > 0) {
        cout << parameters.batch_size << '\n';
    } else {
        cout << "None\n";
    }

    cout << "Duration: " << parameters.duration << " second"
         << (parameters.duration == 1 ? "" : "s") << '\n'
         << "Tuple generation rate: ";
    if (parameters.tuple_rate > 0) {
        cout << parameters.tuple_rate << " tuple"
             << (parameters.tuple_rate == 1 ? "" : "s") << " per second\n";
    } else {
        cout << "unlimited (BEWARE OF QUEUE CONGESTION)\n";
    }

    cout << "Sampling rate: ";
    if (parameters.sampling_rate > 0) {
        cout << parameters.sampling_rate << " measurement"
             << (parameters.sampling_rate == 1 ? "" : "s") << " per second\n";
    } else {
        cout << "unlimited (sample every incoming tuple)\n";
    }

    cout << "Chaining: " << (parameters.use_chaining ? "enabled" : "disabled")
         << '\n';
}

static inline void print_statistics(unsigned long elapsed_time,
                                    unsigned long duration,
                                    unsigned long sent_tuples,
                                    double        average_latency,
                                    unsigned long received_tuples) {
    const auto elapsed_time_in_seconds =
        elapsed_time / static_cast<double>(timeunit_scale_factor);

    const auto throughput =
        elapsed_time > 0 ? sent_tuples / static_cast<double>(elapsed_time)
                         : sent_tuples;

    const auto throughput_in_seconds   = throughput * timeunit_scale_factor;
    const auto service_time            = 1 / throughput;
    const auto service_time_in_seconds = service_time / timeunit_scale_factor;
    const auto latency_in_seconds = average_latency / timeunit_scale_factor;

    cout << "Elapsed time: " << elapsed_time << ' ' << timeunit_string << "s ("
         << elapsed_time_in_seconds << " seconds)\n"
         << "Excess time after source stopped: "
         << elapsed_time - duration * timeunit_scale_factor << ' '
         << timeunit_string << "s\n"
         << "Total number of tuples sent: " << sent_tuples << '\n'
         << "Total number of tuples recieved: " << received_tuples << '\n'
         << "Processed about " << throughput << " tuples per "
         << timeunit_string << " (" << throughput_in_seconds
         << " tuples per second)\n"
         << "Service time: " << service_time << ' ' << timeunit_string << "s ("
         << service_time_in_seconds << " seconds)\n"
         << "Average latency: " << average_latency << ' ' << timeunit_string
         << "s (" << latency_in_seconds << " seconds)\n";
}

static inline string get_datetime_string() {
    const auto current_date = time(nullptr);
    string     date_string  = asctime(localtime(&current_date));
    if (!date_string.empty()) {
        date_string.pop_back(); // needed to remove trailing newline
    }
    return date_string;
}

static inline void serialize_to_json(const Metric<unsigned long> &metric,
                                     unsigned long total_measurements) {
    nlohmann::ordered_json json_stats;
    json_stats["date"]                 = get_datetime_string();
    json_stats["name"]                 = metric.name();
    json_stats["time unit"]            = string {timeunit_string} + 's';
    json_stats["sampled measurements"] = metric.size();
    json_stats["total measurements"]   = total_measurements;

    if (!metric.empty()) {
        const auto mean =
            accumulate(metric.begin(), metric.end(), 0.0) / metric.size();
        json_stats["mean"] = mean;

        for (const auto percentile : {0.0, 0.05, 0.25, 0.5, 0.75, 0.95, 1.0}) {
            const auto percentile_value_position =
                metric.begin() + (metric.size() - 1) * percentile;
            const auto label = to_string(static_cast<int>(percentile * 100))
                               + "th percentile ";
            json_stats[label] = *percentile_value_position;
        }
    } else {
        json_stats["mean"] = 0;
        for (const auto percentile : {"0", "25", "50", "75", "95", "100"}) {
            const auto label  = string {percentile} + "th percentile";
            json_stats[label] = 0;
        }
    }
    ofstream fs {string {"metric-"} + metric.name() + ".json"};
    fs << json_stats.dump(4) << '\n';
}

/*
 * Suspend execution for an amount of time units specified by duration.
 */
static inline void busy_wait(unsigned long duration) {
    const auto start_time = current_time();
    auto       now        = start_time;
    while (now - start_time < duration) {
        now = current_time();
    }
}

/* Global variables */
static atomic_ulong          global_sent_tuples {0};
static atomic_ulong          global_received_tuples {0};
static Metric<unsigned long> global_latency_metric {"latency"};
static Metric<unsigned long> global_interdeparture_metric {
    "interdeparture-time"};
static Metric<unsigned long> global_service_time_metric {"service-time"};
#ifndef NDEBUG
static mutex print_mutex;
#endif

class SourceFunctor {
    static constexpr auto   default_path = "machine-usage.csv";
    vector<MachineMetadata> machine_metadata;
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
            const auto current_observation = machine_metadata[index];
            const auto timestamp           = current_time();
#ifndef NDEBUG
            {
                unique_lock lock {print_mutex};
                clog << "[SOURCE] Sending out tuple with the following "
                        "observation: "
                     << current_observation << '\n';
            }
#endif
            const TupleMetadata tuple_metadata {
                timestamp, timestamp}; // Using timestamp as ID

            shipper.push({tuple_metadata, current_observation});
            ++sent_tuples;
            index = (index + 1) % machine_metadata.size();

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
    Scorer                  scorer {};
    vector<MachineMetadata> observation_list {};
    TupleMetadata           parent_tuple_metadata;
    unsigned long           last_measurement_timestamp {0};

public:
    void operator()(const SourceTuple &              tuple,
                    Shipper<ObservationResultTuple> &shipper) {
        const auto current_measurement_timestamp =
            tuple.observation.measurement_timestamp;
#ifndef NDEBUG
        {
            unique_lock lock {print_mutex};
            clog << "[OBSERVER SCORER] Received tuple with observation "
                    "timestamp: "
                 << current_measurement_timestamp << '\n';
        }
#endif
        if (current_measurement_timestamp > last_measurement_timestamp) {
            if (!observation_list.empty()) {
                const auto score_package_list =
                    scorer.get_scores(observation_list);
                for (const auto &package : score_package_list) {
#ifndef NDEBUG
                    {
                        unique_lock lock {print_mutex};
                        clog << "[OBSERVER SCORER] Sending tuple with id: "
                             << package.id << ", score: " << package.score
                             << '\n';
                    }
#endif
                    shipper.push({parent_tuple_metadata, package.id,
                                  package.score, last_measurement_timestamp,
                                  package.data});
                }
                observation_list.clear();
            }
            last_measurement_timestamp = current_measurement_timestamp;
        }

        if (observation_list.empty()) {
            parent_tuple_metadata = tuple.metadata;
        }
        observation_list.push_back(tuple.observation);
    }
};

// class DataStreamAnomalyScorerFunctor {
//     template<typename T>
//     struct StreamProfile {
//         string id;
//         T      current_data_instance;
//         double stream_anomaly_score;
//         double current_data_instance_score;
//     };

//     unordered_map<string, StreamProfile<MachineMetadata>> stream_profiles;

//     double        lambda {0.017};
//     double        factor {2.71 - lambda};
//     double        threshold {1 / (1 - factor) * 0.5};
//     unsigned long last_measurement_timestamp {0};
//     bool          shrink_next_round {false};

// public:
//     void operator()(const ObservationResultTuple &      tuple,
//                     Shipper<AnomalyResultTuple<Tuple>> &shipper) {
//         const auto current_measurement_timestamp = tuple.timestamp;

//         if (current_measurement_timestamp > last_measurement_timestamp) {
//             for (auto &entry : stream_profiles) {
//                 auto &profile = entry.second;

//                 if (shrink_next_round) {
//                     profile.stream_anomaly_score = 0.0;
//                 }

//                 AnomalyResultTuple<Tuple> output {
//                     entry.first, profile.stream_anomaly_score,
//                     profile.current_data_instance_score,
//                     current_measurement_timestamp,
//                     profile.current_data_instance};
//                 shipper.push(move(output));
//             }

//             if (shrink_next_round) {
//                 shrink_next_round = false;
//             }
//             last_measurement_timestamp = current_measurement_timestamp;
//         }

//         const auto id                          = tuple.id;
//         const auto data_instance_anomaly_score = tuple.score;

//         if (stream_profiles.find(id) != stream_profiles.end()) {
//             stream_profiles[id] = {id, tuple.parent_tuple.metadata,
//                                    data_instance_anomaly_score,
//                                    tuple.score};

//         } else {
//             auto &profile = stream_profiles[id];
//             profile.stream_anomaly_score =
//                 profile.stream_anomaly_score * factor
//                 + data_instance_anomaly_score;
//             profile.current_data_instance       =
//             tuple.parent_tuple.metadata; profile.current_data_instance_score
//             = data_instance_anomaly_score;

//             if (profile.stream_anomaly_score > threshold) {
//                 shrink_next_round = true;
//             }
//         }
//     }
// };

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
            unique_lock lock {print_mutex};
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
            unique_lock lock {print_mutex};
            clog << "[SLIDING WINDOW STREAM ANOMALY SCORER] Sending out tuple "
                    "with observation: ("
                 << tuple.observation << "), ID: " << tuple.id
                 << ", score sum: " << score_sum
                 << ", individual score: " << tuple.score << '\n';
        }
#endif
        return {tuple.metadata,  tuple.id,          score_sum,
                tuple.timestamp, tuple.observation, tuple.score};
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

    unsigned long              previous_timestamp = 0;
    vector<AnomalyResultTuple> stream_list;
    double min_data_instance_score = numeric_limits<double>::max();
    double max_data_instance_score = 0.0;

public:
    void operator()(const AnomalyResultTuple &          input,
                    Shipper<AlertTriggererResultTuple> &shipper) {
        const auto timestamp = input.timestamp;

        if (timestamp > previous_timestamp) {
            if (!stream_list.empty()) {
                const auto abnormal_streams =
                    identify_abnormal_streams(stream_list);
                const size_t median_idx {stream_list.size() / 2};
                const auto   min_score = abnormal_streams[0].anomaly_score;

                assert(median_idx < abnormal_streams.size());
                const auto median_score =
                    abnormal_streams[median_idx].anomaly_score;

#ifndef NDEBUG
                {
                    unique_lock lock {print_mutex};
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
                            unique_lock lock {print_mutex};
                            clog << "[ALERT TRIGGERER] Sending out tuple with "
                                    "stream ID: "
                                 << stream_profile.id
                                 << ", stream score: " << stream_score
                                 << ", stream profile timestamp: "
                                 << stream_profile.timestamp
                                 << ", is_abnormal: "
                                 << (is_abnormal ? "true" : "false")
                                 << ", with observation (" << input.observation
                                 << ")\n";
                        }
#endif
                        shipper.push({input.metadata, stream_profile.id,
                                      stream_score, stream_profile.timestamp,
                                      is_abnormal, input.observation});
                    }
                }
                stream_list.clear();
                min_data_instance_score = numeric_limits<double>::max();
                max_data_instance_score = 0.0;
            }
            previous_timestamp = timestamp;
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
    vector<unsigned long> interdeparture_samples;
    vector<unsigned long> service_time_samples;
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
            const auto latency = difference(arrival_time, input->timestamp);
            const auto interdeparture_time =
                difference(arrival_time, last_arrival_time);

            ++tuples_received;
            last_arrival_time = arrival_time;

            if (is_time_to_sample(arrival_time)) {
                latency_samples.push_back(latency);
                interdeparture_samples.push_back(interdeparture_time);

                // The current service time is computed via this heuristic,
                // it MIGHT not be reliable.
                const auto service_time =
                    interdeparture_time
                    / static_cast<double>(context.getParallelism());
                service_time_samples.push_back(service_time);
                last_sampling_time = arrival_time;
            }
#ifndef NDEBUG
            {
                unique_lock lock {print_mutex};
                clog << "[SINK] id: " << input->id << " "
                     << "anomaly score: " << input->anomaly_score
                     << " is_abnormal: " << input->is_abnormal
                     << " arrival time: " << arrival_time
                     << " ts:" << input->timestamp << " latency: " << latency
                     << '\n';
            }
#endif
        } else {
            global_received_tuples.fetch_add(tuples_received);
            global_latency_metric.merge(latency_samples);
            global_interdeparture_metric.merge(interdeparture_samples);
            global_service_time_metric.merge(service_time_samples);
        }
    }
};

static inline PipeGraph &build_graph(const Parameters &parameters,
                                     PipeGraph &       graph) {
    SourceFunctor source_functor {parameters.duration, parameters.tuple_rate};
    auto          source = Source_Builder {source_functor}
                      .withParallelism(parameters.source_parallelism)
                      .withName("source")
                      .withOutputBatchSize(parameters.batch_size)
                      .build();

    ObserverScorerFunctor<MachineMetadataScorer> observer_functor;
    auto                                         observer_scorer_node =
        FlatMap_Builder {observer_functor}
            .withParallelism(parameters.observer_parallelism)
            .withName("observation scorer")
            .withOutputBatchSize(parameters.batch_size)
            .build();

    SlidingWindowStreamAnomalyScoreFunctor anomaly_scorer_functor;
    auto                                   anomaly_scorer_node =
        Map_Builder {anomaly_scorer_functor}
            .withParallelism(parameters.anomaly_scorer_parallelism)
            .withName("anomaly scorer")
            .withKeyBy([](const ObservationResultTuple &tuple) -> string {
                return tuple.id;
            })
            .withOutputBatchSize(parameters.batch_size)
            .build();

    AlertTriggererFunctor alert_triggerer_functor;
    auto                  alert_triggerer_node =
        FlatMap_Builder {alert_triggerer_functor}
            .withParallelism(parameters.alert_triggerer_parallelism)
            .withName("alert triggerer")
            .withOutputBatchSize(parameters.batch_size)
            .build();

    SinkFunctor sink_functor {parameters.sampling_rate};
    auto        sink = Sink_Builder {sink_functor}
                    .withParallelism(parameters.sink_parallelism)
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

    PipeGraph graph {"mo-machine-outlier", Execution_Mode_t::DETERMINISTIC,
                     Time_Policy_t::INGRESS_TIME};
    build_graph(parameters, graph);

    const auto start_time = current_time();
    graph.run();
    const auto elapsed_time = difference(current_time(), start_time);

    serialize_to_json(global_latency_metric, global_received_tuples);
    serialize_to_json(global_interdeparture_metric, global_received_tuples);
    serialize_to_json(global_service_time_metric, global_received_tuples);

    const auto average_latency =
        accumulate(global_latency_metric.begin(), global_latency_metric.end(),
                   0.0)
        / (!global_latency_metric.empty() ? global_latency_metric.size()
                                          : 1.0);
    print_statistics(elapsed_time, parameters.duration, global_sent_tuples,
                     average_latency, global_received_tuples);
    return 0;
}
