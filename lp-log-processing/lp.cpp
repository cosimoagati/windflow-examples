#include <atomic>
#include <cassert>
#include <getopt.h>
#include <maxminddb.h>
#include <mutex>
#include <nlohmann/json.hpp>
#include <optional>
#include <regex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wreorder"
#pragma GCC diagnostic ignored "-Wextra"
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wsign-compare"
#if defined(__clang__) || !defined(__GNUC__)
#pragma GCC diagnostic ignored "-Wmismatched-tags"
#endif
#pragma GCC diagnostic ignored "-Woverloaded-virtual"
#endif

#include <wf/windflow.hpp>

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif

using namespace std;
using namespace wf;

struct Parameters {
    unsigned source_parallelism         = 1;
    unsigned volume_counter_parallelism = 1;
    unsigned status_counter_parallelism = 1;
    unsigned geo_finder_parallelism     = 1;
    unsigned geo_stats_sink_parallelism = 1;
    unsigned batch_size                 = 0;
    unsigned duration                   = 60;
    unsigned tuple_rate                 = 1000;
    unsigned sampling_rate              = 100;
    bool     use_chaining               = false;
};

struct SourceTuple {
    string        ip;
    string        request;
    string        log_timestamp;
    unsigned      response;
    unsigned      byte_size;
    unsigned long minute_timestamp;
    unsigned long timestamp;
};

struct GeoFinderOutputTuple {
    string        country;
    string        city;
    unsigned long timestamp;
};

struct OutputTuple {
    enum { Volume, Status, Geo } tag;
    string        country;
    string        city;
    unsigned      country_total;
    unsigned      city_total;
    unsigned      status_code;
    unsigned long minute;
    unsigned long count;
    unsigned long timestamp;
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

class MMDB_handle {
    MMDB_s mmdb;
    bool   is_db_valid = false;

public:
    MMDB_handle(const char *path) {
        const int status = MMDB_open(path, MMDB_MODE_MMAP, &mmdb);
        if (status != MMDB_SUCCESS) {
            cerr << "Error opening MaxMind database file\n";
            exit(EXIT_FAILURE);
        }
        is_db_valid = true;
    }

    const MMDB_s &db() const {
        return mmdb;
    }

    MMDB_handle(const MMDB_handle &other) = delete;
    MMDB_handle(MMDB_handle &&other)      = delete;
    MMDB_handle &operator=(const MMDB_handle &other) = delete;
    MMDB_handle &operator=(MMDB_handle &&other) = delete;

    ~MMDB_handle() {
        if (is_db_valid) {
            MMDB_close(&mmdb);
        }
    }
};

constexpr auto current_time = current_time_nsecs;

const auto timeunit_string = current_time == current_time_usecs ? "microsecond"
                             : current_time == current_time_nsecs
                                 ? "nanosecond"
                                 : "time unit";

const unsigned long timeunit_scale_factor =
    current_time == current_time_usecs   ? 1000000
    : current_time == current_time_nsecs ? 1000000000
                                         : 1;
const struct option long_opts[] = {
    {"help", 0, 0, 'h'},        {"rate", 1, 0, 'r'},  {"sampling", 1, 0, 's'},
    {"parallelism", 1, 0, 'p'}, {"batch", 1, 0, 'b'}, {"chaining", 1, 0, 'c'},
    {"duration", 1, 0, 'd'},    {0, 0, 0, 0}};

/*
 * Return difference between a and b, accounting for unsigned arithmetic
 * wraparound.
 */
unsigned long difference(unsigned long a, unsigned long b) {
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

vector<string> split_log_fields(const string &line) {
    const regex log_regex {
        "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] "
        "\"(.+?)\" (\\d{3}) (\\S+)(.*?)"};
    smatch         log_field_matches;
    vector<string> result;
    if (regex_match(line, log_field_matches, log_regex)) {
        for (const auto &m : log_field_matches) {
            result.push_back(m);
        }
    }
    return result;
}

optional<string> lookup_country(const MMDB_s *mmdb, const char *ip_string) {
    int  gai_error;
    int  mmdb_error;
    auto db_node =
        MMDB_lookup_string(mmdb, ip_string, &gai_error, &mmdb_error);
    if (gai_error != 0 || mmdb_error != MMDB_SUCCESS || !db_node.found_entry) {
        return {};
    }
    MMDB_entry_data_s entry_data;
    const int status = MMDB_get_value(&db_node.entry, &entry_data, "country",
                                      "names", "en", NULL);
    if (status != MMDB_SUCCESS || !entry_data.has_data
        || entry_data.type != MMDB_DATA_TYPE_UTF8_STRING) {
        return {};
    }
    string result;
    for (unsigned i = 0; i < entry_data.data_size; ++i) {
        result.push_back(entry_data.utf8_string[i]);
    }
    result.shrink_to_fit();
    return result;
}

optional<SourceTuple> build_source_tuple(const string &line) {
    const auto tokens = split_log_fields(line);
    if (tokens.size() != 9) {
        return {};
    }
    // TODO: Obtain minutes!
    SourceTuple tuple;
    tuple.ip            = tokens[1];
    tuple.log_timestamp = tokens[4];
    tuple.request       = tokens[5];
    tuple.response      = stoul(tokens[6]);
    tuple.byte_size     = tokens[7] == string {"-"} ? 0 : stoul(tokens[7]);
    return tuple;
}

static inline vector<size_t> get_parallelism_degrees(const char *degrees) {
    vector<size_t> parallelism_degrees;
    for (const auto &s : string_split(degrees, ',')) {
        parallelism_degrees.push_back(atoi(s.data()));
    }
    return parallelism_degrees;
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
            if (degrees.size() != 4) {
                cerr << "Error in parsing the input arguments.  Parallelism "
                        "degree string requires exactly five elements.\n";
                exit(EXIT_FAILURE);
            } else {
                parameters.source_parallelism         = degrees[0];
                parameters.volume_counter_parallelism = degrees[1];
                parameters.status_counter_parallelism = degrees[2];
                parameters.geo_finder_parallelism     = degrees[3];
                parameters.geo_stats_sink_parallelism = degrees[4];
            }
            break;
        }
        case 'c':
            parameters.use_chaining = atoi(optarg) > 0 ? true : false;
            break;
        case 'd':
            parameters.duration = atoi(optarg);
            break;
        case 'h':
            cout << "Parameters: --rate <value> --sampling "
                    "<value> --batch <size> --parallelism "
                    "<nEventSource,nRewardSource,nReinforcementLearner,nSink> "
                    "[--duration <seconds>] "
                    "[--chaining <value>]\n";
            exit(EXIT_SUCCESS);
        default:
            cerr << "Error in parsing the input arguments.  Use the --help "
                    "(-h) option for usage information.\n";
            exit(EXIT_FAILURE);
        }
    }
}

void validate_args(const Parameters &parameters) {
    if (parameters.duration == 0) {
        cerr << "Error: duration must be positive\n";
        exit(EXIT_FAILURE);
    }

    if (parameters.source_parallelism == 0
        || parameters.volume_counter_parallelism == 0
        || parameters.status_counter_parallelism == 0
        || parameters.geo_finder_parallelism == 0
        || parameters.geo_stats_sink_parallelism == 0) {
        cerr << "Error: parallelism degree must be positive\n";
        exit(EXIT_FAILURE);
    }

    const auto max_threads = thread::hardware_concurrency();

    if (parameters.source_parallelism > max_threads) {
        cerr << "Error: source parallelism degree is too large\n";
        exit(EXIT_FAILURE);
    }

    if (parameters.volume_counter_parallelism > max_threads) {
        cerr << "Error: volume counter parallelism degree is too large\n";
        exit(EXIT_FAILURE);
    }

    if (parameters.status_counter_parallelism > max_threads) {
        cerr << "Error: status counter parallelism parallelism degree is too "
                "large\n";
        exit(EXIT_FAILURE);
    }

    if (parameters.geo_finder_parallelism > max_threads) {
        cerr << "Error: sink parallelism degree is too large\n";
        exit(EXIT_FAILURE);
    }

    if (parameters.geo_stats_sink_parallelism > max_threads) {
        cerr << "Error: geo stats (sink) parallelism is too large\n";
        exit(EXIT_FAILURE);
    }

    if (parameters.source_parallelism + parameters.volume_counter_parallelism
                + parameters.status_counter_parallelism
                + parameters.geo_finder_parallelism
                + parameters.geo_stats_sink_parallelism
            >= max_threads
        && !parameters.use_chaining) {
        cerr << "Error: the total number of hardware threads specified is too "
                "high to be used without chaining.\n";
        exit(EXIT_FAILURE);
    }
}

void print_initial_parameters(const Parameters &parameters) {
    cout << "Running graph with the following parameters:\n"
         << "Source parallelism: " << parameters.source_parallelism << '\n'
         << "Volume counter parallelism: "
         << parameters.volume_counter_parallelism << '\n'
         << "Status counter parallelism: "
         << parameters.status_counter_parallelism << '\n'
         << "Geo finder parallelism: " << parameters.geo_finder_parallelism
         << '\n'
         << "Geo stats (sink) parallelism: "
         << parameters.geo_stats_sink_parallelism << '\n'
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
    string     date_string {asctime(localtime(&current_date))};
    if (!date_string.empty()) {
        date_string.pop_back(); // needed to remove trailing newline
    }
    return date_string;
}

void serialize_to_json(const Metric<unsigned long> &metric,
                       unsigned long                total_measurements) {
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
void busy_wait(unsigned long duration) {
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

// class SourceFunctor {
// public:
//     operator()(Source_Shipper<Tuple> &shipper) {}
// };

template<typename T>
class CircularFifoQueue {
    vector<T> buffer;
    size_t    head     = 0;
    size_t    tail     = 0;
    bool      is_empty = true;

public:
    CircularFifoQueue(size_t size) : buffer(size) {
        if (size == 0) {
            cerr << "Error initializing circular queue: size must be "
                    "positive\n";
            exit(EXIT_FAILURE);
        }
    }

    void add(const T &element) {
        buffer[head] = element;
        head         = (head + 1) % buffer.size();
        if (head == tail) {
            tail = (tail + 1) % buffer.size();
        }
        is_empty = true;
    }

    bool empty() const {
        return is_empty;
    }

    bool full() const {
        return false;
    }

    T remove() {
        assert(!is_empty);
        const auto element = move(buffer[tail]);
        tail               = (tail + 1) % buffer.size();
        if (tail == head) {
            is_empty = true;
        }
        return element;
    }
};

class VolumeCounterFunctor {
    // logger
    CircularFifoQueue<unsigned long>            buffer;
    unordered_map<unsigned long, unsigned long> counts;

public:
    VolumeCounterFunctor(size_t window_size = 60)
        : buffer {window_size}, counts {window_size} {}

    void operator()(const SourceTuple &input, Shipper<OutputTuple> &shipper) {
        const unsigned long minute       = input.minute_timestamp;
        const auto          counts_entry = counts.find(minute);

        if (counts_entry == counts.end()) {
            if (buffer.full()) {
                const unsigned long old_minute = buffer.remove();
                counts.erase(old_minute);
            }

            counts.insert({minute, 1});
            buffer.add(minute);
        } else {
            counts_entry->second += 1;
        }
        OutputTuple output;
        output.minute    = minute;
        output.count     = counts.find(minute)->second;
        output.timestamp = input.timestamp;
        shipper.push(move(output));
    }
};

class StatusCounterFunctor {
    // logger
    unordered_map<unsigned, unsigned long> counts;

public:
    void operator()(const SourceTuple &input, Shipper<OutputTuple> &shipper) {
        const auto status_code  = input.response;
        const auto counts_entry = counts.find(status_code);
        if (counts_entry == counts.end()) {
            counts.insert({status_code, 0});
        } else {
            counts_entry->second += 1;
        }
        OutputTuple output;
        output.tag         = OutputTuple::Status;
        output.status_code = status_code;
        output.count       = counts.find(status_code)->second;
        output.timestamp   = input.timestamp;
        shipper.push(move(output));
    }
};

class CountryStats {
    static constexpr unsigned count_index      = 0;
    static constexpr unsigned percentage_intex = 1;

    unsigned                                country_total = 0;
    string                                  country_name;
    unordered_map<string, vector<unsigned>> city_stats;

public:
    CountryStats(const string &country_name) : country_name {country_name} {}

    void city_found(const string &city_name) {
        ++country_total;

        auto city_stats_entry = city_stats.find(city_name);
        if (city_stats_entry != city_stats.end()) {
            assert(count_index < city_stats_entry->second.size());
            city_stats_entry->second[count_index] += 1;
        } else {
            city_stats.insert({city_name, {1, 0}});
        }
        auto &stats = city_stats.find(city_name)->second;

        assert(count_index < stats.size());
        assert(percentage_intex < stats.size());

        const double percent =
            stats[count_index] / static_cast<double>(country_total);
        stats[percentage_intex] = static_cast<unsigned>(percent);
    }

    unsigned get_country_total() {
        return country_total;
    }

    unsigned get_city_total(const string &city_name) {
        const auto entry = city_stats.find(city_name);
        assert(entry != city_stats.end());
        assert(count_index < entry->second.size());
        return entry->second[count_index];
    }
};

class GeoStatsFunctor {
    unordered_map<string, CountryStats> stats;

public:
    void operator()(const GeoFinderOutputTuple &tuple,
                    Shipper<OutputTuple> &      shipper) {
        if (stats.find(tuple.country) == stats.end()) {
            stats.insert({tuple.country, {tuple.country}});
        }

        auto &current_stats = stats.find(tuple.country)->second;
        current_stats.city_found(tuple.city);

        OutputTuple output;
        output.tag           = OutputTuple::Geo;
        output.country       = tuple.country;
        output.country_total = current_stats.get_country_total();
        output.city          = tuple.city;
        output.city_total    = current_stats.get_city_total(tuple.city);
        shipper.push(move(output));
    }
};

class SinkFunctor {
#ifndef NDEBUG
    inline static mutex print_mutex {};
#endif
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

    void operator()(optional<OutputTuple> &input, RuntimeContext &context) {
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
            const lock_guard lock {print_mutex};
            cout << "arrival time: " << arrival_time
                 << " ts:" << input->timestamp << " latency: " << latency
                 << '\n';
            switch (input->tag) {
            case OutputTuple::Volume:
                cout << "Received volume\n";
                break;
            case OutputTuple::Status:
                cout << "Received status\n";
                break;
            case OutputTuple::Geo:
                cout << "Received geo\n";
                break;
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
    // SourceFunctor source_functor {parameters.duration,
    // parameters.tuple_rate}; auto          source = Source_Builder
    // {source_functor}
    //                   .withParallelism(parameters.source_parallelism)
    //                   .withName("source")
    //                   .withOutputBatchSize(parameters.batch_size)
    //                   .build();

    // MapFunctor<BasicClassifier> map_functor;
    // auto                        classifier_node = Map_Builder
    // {map_functor}
    //                            .withParallelism(parameters.map_parallelism)
    //                            .withName("classifier")
    //                            .withOutputBatchSize(parameters.batch_size)
    //                            .build();

    // SinkFunctor sink_functor {parameters.sampling_rate};
    // auto        sink = Sink_Builder {sink_functor}
    //                 .withParallelism(parameters.sink_parallelism)
    //                 .withName("sink")
    //                 .build();

    // if (parameters.use_chaining) {
    //     graph.add_source(source).chain(classifier_node).chain_sink(sink);
    // } else {
    //     graph.add_source(source).add(classifier_node).add_sink(sink);
    // }
    // return graph;
}

int main(int argc, char *argv[]) {
    // TODO
    return 0;
}