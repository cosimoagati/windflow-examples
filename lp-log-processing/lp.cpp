/*
 * Copyright (C) 2021-2022 Cosimo Agati
 *
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the Affero GNU General Public License
 * as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * You should have received a copy of the GNU AGPLv3 with this software,
 * if not, please visit <https://www.gnu.org/licenses/>
 */

#define _XOPEN_SOURCE
#include <arpa/inet.h>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <ctime>
#include <fstream>
#include <getopt.h>
#include <maxminddb.h>
#include <mutex>
#include <nlohmann/json.hpp>
#include <numeric>
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
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
    source_id         = 0,
    volume_counter_id = 1,
    status_counter_id = 2,
    geo_finder_id     = 3,
    geo_stats_id      = 4,
    sink_id           = 5,
    num_nodes         = 6
};

struct Parameters {
    const char *     metric_output_directory   = ".";
    Execution_Mode_t execution_mode            = Execution_Mode_t::DEFAULT;
    Time_Policy_t    time_policy               = Time_Policy_t::INGRESS_TIME;
    unsigned         parallelism[num_nodes]    = {1, 1, 1, 1, 1, 1};
    unsigned         batch_size[num_nodes - 1] = {0, 0, 0, 0, 0};
    unsigned         duration                  = 60;
    unsigned         tuple_rate                = 0;
    unsigned         sampling_rate             = 100;
    bool             use_chaining              = false;
};

enum class TupleTag { Volume, Status, Geo };

// TODO: Use unions to save space?
struct SourceTuple {
    TupleTag      tag;
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
    TupleTag      tag;
    string        country;
    string        city;
    unsigned      country_total;
    unsigned      city_total;
    unsigned      status_code;
    unsigned long minute;
    unsigned long count;
    unsigned long timestamp;
};

static const struct option long_opts[] = {{"help", 0, 0, 'h'},
                                          {"rate", 1, 0, 'r'},
                                          {"sampling", 1, 0, 's'},
                                          {"parallelism", 1, 0, 'p'},
                                          {"batch", 1, 0, 'b'},
                                          {"chaining", 1, 0, 'c'},
                                          {"duration", 1, 0, 'd'},
                                          {"outputdir", 1, 0, 'o'},
                                          {"execmode", 1, 0, 'e'},
                                          {"timepolicy", 1, 0, 't'},
                                          {0, 0, 0, 0}};

class MMDB_handle {
    MMDB_s mmdb;

public:
    MMDB_handle(const char *path = "GeoLite2-City.mmdb") {
        const int status = MMDB_open(path, MMDB_MODE_MMAP, &mmdb);
        if (status != MMDB_SUCCESS) {
            cerr << "Error opening MaxMind database file\n";
            exit(EXIT_FAILURE);
        }
    }

    MMDB_handle(const MMDB_handle &other)
        : MMDB_handle {other.mmdb.filename} {}

    MMDB_handle &operator=(const MMDB_handle &other) {
        MMDB_close(&mmdb);
        const int status =
            MMDB_open(other.mmdb.filename, MMDB_MODE_MMAP, &mmdb);
        if (status != MMDB_SUCCESS) {
            cerr << "Error opening MaxMind database file\n";
            exit(EXIT_FAILURE);
        }
        return *this;
    }

    MMDB_handle(MMDB_handle &&other) = delete;
    MMDB_handle &operator=(MMDB_handle &&other) = delete;

    ~MMDB_handle() {
        MMDB_close(&mmdb);
    }

    const MMDB_s &db() const {
        return mmdb;
    }
};

static inline unsigned long
get_millis_date_truncated_by_minute(const char *date_string) {
    tm time;
    memset(&time, 0, sizeof(time));
    const char *ptr = strptime(date_string, "%d/%b/%Y:%H:%M:%S %z", &time);
    if (!ptr) {
        cerr << "get_millis_date_truncated_by_minute:  error building "
                "tm structure\n";
        exit(EXIT_FAILURE);
    }
    time.tm_sec   = 0;
    time_t result = mktime(&time);
    if (result == (time_t) -1) {
        cerr << "get_millis_date_truncated_by_minute:  error building time_t "
                "structure\n";
        exit(EXIT_FAILURE);
    }
    return (unsigned long) result * 1000;
}

static inline bool is_valid_ip_address(const char *ip) {
    sockaddr_in sa;
    const int   result = inet_pton(AF_INET, ip, &(sa.sin_addr));
    return result == 1;
}

static inline vector<string> split_log_fields(const string &line) {
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

static inline string char_buf_to_string(const char *buf, size_t size) {
    string result;
    for (size_t i = 0; i < size; ++i) {
        result.push_back(buf[i]);
    }
    result.shrink_to_fit();
    return result;
}

static inline pair<optional<string>, optional<string>>
lookup_country_and_city(const MMDB_s &mmdb, const char *ip_string) {
    int  gai_error;
    int  mmdb_error;
    auto db_node =
        MMDB_lookup_string(&mmdb, ip_string, &gai_error, &mmdb_error);
    if (gai_error != 0 || mmdb_error != MMDB_SUCCESS || !db_node.found_entry) {
        return {{}, {}};
    }
    MMDB_entry_data_s                        entry_data;
    pair<optional<string>, optional<string>> result;

    int status = MMDB_get_value(&db_node.entry, &entry_data, "country",
                                "names", "en", NULL);
    if (status != MMDB_SUCCESS || !entry_data.has_data
        || entry_data.type != MMDB_DATA_TYPE_UTF8_STRING) {
        result.first = {};
    } else {
        result.first =
            char_buf_to_string(entry_data.utf8_string, entry_data.data_size);
    }

    status = MMDB_get_value(&db_node.entry, &entry_data, "city", "names", "en",
                            NULL);
    if (status != MMDB_SUCCESS || !entry_data.has_data
        || entry_data.type != MMDB_DATA_TYPE_UTF8_STRING) {
        result.second = {};
    } else {
        result.second =
            char_buf_to_string(entry_data.utf8_string, entry_data.data_size);
    }
    return result;
}

static inline optional<SourceTuple> build_source_tuple(const string &line) {
    const auto tokens = split_log_fields(line);
    if (tokens.size() != 9) {
        return {};
    }
    SourceTuple tuple;
    tuple.ip            = tokens[1];
    tuple.log_timestamp = tokens[4];
    tuple.minute_timestamp =
        get_millis_date_truncated_by_minute(tuple.log_timestamp.c_str());
    tuple.request   = tokens[5];
    tuple.response  = stoul(tokens[6]);
    tuple.byte_size = tokens[7] == string {"-"} ? 0 : stoul(tokens[7]);
    return tuple;
}

static inline vector<SourceTuple> parse_logs(const char *path) {
    ifstream            log_stream {path};
    vector<SourceTuple> logs;

    string line;
    while (log_stream.good() && getline(log_stream, line)) {
        auto log = build_source_tuple(line);
        if (log) {
            logs.push_back(move(*log));
        }
    }
    return logs;
}

#ifndef NDEBUG
static inline string get_log_output_message(const OutputTuple &input,
                                            unsigned long      arrival_time,
                                            unsigned long      latency) {
    stringstream msg;

    msg << "[SINK] Received ";
    switch (input.tag) {
    case TupleTag::Volume:
        msg << "volume - count: " << input.count
            << ", timestampMinutes: " << input.minute;
        break;
    case TupleTag::Status:
        msg << "status - response: " << input.status_code
            << ", count: " << input.count;
        break;
    case TupleTag::Geo:
        msg << "Geo stats - country: " << input.country
            << ", city: " << input.city << ", cityTotal: " << input.city_total
            << ", countryTotal: " << input.country_total;
        break;
    default:
        assert(false);
        break;
    }
    msg << " arrival time: " << arrival_time << " ts: " << input.timestamp
        << " latency: " << latency << '\n';
    return msg.str();
}
#endif

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
                    "<nSource,nVolumeCounter,nStatusCounter,nGeoFinder,"
                    "nGeoStats,nSink> "
                    "[--duration <seconds>] "
                    "[--chaining <value>]\n";
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
         << "Volume counter parallelism:\t"
         << parameters.parallelism[volume_counter_id] << '\n'
         << "Status counter parallelism:\t"
         << parameters.parallelism[status_counter_id] << '\n'
         << "Geo finder parallelism:\t"
         << parameters.parallelism[geo_finder_id] << '\n'
         << "Geo stats parallelism:\t" << parameters.parallelism[geo_stats_id]
         << '\n'
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
         << "Tuple generation rate:\t";
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

static inline void
print_statistics(unsigned long elapsed_time, unsigned long duration,
                 unsigned long sent_tuples, double average_total_latency,
                 double average_volume_latency, double average_status_latency,
                 double average_geo_latency, unsigned long received_tuples) {
    const auto elapsed_time_in_seconds =
        elapsed_time / static_cast<double>(timeunit_scale_factor);

    const auto throughput =
        elapsed_time > 0 ? sent_tuples / static_cast<double>(elapsed_time)
                         : sent_tuples;

    const auto throughput_in_seconds   = throughput * timeunit_scale_factor;
    const auto service_time            = 1 / throughput;
    const auto service_time_in_seconds = service_time / timeunit_scale_factor;
    const auto latency_in_seconds =
        average_total_latency / timeunit_scale_factor;
    const auto volume_latency_in_seconds =
        average_volume_latency / timeunit_scale_factor;
    const auto status_latency_in_seconds =
        average_status_latency / timeunit_scale_factor;
    const auto geo_latency_in_seconds =
        average_geo_latency / timeunit_scale_factor;

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
         << "Average latency: " << average_total_latency << ' '
         << timeunit_string << "s (" << latency_in_seconds << " seconds)\n"
         << "Average volume latency: " << average_volume_latency << ' '
         << timeunit_string << "s (" << volume_latency_in_seconds
         << " seoncds)\n"
         << "Average status latency: " << average_status_latency << ' '
         << timeunit_string << "s (" << status_latency_in_seconds
         << " seoncds)\n"
         << "Average geo latency: " << average_geo_latency << ' '
         << timeunit_string << "s (" << geo_latency_in_seconds
         << " seoncds)\n";
}

/*
 * Global variables
 */
static atomic_ulong global_sent_tuples {0};
static atomic_ulong global_received_tuples {0};
static atomic_ulong global_volume_received_tuples {0};
static atomic_ulong global_status_received_tuples {0};
static atomic_ulong global_geo_received_tuples {0};

static Metric<unsigned long> global_total_latency_metric {"lp-total-latency"};
static Metric<unsigned long> global_volume_latency_metric {
    "lp-volume-latency"};
static Metric<unsigned long> global_status_latency_metric {
    "lp-status-latency"};
static Metric<unsigned long> global_geo_latency_metric {"lp-geo-latency"};
#ifndef NDEBUG
static mutex print_mutex;
#endif

class SourceFunctor {
    vector<SourceTuple> logs;
    unsigned long       duration;
    unsigned            tuple_rate_per_second;

public:
    SourceFunctor(unsigned d, unsigned rate,
                  const char *path = "http-server.log")
        : logs {parse_logs(path)}, duration {d * timeunit_scale_factor},
          tuple_rate_per_second {rate} {
        if (logs.empty()) {
            cerr << "Error: empty log stream.  Check whether log file exists "
                    "and is readable\n";
            exit(EXIT_FAILURE);
        }
    }

    void operator()(Source_Shipper<SourceTuple> &shipper) {
        const auto    end_time    = current_time() + duration;
        unsigned long sent_tuples = 0;
        size_t        index       = 0;

        while (current_time() < end_time) {
            auto volume_source_tuple     = logs[index];
            auto status_source_tuple     = logs[index];
            auto geo_finder_source_tuple = logs[index];
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[SOURCE] Sending log with minute timestamp: "
                     << logs[index].minute_timestamp << '\n';
            }
#endif
            volume_source_tuple.tag     = TupleTag::Volume;
            status_source_tuple.tag     = TupleTag::Status;
            geo_finder_source_tuple.tag = TupleTag::Geo;

            const auto timestamp              = current_time();
            volume_source_tuple.timestamp     = timestamp;
            status_source_tuple.timestamp     = timestamp;
            geo_finder_source_tuple.timestamp = timestamp;

            shipper.push(move(volume_source_tuple));
            shipper.push(move(status_source_tuple));
            shipper.push(move(geo_finder_source_tuple));

            sent_tuples += 3;
            index = (index + 1) % logs.size();

            if (tuple_rate_per_second > 0) {
                const unsigned long delay =
                    (1.0 / tuple_rate_per_second) * timeunit_scale_factor;
                busy_wait(delay);
            }
        }
        global_sent_tuples.fetch_add(sent_tuples);
    }
};

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
        is_empty = false;
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
    CircularFifoQueue<unsigned long>            buffer;
    unordered_map<unsigned long, unsigned long> counts;

public:
    VolumeCounterFunctor(size_t window_size = 60)
        : buffer {window_size}, counts {window_size} {}

    OutputTuple operator()(const SourceTuple &input) {
        const unsigned long minute       = input.minute_timestamp;
        const auto          counts_entry = counts.find(minute);
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[VOLUME COUNTER] Received log with minute timestamp: "
                 << minute << '\n';
        }
#endif
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
        output.tag       = TupleTag::Volume;
        output.minute    = minute;
        output.count     = counts.find(minute)->second;
        output.timestamp = input.timestamp;
        return output;
    }
};

class StatusCounterFunctor {
    unordered_map<unsigned, unsigned long> counts;

public:
    OutputTuple operator()(const SourceTuple &input) {
        const auto status_code = input.response;
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[STATUS COUNTER] Received log with response status code: "
                 << status_code << '\n';
        }
#endif
        const auto counts_entry = counts.find(status_code);
        if (counts_entry == counts.end()) {
            counts.insert({status_code, 0});
        } else {
            counts_entry->second += 1;
        }
        OutputTuple output;
        output.tag         = TupleTag::Status;
        output.status_code = status_code;
        output.count       = counts.find(status_code)->second;
        output.timestamp   = input.timestamp;
        return output;
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

class GeoFinderFunctor {
    MMDB_handle mmdb;

public:
    void operator()(const SourceTuple &            input,
                    Shipper<GeoFinderOutputTuple> &shipper) {
        const auto ip = input.ip.c_str();
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[GEO FINDER] Received log with ip address: " << ip
                 << '\n';
        }
#endif
        if (is_valid_ip_address(ip)) {
            const auto  ip_info = lookup_country_and_city(mmdb.db(), ip);
            const auto &country = ip_info.first;
            const auto &city    = ip_info.second;
            GeoFinderOutputTuple output {country ? *country : "null",
                                         city ? *city : "null",
                                         input.timestamp};
            shipper.push(move(output));
        }
    }
};

class GeoStatsFunctor {
    unordered_map<string, CountryStats> stats;

public:
    OutputTuple operator()(const GeoFinderOutputTuple &input) {
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[GEO STATS] Received log with country " << input.country
                 << " and city " << input.city << '\n';
        }
#endif
        if (stats.find(input.country) == stats.end()) {
            stats.insert({input.country, {input.country}});
        }

        auto &current_stats = stats.find(input.country)->second;
        current_stats.city_found(input.city);

        OutputTuple output;
        output.tag           = TupleTag::Geo;
        output.country       = input.country;
        output.country_total = current_stats.get_country_total();
        output.city          = input.city;
        output.city_total    = current_stats.get_city_total(input.city);
        output.timestamp     = input.timestamp;
        return output;
    }
};

class SinkFunctor {
    vector<unsigned long>                          latency_samples;
    unordered_map<TupleTag, vector<unsigned long>> specific_latency_samples {
        {TupleTag::Volume, {}}, {TupleTag::Status, {}}, {TupleTag::Geo, {}}};

    unsigned long                          tuples_received = 0;
    unordered_map<TupleTag, unsigned long> specific_tuples_received {
        {TupleTag::Volume, 0}, {TupleTag::Status, 0}, {TupleTag::Geo, 0}};

    unsigned long last_sampling_time = current_time();
    unsigned long last_arrival_time  = last_sampling_time;
    unsigned      sampling_rate;

    bool is_time_to_sample(unsigned long arrival_time) const {
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

    void operator()(optional<OutputTuple> &input) {
        if (input) {
            assert(input->tag == TupleTag::Volume
                   || input->tag == TupleTag::Status
                   || input->tag == TupleTag::Geo);

            const auto arrival_time = current_time();
            const auto latency = difference(arrival_time, input->timestamp);

            ++tuples_received;
            ++specific_tuples_received[input->tag];
            last_arrival_time = arrival_time;

            if (is_time_to_sample(arrival_time)) {
                latency_samples.push_back(latency);
                specific_latency_samples[input->tag].push_back(latency);
                last_sampling_time = arrival_time;
#ifndef NDEBUG
                {
                    lock_guard lock {print_mutex};
                    const auto tuple_tag_string =
                        input->tag == TupleTag::Volume   ? "VOLUME"
                        : input->tag == TupleTag::Status ? "STATUS"
                        : input->tag == TupleTag::Geo    ? "GEO"
                                                         : "UNKNOWN";
                    clog << "[SINK] Sampled tuple of kind " << tuple_tag_string
                         << '\n';
                }
#endif
            }
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << get_log_output_message(*input, arrival_time, latency);
            }
#endif
        } else {
            global_received_tuples.fetch_add(tuples_received);
            global_volume_received_tuples.fetch_add(
                specific_tuples_received[TupleTag::Volume]);
            global_status_received_tuples.fetch_add(
                specific_tuples_received[TupleTag::Status]);
            global_geo_received_tuples.fetch_add(
                specific_tuples_received[TupleTag::Geo]);

            global_total_latency_metric.merge(latency_samples);
            global_volume_latency_metric.merge(
                specific_latency_samples[TupleTag::Volume]);
            global_status_latency_metric.merge(
                specific_latency_samples[TupleTag::Status]);
            global_geo_latency_metric.merge(
                specific_latency_samples[TupleTag::Geo]);
        }
    }
};

static inline PipeGraph &build_graph(const Parameters &parameters,
                                     PipeGraph &       graph) {
    SourceFunctor source_functor {parameters.duration, parameters.tuple_rate};
    auto          source_node =
        Source_Builder {source_functor}
            .withParallelism(parameters.parallelism[source_id])
            .withName("source")
            .withOutputBatchSize(parameters.batch_size[source_id])
            .build();

    VolumeCounterFunctor volume_counter_functor;
    auto                 volume_counter_node =
        Map_Builder {volume_counter_functor}
            .withParallelism(parameters.parallelism[volume_counter_id])
            .withName("volume counter")
            .withOutputBatchSize(parameters.batch_size[volume_counter_id])
            .withKeyBy([](const SourceTuple &t) -> unsigned long {
                return t.minute_timestamp;
            })
            .build();

    StatusCounterFunctor status_counter_functor;
    auto                 status_counter_node =
        Map_Builder {status_counter_functor}
            .withParallelism(parameters.parallelism[status_counter_id])
            .withName("status counter")
            .withOutputBatchSize(parameters.batch_size[status_counter_id])
            .withKeyBy(
                [](const SourceTuple &t) -> unsigned { return t.response; })
            .build();

    GeoFinderFunctor geo_finder_functor;
    auto             geo_finder_node =
        FlatMap_Builder {geo_finder_functor}
            .withParallelism(parameters.parallelism[geo_finder_id])
            .withName("geo finder")
            .withOutputBatchSize(parameters.batch_size[geo_finder_id])
            .build();

    GeoStatsFunctor geo_stats_functor;
    auto            geo_stats_node =
        Map_Builder {geo_stats_functor}
            .withParallelism(parameters.parallelism[geo_stats_id])
            .withName("geo stats")
            .withOutputBatchSize(parameters.batch_size[geo_stats_id])
            .withKeyBy([](const GeoFinderOutputTuple &t) -> string {
                return t.country;
            })
            .build();

    SinkFunctor sink_functor {parameters.sampling_rate};
    auto        sink_node = Sink_Builder {sink_functor}
                         .withParallelism(parameters.parallelism[sink_id])
                         .withName("sink")
                         .build();

    auto &source_pipe = graph.add_source(source_node);
    source_pipe.split(
        [](const SourceTuple &t) {
            switch (t.tag) {
            case TupleTag::Volume:
                return 0;
            case TupleTag::Status:
                return 1;
            case TupleTag::Geo:
                return 2;
            default:
                assert(false);
                break;
            }
            return 0; // Make the compiler happy
        },
        3);

    if (parameters.use_chaining) {
        auto &volume_pipe = source_pipe.select(0).chain(volume_counter_node);
        auto &status_counter_pipe =
            source_pipe.select(1).chain(status_counter_node);
        auto &geo_pipe =
            source_pipe.select(2).chain(geo_finder_node).chain(geo_stats_node);
        volume_pipe.merge(status_counter_pipe, geo_pipe).chain_sink(sink_node);
    } else {
        auto &volume_pipe = source_pipe.select(0).add(volume_counter_node);
        auto &status_counter_pipe =
            source_pipe.select(1).add(status_counter_node);
        auto &geo_pipe =
            source_pipe.select(2).add(geo_finder_node).add(geo_stats_node);
        volume_pipe.merge(status_counter_pipe, geo_pipe).add_sink(sink_node);
    }
    return graph;
}

int main(int argc, char *argv[]) {
    Parameters parameters;
    parse_args(argc, argv, parameters);
    validate_args(parameters);
    print_initial_parameters(parameters);

    PipeGraph graph {"lp-log-processing", parameters.execution_mode,
                     parameters.time_policy};
    build_graph(parameters, graph);

    const auto start_time = current_time();
    graph.run();
    const auto elapsed_time = difference(current_time(), start_time);

    const double throughput =
        elapsed_time > 0
            ? (global_sent_tuples.load() / static_cast<double>(elapsed_time))
            : global_sent_tuples.load();

    const double service_time = 1 / throughput;

    const auto latency_stats = get_distribution_stats(
        global_total_latency_metric, parameters, global_received_tuples);
    serialize_json(latency_stats, "lp-total-latency",
                   parameters.metric_output_directory);

    const auto volume_latency_stats =
        get_distribution_stats(global_volume_latency_metric, parameters,
                               global_volume_received_tuples);
    serialize_json(volume_latency_stats, "lp-volume-latency",
                   parameters.metric_output_directory);

    const auto status_latency_stats =
        get_distribution_stats(global_status_latency_metric, parameters,
                               global_status_received_tuples);
    serialize_json(status_latency_stats, "lp-status-latency",
                   parameters.metric_output_directory);

    const auto geo_latency_stats = get_distribution_stats(
        global_geo_latency_metric, parameters, global_geo_received_tuples);
    serialize_json(geo_latency_stats, "lp-geo-latency",
                   parameters.metric_output_directory);

    const auto throughput_stats = get_single_value_stats(
        throughput, "throughput", parameters, global_sent_tuples);
    serialize_json(throughput_stats, "lp-throughput",
                   parameters.metric_output_directory);

    const auto service_time_stats = get_single_value_stats(
        service_time, "service time", parameters, global_sent_tuples);
    serialize_json(service_time_stats, "lp-service-time",
                   parameters.metric_output_directory);

    const auto average_total_latency =
        accumulate(global_total_latency_metric.begin(),
                   global_total_latency_metric.end(), 0.0)
        / (!global_total_latency_metric.empty()
               ? global_total_latency_metric.size()
               : 1.0);

    const auto average_volume_latency =
        accumulate(global_volume_latency_metric.begin(),
                   global_volume_latency_metric.end(), 0.0)
        / (!global_volume_latency_metric.empty()
               ? global_volume_latency_metric.size()
               : 1.0);

    const auto average_status_latency =
        accumulate(global_status_latency_metric.begin(),
                   global_status_latency_metric.end(), 0.0)
        / (!global_status_latency_metric.empty()
               ? global_status_latency_metric.size()
               : 1.0);
    const auto average_geo_latency =
        accumulate(global_geo_latency_metric.begin(),
                   global_geo_latency_metric.end(), 0.0)
        / (!global_geo_latency_metric.empty()
               ? global_geo_latency_metric.size()
               : 1.0);

    print_statistics(elapsed_time, parameters.duration, global_sent_tuples,
                     average_total_latency, average_volume_latency,
                     average_status_latency, average_geo_latency,
                     global_received_tuples);
    return 0;
}
