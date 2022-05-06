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

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <getopt.h>
#include <mutex>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <unordered_map>
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
    source_id              = 0,
    topic_extractor_id     = 1,
    rolling_counter_id     = 2,
    intermediate_ranker_id = 3,
    total_ranker_id        = 4,
    sink_id                = 5,
    num_nodes              = 6
};

struct Parameters {
    const char *     metric_output_directory   = ".";
    Execution_Mode_t execution_mode            = Execution_Mode_t::DEFAULT;
    Time_Policy_t    time_policy               = Time_Policy_t::INGRESS_TIME;
    unsigned         parallelism[num_nodes]    = {1, 1, 1, 1, 1, 1};
    unsigned         batch_size[num_nodes - 1] = {0, 0, 0, 0, 0};
    unsigned         rolling_counter_frequency = 2;
    unsigned         intermediate_ranker_frequency = 2;
    unsigned         total_ranker_frequency        = 2;
    unsigned         duration                      = 60;
    unsigned         tuple_rate                    = 1000;
    unsigned         sampling_rate                 = 100;
    bool             use_chaining                  = false;
};

struct TupleMetadata {
    unsigned long id;
    unsigned long timestamp;
};

struct Tweet {
    TupleMetadata metadata;
    string        id;
    string        text;
    unsigned long timestamp;
};

struct Topic {
    TupleMetadata metadata;
    string        word;
    bool          is_tick_tuple;
};

struct Counts {
    TupleMetadata metadata;
    string        word;
    unsigned long count;
    size_t        window_length;
    bool          is_tick_tuple;
};

static const struct option long_opts[] = {
    {"help", 0, 0, 'h'},       {"rate", 1, 0, 'r'},
    {"sampling", 1, 0, 's'},   {"parallelism", 1, 0, 'p'},
    {"batch", 1, 0, 'b'},      {"chaining", 1, 0, 'c'},
    {"duration", 1, 0, 'd'},   {"frequency", 1, 0, 'f'},
    {"outputdir", 1, 0, 'o'},  {"execmode", 1, 0, 'e'},
    {"timepolicy", 1, 0, 't'}, {0, 0, 0, 0}};

template<typename T>
class Rankable {
    T      object;
    size_t count;

public:
    Rankable(const T &object, size_t count) : object {object}, count {count} {}

    const T &get_object() const {
        return object;
    }

    size_t get_count() const {
        return count;
    }
};

#ifndef NDEBUG
template<typename T>
ostream &operator<<(ostream &stream, const Rankable<T> rankable) {
    stream << rankable.get_object() << ": " << rankable.get_count();
    return stream;
}
#endif

template<typename T>
bool operator<(const Rankable<T> &a, const Rankable<T> &b) {
    return a.get_count() < b.get_count();
}

template<typename T>
class Rankings {
    static constexpr auto default_count = 10u;

    size_t              max_size_field;
    vector<Rankable<T>> ranked_items;

    optional<size_t> find_rank_of(Rankable<T> r) {
        const auto &tag = r.get_object();
        for (size_t rank {0}; rank < ranked_items.size(); ++rank) {
            const auto &current_obj = ranked_items[rank].get_object();

            if (current_obj == tag) {
                return rank;
            }
        }
        return {};
    }

    void rerank() {
        sort(ranked_items.begin(), ranked_items.end());
        reverse(ranked_items.begin(), ranked_items.end());
    }

    void shrink_rankings_if_needed() {
        if (ranked_items.size() > max_size_field) {
            ranked_items.erase(ranked_items.begin() + max_size_field);
        }
    }

    void add_or_replace(const Rankable<T> &rankable) {
        const auto rank = find_rank_of(rankable);
        if (rank) {
            assert(*rank < ranked_items.size());
            ranked_items[*rank] = rankable;
        } else {
            ranked_items.push_back(rankable);
        }
    }

public:
    Rankings(size_t top_n = default_count) : max_size_field {top_n} {
        if (top_n < 1) {
            cerr << "Error initializing Rankings object: top_n must be >= 1\n";
            exit(EXIT_FAILURE);
        }
    }

    size_t max_size() const {
        return max_size_field;
    }

    unsigned size() const {
        return ranked_items.size();
    }

    vector<Rankable<T>> get_rankings() const {
        vector<Rankable<T>> result;
        for (const auto &item : ranked_items) {
            result.push_back(item);
        }
        return result;
    }

    void update_with(Rankable<T> r) {
        // XXX: THIS SHOULD BE THREAD SAFE!!!
        add_or_replace(r);
        rerank();
        shrink_rankings_if_needed();
    }

    void update_with(const Rankings<T> &other) {
        for (const auto &r : other.get_rankings()) {
            update_with(r);
        }
    }

    void prune_zero_counts() {
        size_t i {0};
        while (i < ranked_items.size()) {
            if (ranked_items[i].get_count() == 0) {
                ranked_items.erase(ranked_items.begin() + i);
            } else {
                ++i;
            }
        }
    }

    typename vector<Rankable<T>>::const_iterator begin() const {
        return ranked_items.begin();
    }

    typename vector<Rankable<T>>::const_iterator end() const {
        return ranked_items.end();
    }
};

#ifndef NDEBUG
template<typename T>
ostream &operator<<(ostream &stream, const Rankings<T> &rankings) {
    stream << "Rankings: ";
    const auto items = rankings.get_rankings();
    for (size_t i = 0; i < items.size(); ++i) {
        stream << items[i];
        if (i != items.size() - 1) {
            stream << ", ";
        }
    }
    return stream;
}
#endif

struct RankingsTuple {
    TupleMetadata    metadata;
    Rankings<string> rankings;
    bool             is_tick_tuple;
};

template<typename T>
class SlotBasedCounter {
    unordered_map<T, vector<unsigned long>> counts_map;
    size_t                                  num_slots;

    unsigned long compute_total_count(T obj) {
        const auto counts_entry = counts_map.find(obj);

        assert(counts_entry != counts_map.end());

        unsigned long total = 0;
        for (const auto count : counts_entry->second) {
            total += count;
        }
        return total;
    }

    void reset_slot_count_to_zero(T obj, unsigned slot) {
        const auto counts_entry = counts_map.find(obj);

        assert(counts_entry != counts_map.end());
        counts_entry->second[slot] = 0;
    }

    bool should_be_removed_from_counter(T obj) {
        return compute_total_count(obj) == 0;
    }

public:
    SlotBasedCounter(size_t num_slots) : num_slots {num_slots} {
        if (num_slots == 0) {
            cerr << "Error: SlotBasedCounter must be initialized with a "
                    "positive num_slots value\n";
            exit(EXIT_FAILURE);
        }
    }

    void increment_count(const T &obj, size_t slot, unsigned long increment) {
        assert(slot < num_slots);

        if (counts_map.find(obj) == counts_map.end()) {
            counts_map.insert({obj, vector<unsigned long>(num_slots)});
        }
        counts_map.find(obj)->second[slot] += increment;
    }

    void increment_count(const T &obj, size_t slot) {
        increment_count(obj, slot, 1);
    }

    unsigned long get_count(const T &obj, size_t slot) {
        assert(slot < num_slots);

        const auto counts_entry = counts_map.find(obj);
        if (counts_entry == counts_map.end()) {
            return 0;
        } else {
            return counts_entry->second[slot];
        }
    }

    unordered_map<T, unsigned long> get_counts() {
        unordered_map<T, unsigned long> result;
        for (const auto &kv : counts_map) {
            result.insert_or_assign(kv.first, compute_total_count(kv.first));
        }
        return result;
    }

    void wipe_slot(size_t slot) {
        assert(slot < num_slots);

        for (const auto &kv : counts_map) {
            reset_slot_count_to_zero(kv.first, slot);
        }
    }

    void wipe_zeros() {
        vector<T> objs_to_be_removed;
        for (const auto &kv : counts_map) {
            if (should_be_removed_from_counter(kv.first)) {
                objs_to_be_removed.push_back(kv.first);
            }
        }
        for (const auto &obj : objs_to_be_removed) {
            counts_map.erase(obj);
        }
    }
};

template<typename T>
class SlidingWindowCounter {
    SlotBasedCounter<T> obj_counter;
    size_t              window_length_in_slots;
    size_t              head_slot = 0;
    size_t              tail_slot;

    size_t slot_after(size_t slot) {
        assert(slot < window_length_in_slots);
        return (slot + 1) % window_length_in_slots;
    }

    void advance_head() {
        head_slot = tail_slot;
        tail_slot = slot_after(tail_slot);
    }

public:
    SlidingWindowCounter(size_t window_length_in_slots)
        : obj_counter {window_length_in_slots}, window_length_in_slots {
                                                    window_length_in_slots} {
        if (window_length_in_slots < 2) {
            cerr << "Error: Window length for sliding window counter must be "
                    "at least two\n";
            exit(EXIT_FAILURE);
        }
        tail_slot = slot_after(head_slot);
    }

    void increment_count(const T &obj) {
        obj_counter.increment_count(obj, head_slot);
    }

    void increment_count(const T &obj, unsigned long increment) {
        obj_counter.increment_count(obj, head_slot, increment);
    }

    unordered_map<T, unsigned long> get_counts_then_advance_window() {
        const auto counts = obj_counter.get_counts();
        obj_counter.wipe_zeros();
        obj_counter.wipe_slot(tail_slot);
        advance_head();
        return counts;
    }
};

static inline vector<string_view> string_split(const string_view &s,
                                               const string &     delims) {
    const auto is_delim = [=](char c) {
        return delims.find(c) != string::npos;
    };
    auto                word_begin = find_if_not(s.begin(), s.end(), is_delim);
    vector<string_view> words;

    while (word_begin < s.end()) {
        const auto word_end = find_if(word_begin + 1, s.end(), is_delim);
        words.emplace_back(word_begin, word_end - word_begin);
        word_begin = find_if_not(word_end, s.end(), is_delim);
    }
    return words;
}

static inline vector<string> get_tweets_from_file(const char *filename) {
    ifstream       twitterstream {filename};
    vector<string> tweets;

    while (twitterstream.good()) {
        nlohmann::json new_tweet;
        twitterstream >> new_tweet;
        tweets.push_back(move(new_tweet["data"]["text"]));
        twitterstream >> ws;
    }
    tweets.shrink_to_fit();
    return tweets;
}

static inline void parse_args(int argc, char **argv, Parameters &parameters) {
    int option;
    int index;

    while ((option = getopt_long(argc, argv, "r:s:p:b:c:d:f:o:e:t:h",
                                 long_opts, &index))
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
        case 'f': {
            const auto frequencies = get_nums_split_by_commas(optarg);
            if (frequencies.size() != 3) {
                cerr << "Error in parsing input arguments.  Frequencies "
                        "string requires exactly 3 elements\n";
                exit(EXIT_FAILURE);
            }
            parameters.rolling_counter_frequency     = frequencies[0];
            parameters.intermediate_ranker_frequency = frequencies[1];
            parameters.total_ranker_frequency        = frequencies[2];
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
            parameters.use_chaining = atoi(optarg) > 0 ? true : false;
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
                    "<nSource,nTopicExtractor,nRollingCounter,"
                    "nIntermediateRanker,nTotalRanker,"
                    "nSink> [--duration <seconds>] "
                    "[--chaining <value>]\n";
            exit(EXIT_SUCCESS);
        default:
            cerr << "Error in parsing the input arguments.  Use the --help "
                    "(-h) option for usage information.\n";
            exit(EXIT_FAILURE);
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

    constexpr unsigned timer_threads = 3;
    const auto max_threads = thread::hardware_concurrency() - timer_threads;

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
         << "Topic extractor parallelism:\t"
         << parameters.parallelism[topic_extractor_id] << '\n'
         << "Rolling counter parallelism:\t"
         << parameters.parallelism[rolling_counter_id] << '\n'
         << "Intermediate ranker parallelism:\t"
         << parameters.parallelism[intermediate_ranker_id] << '\n'
         << "Total ranker parallelism:\t"
         << parameters.parallelism[total_ranker_id] << '\n'
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
    cout << "Rolling counter output frequency:\t"
         << parameters.rolling_counter_frequency << " seconds\n"
         << "Intermediate ranker frequency:\t"
         << parameters.intermediate_ranker_frequency << " seconds\n"
         << "Total ranker frequency:\t" << parameters.total_ranker_frequency
         << " seconds\n";
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

/*
 * Global variables
 */
static atomic_ulong          global_sent_tuples {0};
static atomic_ulong          global_received_tuples {0};
static Metric<unsigned long> global_latency_metric {"latency"};
static Metric<unsigned long> global_interdeparture_metric {
    "interdeparture-time"};
static Metric<unsigned long> global_service_time_metric {"service-time"};
#ifndef NDEBUG
static mutex print_mutex;
#endif

template<typename OutputTuple>
class TimerFunctor {
    unsigned long last_tick_time = current_time();
    unsigned long duration;
    unsigned long time_units_between_ticks;
    unsigned      replicas;

public:
    TimerFunctor(unsigned d, unsigned seconds_per_tick, unsigned replicas)
        : duration {d * timeunit_scale_factor},
          time_units_between_ticks {seconds_per_tick * timeunit_scale_factor},
          replicas {replicas} {
        if (time_units_between_ticks == 0) {
            cerr << "Error: the amount of time units between ticks "
                    "must be positive\n";
            exit(EXIT_FAILURE);
        }
    }

    void operator()(Source_Shipper<OutputTuple> &shipper) {
        unsigned long       now      = current_time();
        const unsigned long end_time = now + duration;

        while (now < end_time) {
            unsigned long delta = difference(current_time(), last_tick_time);
            while (delta >= time_units_between_ticks) {
                for (unsigned i = 0; i < replicas; ++i) {
                    OutputTuple tuple;
                    tuple.is_tick_tuple = true;
                    shipper.push(move(tuple));
                }
                delta -= time_units_between_ticks;
                last_tick_time += time_units_between_ticks;
            }
            usleep(time_units_between_ticks / timeunit_scale_factor * 1000000);
            now = current_time();
        }
    }
};

using RollingCounterTimerFunctor     = TimerFunctor<Topic>;
using IntermediateRankerTimerFunctor = TimerFunctor<Counts>;
using TotalRankerTimerFunctor        = TimerFunctor<RankingsTuple>;

template<typename T>
class CircularFifoBuffer {
    vector<T> buffer;
    size_t    head     = 0;
    size_t    tail     = 0;
    bool      is_empty = true;

public:
    CircularFifoBuffer(size_t size) : buffer(size) {
        if (size == 0) {
            cerr << "Error initializing circular buffer: size must be "
                    "positive\n";
            exit(EXIT_FAILURE);
        }
    }

    size_t max_size() const {
        return buffer.size();
    }

    void add(const T &element) {
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[CIRCULAR FIFO BUFFER] Adding " << element << '\n';
        }
#endif
        buffer[head] = element;
        head         = (head + 1) % buffer.size();
        if (head == tail) {
            tail = (tail + 1) % buffer.size();
        }
        is_empty = false;
    }

    const T &get() const {
        assert(!is_empty);
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[CIRCULAR FIFO BUFFER] Returning " << buffer[tail]
                 << '\n';
        }
#endif
        return buffer[tail];
    }
};

static inline uint64_t current_time_msecs() __attribute__((always_inline));
static inline uint64_t current_time_msecs() {
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return (t.tv_sec) * 1000UL + (t.tv_nsec / 1000000UL);
}

class NthLastModifiedTimeTracker {
    static constexpr unsigned millis_in_sec = 1000;

    CircularFifoBuffer<unsigned long> last_modified_times_millis;

public:
    NthLastModifiedTimeTracker(size_t num_times_to_track)
        : last_modified_times_millis {num_times_to_track} {
        if (num_times_to_track < 1) {
            cerr << "Error: num_times_to_track must be positive\n";
            exit(EXIT_FAILURE);
        }
        const auto now_cached = current_time_msecs();
        for (size_t i = 0; i < last_modified_times_millis.max_size(); ++i) {
            last_modified_times_millis.add(now_cached);
        }
    }

    unsigned seconds_since_oldest_modification() {
        const auto modified_time_millis = last_modified_times_millis.get();
        return static_cast<unsigned>(
            (current_time_msecs() - modified_time_millis) / millis_in_sec);
    }

    void mark_as_modified() {
        last_modified_times_millis.add(current_time_msecs());
    }
};

class SourceFunctor {
    static constexpr auto default_path = "tweetstream.jsonl";
    vector<string>        tweets;
    unsigned long         duration;
    unsigned              tuple_rate_per_second;

public:
    SourceFunctor(unsigned d, unsigned rate, const char *path = default_path)
        : tweets {get_tweets_from_file(path)},
          duration {d * timeunit_scale_factor}, tuple_rate_per_second {rate} {
        if (tweets.empty()) {
            cerr << "Error: empty tweet stream.  Check whether dataset "
                    "file "
                    "exists and is readable\n";
            exit(EXIT_FAILURE);
        }
    }

    void operator()(Source_Shipper<Tweet> &shipper) {
        const auto    end_time    = current_time() + duration;
        unsigned long sent_tuples = 0;
        size_t        index       = 0;

        while (current_time() < end_time) {
            auto tweet = tweets[index];
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[SOURCE] Sending the following tweet: " << tweet
                     << '\n';
            }
#endif
            const auto timestamp = current_time();
            shipper.push({{timestamp, timestamp}, "", move(tweet), timestamp});
            ++sent_tuples;
            index = (index + 1) % tweets.size();

            if (tuple_rate_per_second > 0) {
                const unsigned long delay =
                    (1.0 / tuple_rate_per_second) * timeunit_scale_factor;
                busy_wait(delay);
            }
        }
        global_sent_tuples.fetch_add(sent_tuples);
    }
};

class TopicExtractorFunctor {
public:
    void operator()(const Tweet &tweet, Shipper<Topic> &shipper) {
        if (!tweet.text.empty()) {
            const auto words = string_split(tweet.text, " \n\t");
            for (const auto &word : words) {
                assert(!word.empty());
                if (word[0] == '#') {
#ifndef NDEBUG
                    {
                        lock_guard lock {print_mutex};
                        clog << "[TOPIC EXTRACTOR] Extracted topic: " << word
                             << '\n';
                    }
#endif
                    shipper.push({tweet.metadata, string {word}, false});
                }
            }
        }
    }
};

class RollingCounterFunctor {
    unsigned                     window_length_in_seconds;
    SlidingWindowCounter<string> counter;
    NthLastModifiedTimeTracker   last_modified_tracker;

    TupleMetadata first_parent {0, 0};

    void ship_all(Shipper<Counts> &shipper) {
        const auto counts = counter.get_counts_then_advance_window();
        const auto actual_window_length_in_seconds =
            last_modified_tracker.seconds_since_oldest_modification();
        last_modified_tracker.mark_as_modified();

#ifndef NDEBUG
        if (actual_window_length_in_seconds != window_length_in_seconds) {
            {
                lock_guard lock {print_mutex};
                clog << "[ROLLING COUNTER] Warning: actual window "
                        "length "
                        "is "
                     << actual_window_length_in_seconds
                     << " when it should be " << window_length_in_seconds
                     << " seconds (you can safely ignore this warning "
                        "during "
                        "the startup phase)\n";
            }
        }
#endif
        for (const auto &kv : counts) {
            const auto &word  = kv.first;
            const auto  count = kv.second;
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[ROLLING COUNTER] Sending word: " << word
                     << " with count: " << count << '\n';
            }
#endif
            shipper.push({first_parent, word, count,
                          actual_window_length_in_seconds, false});
        }
        first_parent = {0, 0};
    }

public:
    RollingCounterFunctor(unsigned window_length_in_seconds  = 300,
                          unsigned emit_frequency_in_seconds = 60)
        : window_length_in_seconds {window_length_in_seconds},
          counter {window_length_in_seconds / emit_frequency_in_seconds},
          last_modified_tracker {window_length_in_seconds
                                 / emit_frequency_in_seconds} {}

    void operator()(const Topic &topic, Shipper<Counts> &shipper) {
        if (topic.is_tick_tuple) {
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[ROLLING COUNTER] Received tick tuple at "
                        "time (in miliseconds) "
                     << current_time_msecs() << '\n';
            }
#endif
            ship_all(shipper);
        } else {
            const auto obj = topic.word;
            counter.increment_count(obj);
            if (first_parent.id == 0 && first_parent.timestamp == 0) {
                first_parent = topic.metadata;
            }
        }
    }
};

template<typename InputType,
         void update_rankings(const InputType &, Rankings<string> &)>
class RankerFunctor {
    unsigned         count;
    Rankings<string> rankings;
    TupleMetadata    first_parent {0, 0};

public:
    RankerFunctor(unsigned count = 10) : count {count} {}

    void operator()(const InputType &counts, Shipper<RankingsTuple> &shipper) {
        if (counts.is_tick_tuple) {
            shipper.push({first_parent, rankings, false});
            first_parent = {0, 0};
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[RANKER] Current rankings are " << rankings << '\n';
            }
#endif
        } else {
            update_rankings(counts, rankings);
            if (first_parent.id == 0 && first_parent.timestamp == 0) {
                first_parent = counts.metadata;
            }
        }
    }
};

static inline void update_intermediate_rankings(const Counts &    counts,
                                                Rankings<string> &rankings) {
    // XXX: Is this field relevant?
    // const auto  window_length = counts.window_length;
    Rankable<string> rankable {counts.word, counts.count};
    rankings.update_with(rankable);
}

using IntermediateRankerFunctor =
    RankerFunctor<Counts, update_intermediate_rankings>;

static inline void update_total_rankings(const RankingsTuple &partial_rankings,
                                         Rankings<string> &   total_rankings) {
    total_rankings.update_with(partial_rankings.rankings);
    total_rankings.prune_zero_counts();
}

using TotalRankerFunctor = RankerFunctor<RankingsTuple, update_total_rankings>;

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
    SinkFunctor(unsigned rate = 100) : sampling_rate {rate} {}

    void operator()(optional<RankingsTuple> &input, RuntimeContext &context) {
        if (input) {
            const auto arrival_time = current_time();
            const auto latency =
                difference(arrival_time, input->metadata.timestamp);
            const auto interdeparture_time =
                difference(arrival_time, last_arrival_time);

            ++tuples_received;
            last_arrival_time = arrival_time;

            if (is_time_to_sample(arrival_time)) {
                latency_samples.push_back(latency);
                interdeparture_samples.push_back(interdeparture_time);

                // The current service time is computed via this
                // heuristic, it MIGHT not be reliable.
                const auto service_time =
                    interdeparture_time
                    / static_cast<double>(context.getParallelism());
                service_time_samples.push_back(service_time);
                last_sampling_time = arrival_time;
            }
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[SINK] Received tuple containing the "
                        "following "
                        "rankings: "
                     << input->rankings << ", arrival time: " << arrival_time
                     << " ts:" << input->metadata.timestamp
                     << " latency: " << latency << '\n';
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
                      .withParallelism(parameters.parallelism[source_id])
                      .withName("source")
                      .withOutputBatchSize(parameters.batch_size[source_id])
                      .build();

    TopicExtractorFunctor topic_extractor_functor;
    auto                  topic_extractor_node =
        FlatMap_Builder {topic_extractor_functor}
            .withParallelism(parameters.parallelism[topic_extractor_id])
            .withName("topic extractor")
            .withOutputBatchSize(parameters.batch_size[topic_extractor_id])
            .build();

    RollingCounterTimerFunctor rolling_counter_timer_functor {
        parameters.duration, parameters.rolling_counter_frequency,
        parameters.parallelism[rolling_counter_id]};
    auto rolling_counter_timer_node =
        Source_Builder {rolling_counter_timer_functor}
            .withParallelism(1)
            .withName("rolling counter timer")
            .withOutputBatchSize(0)
            .build();

    RollingCounterFunctor rolling_counter_functor {
        300, parameters.intermediate_ranker_frequency};
    auto rolling_counter_node =
        FlatMap_Builder {rolling_counter_functor}
            .withParallelism(parameters.parallelism[rolling_counter_id])
            .withName("rolling counter")
            .withOutputBatchSize(parameters.batch_size[rolling_counter_id])
            .withKeyBy([](const Topic &topic) -> string { return topic.word; })
            .build();

    IntermediateRankerTimerFunctor intermediate_ranker_timer_functor {
        parameters.duration, parameters.intermediate_ranker_frequency,
        parameters.parallelism[intermediate_ranker_id]};
    auto intermediate_ranker_timer_node =
        Source_Builder {intermediate_ranker_timer_functor}
            .withParallelism(1)
            .withName("intermediate ranker timer")
            .withOutputBatchSize(0)
            .build();

    IntermediateRankerFunctor intermediate_ranker_functor;
    auto                      intermediate_ranker_node =
        FlatMap_Builder {intermediate_ranker_functor}
            .withParallelism(parameters.parallelism[intermediate_ranker_id])
            .withName("intermediate ranker")
            .withOutputBatchSize(parameters.batch_size[intermediate_ranker_id])
            .withKeyBy(
                [](const Counts &count) -> string { return count.word; })
            .build();

    TotalRankerTimerFunctor total_ranker_timer_functor {
        parameters.duration, parameters.total_ranker_frequency,
        parameters.parallelism[total_ranker_id]};
    auto total_ranker_timer_node = Source_Builder {total_ranker_timer_functor}
                                       .withParallelism(1)
                                       .withName("total ranker timer")
                                       .withOutputBatchSize(1)
                                       .build();

    TotalRankerFunctor total_ranker_functor;
    auto               total_ranker_node =
        FlatMap_Builder {total_ranker_functor}
            .withParallelism(parameters.parallelism[total_ranker_id])
            .withName("total ranker")
            .withOutputBatchSize(parameters.batch_size[total_ranker_id])
            .build();

    SinkFunctor sink_functor {parameters.sampling_rate};
    auto        sink = Sink_Builder {sink_functor}
                    .withParallelism(parameters.parallelism[sink_id])
                    .withName("sink")
                    .build();

    if (parameters.use_chaining) {
        auto &topic_extractor_pipe =
            graph.add_source(source).chain(topic_extractor_node);
        auto &rolling_counter_timer_pipe =
            graph.add_source(rolling_counter_timer_node);
        auto &intermediate_ranker_timer_pipe =
            graph.add_source(intermediate_ranker_timer_node);
        auto &total_ranker_timer_pipe =
            graph.add_source(total_ranker_timer_node);

        topic_extractor_pipe.merge(rolling_counter_timer_pipe)
            .chain(rolling_counter_node)
            .merge(intermediate_ranker_timer_pipe)
            .chain(intermediate_ranker_node)
            .merge(total_ranker_timer_pipe)
            .chain(total_ranker_node)
            .chain_sink(sink);
    } else {
        auto &topic_extractor_pipe =
            graph.add_source(source).add(topic_extractor_node);
        auto &rolling_counter_timer_pipe =
            graph.add_source(rolling_counter_timer_node);
        auto &intermediate_ranker_timer_pipe =
            graph.add_source(intermediate_ranker_timer_node);
        auto &total_ranker_timer_pipe =
            graph.add_source(total_ranker_timer_node);

        topic_extractor_pipe.merge(rolling_counter_timer_pipe)
            .add(rolling_counter_node)
            .merge(intermediate_ranker_timer_pipe)
            .add(intermediate_ranker_node)
            .merge(total_ranker_timer_pipe)
            .add(total_ranker_node)
            .add_sink(sink);
    }
    return graph;
}

int main(int argc, char *argv[]) {
    Parameters parameters;
    parse_args(argc, argv, parameters);
    validate_args(parameters);
    print_initial_parameters(parameters);

    PipeGraph graph {"tt-trending-topics", parameters.execution_mode,
                     parameters.time_policy};
    build_graph(parameters, graph);

    const auto start_time = current_time();
    graph.run();
    const auto elapsed_time = difference(current_time(), start_time);

    serialize_to_json(global_latency_metric,
                      parameters.metric_output_directory,
                      global_received_tuples);
    serialize_to_json(global_interdeparture_metric,
                      parameters.metric_output_directory,
                      global_received_tuples);
    serialize_to_json(global_service_time_metric,
                      parameters.metric_output_directory,
                      global_received_tuples);

    const auto average_latency =
        accumulate(global_latency_metric.begin(), global_latency_metric.end(),
                   0.0)
        / (!global_latency_metric.empty() ? global_latency_metric.size()
                                          : 1.0);
    print_statistics(elapsed_time, parameters.duration, global_sent_tuples,
                     average_latency, global_received_tuples);
    return 0;
}
