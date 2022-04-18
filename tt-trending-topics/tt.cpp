#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <getopt.h>
#include <mutex>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <wf/windflow.hpp>

using namespace std;
using namespace wf;

constexpr auto current_time = current_time_nsecs;

const auto timeunit_string = current_time == current_time_usecs ? "microsecond"
                             : current_time == current_time_nsecs
                                 ? "nanosecond"
                                 : "time unit";

const auto timeunit_scale_factor =
    current_time == current_time_usecs   ? 1000000ul
    : current_time == current_time_nsecs ? 1000000000ul
                                         : 1ul;

const struct option long_opts[] = {
    {"help", 0, 0, 'h'},        {"rate", 1, 0, 'r'},  {"sampling", 1, 0, 's'},
    {"parallelism", 1, 0, 'p'}, {"batch", 1, 0, 'b'}, {"chaining", 1, 0, 'c'},
    {"duration", 1, 0, 'd'},    {0, 0, 0, 0}};

struct Parameters {
    unsigned source_parallelism {1};
    unsigned topic_extractor_parallelism {1};
    unsigned rolling_counter_parallelism {1};
    unsigned intermediate_ranker_parallelism {1};
    unsigned total_ranker_parallelism {1};
    unsigned sink_parallelism {1};
    unsigned batch_size {0};
    unsigned duration {60};
    unsigned tuple_rate {1000};
    unsigned sampling_rate {100};
    bool     use_chaining {false};
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
};

struct Counts {
    TupleMetadata metadata;
    string        word;
    unsigned long count;
    size_t        window_length;
};

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
};

struct RankingsTuple {
    TupleMetadata    metadata;
    Rankings<string> rankings;
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
template<typename T>
class SlotBasedCounter {
    unordered_map<T, vector<unsigned long>> counts_map;
    size_t                                  num_slots;

    unsigned long compute_total_count(T obj) {
        if (counts_map.find(obj) == counts_map.end()) {
            return 0;
        }

        const auto &  curr_obj_counts = counts_map[obj];
        unsigned long total {0};
        for (const auto count : curr_obj_counts) {
            total += count;
        }
        return total;
    }

    void reset_slot_count_to_zero(T obj, unsigned slot) {
        counts_map[obj][slot] = 0;
    }

    bool should_be_removed_from_counter(T obj) {
        return compute_total_count(obj) == 0;
    }

public:
    SlotBasedCounter(size_t num_slots) : num_slots {num_slots} {}

    void increment_count(T obj, size_t slot, unsigned long increment) {
        assert(slot < num_slots);

        // XXX: this should work as expected, but double check!
        if (counts_map.find(obj) == counts_map.end()) {
            counts_map[obj].reserve(num_slots);
        }
        counts_map[obj][slot] += increment;
    }

    void increment_count(T obj, size_t slot) {
        increment_count(obj, slot, 1);
    }

    unsigned long get_count(T obj, size_t slot) {
        assert(slot < num_slots);

        if (counts_map.find(obj) == counts_map.end()) {
            return 0;
        }
        return counts_map[obj][slot];
    }

    unordered_map<T, unsigned long> get_counts() {
        unordered_map<T, unsigned long> result;

        for (const auto &kv : counts_map) {
            result[kv.first] = compute_total_count(kv.first);
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
    size_t              head_slot;
    size_t              tail_slot;
    size_t              window_length_in_slots;

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
        : obj_counter {window_length_in_slots}, head_slot {0},
          tail_slot {slot_after(head_slot)}, window_length_in_slots {
                                                 window_length_in_slots} {
        if (window_length_in_slots < 2) {
            cerr << "Error: Window length for sliding window counter must be "
                    "at least two\n";
            exit(EXIT_FAILURE);
        }
    }

    void increment_count(T obj) {
        obj_counter.increment_count(obj, head_slot);
    }

    void increment_count(T obj, unsigned long increment) {
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

template<typename T>
class CircularFifoBuffer {
    vector<T> buffer;
    size_t    head {0};

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
        buffer[head] = element;
        head         = (head + 1) % buffer.size();
    }

    const T &get() const {
        return buffer[head];
    }

    T &get() {
        return buffer[head];
    }
};

inline uint64_t current_time_msecs() __attribute__((always_inline));
inline uint64_t current_time_msecs() {
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return (t.tv_sec) * 1000UL + (t.tv_nsec / 1000000UL);
}

class NthLastModifiedTimeTracker {
    static constexpr auto             millis_in_sec = 1000u;
    CircularFifoBuffer<unsigned long> last_modified_times_millis;

    void init_last_modified_times_millis() {
        const auto now_cached = current_time_msecs();
        for (size_t i {0}; i < last_modified_times_millis.max_size(); ++i) {
            last_modified_times_millis.add(now_cached);
        }
    }

    void update_last_modified_time() {
        last_modified_times_millis.add(current_time_msecs());
    }

public:
    NthLastModifiedTimeTracker(size_t num_times_to_track)
        : last_modified_times_millis {num_times_to_track} {
        if (num_times_to_track < 1) {
            cerr << "Error: num_times_to_track must be positive\n";
            exit(EXIT_FAILURE);
        }
        init_last_modified_times_millis();
    }

    unsigned seconds_since_oldest_modification() {
        const auto modified_time_millis = last_modified_times_millis.get();
        return (unsigned) (current_time_msecs() - modified_time_millis);
    }

    void mark_as_modified() {
        update_last_modified_time();
    }
};

/*
 * Return difference between a and b, accounting for unsigned arithmetic
 * wraparound.
 */
unsigned long difference(unsigned long a, unsigned long b) {
    return max(a, b) - min(a, b);
}

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
    return tweets;
}

static inline vector<size_t> get_parallelism_degrees(const char *degrees) {
    vector<size_t> parallelism_degrees;
    for (const auto &s : string_split(degrees, ",")) {
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
            if (degrees.size() != 6) {
                cerr << "Error in parsing the input arguments.  Parallelism "
                        "degree string requires exactly three elements.\n";
                exit(EXIT_FAILURE);
            } else {
                parameters.source_parallelism              = degrees[0];
                parameters.topic_extractor_parallelism     = degrees[1];
                parameters.rolling_counter_parallelism     = degrees[2];
                parameters.intermediate_ranker_parallelism = degrees[3];
                parameters.total_ranker_parallelism        = degrees[4];
                parameters.sink_parallelism                = degrees[5];
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

void validate_args(const Parameters &parameters) {
    if (parameters.duration == 0) {
        cerr << "Error: duration must be positive\n";
        exit(EXIT_FAILURE);
    }

    const auto max_threads = thread::hardware_concurrency();

    if (parameters.source_parallelism == 0
        || parameters.topic_extractor_parallelism == 0
        || parameters.rolling_counter_parallelism == 0
        || parameters.intermediate_ranker_parallelism == 0
        || parameters.total_ranker_parallelism == 0
        || parameters.sink_parallelism == 0) {
        cerr << "Error: parallelism degree must be positive\n";
        exit(EXIT_FAILURE);
    }

    if (parameters.source_parallelism > max_threads) {
        cerr << "Error: source parallelism is too high\n";
        exit(EXIT_FAILURE);
    }

    if (parameters.topic_extractor_parallelism > max_threads) {
        cerr << "Error: topic extractor parallelism is too high\n";
        exit(EXIT_FAILURE);
    }

    if (parameters.rolling_counter_parallelism > max_threads) {
        cerr << "Error: rolling counter parallelism is too high\n";
        exit(EXIT_FAILURE);
    }

    if (parameters.intermediate_ranker_parallelism > max_threads) {
        cerr << "Error: intermediate ranker parallelism is too high\n";
        exit(EXIT_FAILURE);
    }

    if (parameters.total_ranker_parallelism > max_threads) {
        cerr << "Error: total ranker parallelism is too high\n";
        exit(EXIT_FAILURE);
    }

    if (parameters.sink_parallelism > max_threads) {
        cerr << "Error: sink parallelism is too high\n";
        exit(EXIT_FAILURE);
    }
}

void print_initial_parameters(const Parameters &parameters) {
    cout << "Running graph with the following parameters:\n"
         << "Source parallelism: " << parameters.source_parallelism << '\n'
         << "Topic extractor parallelism: "
         << parameters.topic_extractor_parallelism << '\n'
         << "Rolling counter parallelism: "
         << parameters.rolling_counter_parallelism << '\n'
         << "Intermediate ranker parallelism: "
         << parameters.intermediate_ranker_parallelism << '\n'
         << "Total ranker parallelism: " << parameters.total_ranker_parallelism
         << '\n'
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

class SourceFunctor {
    static constexpr auto default_path = "tweetstream.jsonl";
    vector<string>        tweets;
    unsigned long         duration;
    unsigned              tuple_rate_per_second;

public:
    SourceFunctor(unsigned d = 60, unsigned rate = 60,
                  const char *path = default_path)
        : tweets {get_tweets_from_file(path)},
          duration {d * timeunit_scale_factor}, tuple_rate_per_second {rate} {}

    void operator()(Source_Shipper<Tweet> &shipper) {
        const auto end_time    = current_time() + duration;
        auto       sent_tuples = 0ul;
        size_t     index       = 0;

        while (current_time() < end_time) {
            auto       tweet     = tweets[index];
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
                    shipper.push({tweet.metadata, string {word}});
                }
            }
        }
    }
};

class RollingCounterFunctor {
    unsigned                     window_length_in_seconds;
    unsigned                     emit_frequency_in_milliseconds;
    unsigned long                last_shipping_time;
    SlidingWindowCounter<string> counter;
    NthLastModifiedTimeTracker   last_modified_tracker;

    TupleMetadata first_parent {0};

    void ship_all(Shipper<Counts> &shipper) {
        const auto counts = counter.get_counts_then_advance_window();
        const auto actual_window_length_in_seconds =
            last_modified_tracker.seconds_since_oldest_modification();

        for (const auto &kv : counts) {
            const auto &word  = kv.first;
            const auto  count = kv.second;
            shipper.push(
                {first_parent, word, count, actual_window_length_in_seconds});
        }
        first_parent = {0};
    }

public:
    RollingCounterFunctor(unsigned window_length_in_seconds  = 300,
                          unsigned emit_frequency_in_seconds = 60)
        : window_length_in_seconds {window_length_in_seconds},
          emit_frequency_in_milliseconds {emit_frequency_in_seconds * 1000u},
          last_shipping_time {current_time_msecs()},
          counter {window_length_in_seconds / emit_frequency_in_seconds},
          last_modified_tracker {window_length_in_seconds
                                 / emit_frequency_in_seconds} {}

    void operator()(const Topic &topic, Shipper<Counts> &shipper) {
        const auto obj = topic.word;
        counter.increment_count(obj);

        if (first_parent.id == 0 && first_parent.timestamp == 0) {
            first_parent = topic.metadata;
        }

        auto time_diff = current_time_msecs() - last_shipping_time;
        while (time_diff >= emit_frequency_in_milliseconds) {
            ship_all(shipper);
            last_shipping_time = current_time_msecs();
            time_diff -= emit_frequency_in_milliseconds;
        }
    }
};

template<typename InputType,
         void update_rankings(const InputType &, Rankings<string> &)>
class RankerFunctor {
    static constexpr auto DEFAULT_COUNT = 10u;

    unsigned         emit_frequency_in_milliseconds;
    unsigned long    last_shipping_time;
    unsigned         count;
    Rankings<string> rankings;
    TupleMetadata    first_parent {0};

public:
    RankerFunctor(unsigned count                          = DEFAULT_COUNT,
                  unsigned emit_frequency_in_milliseconds = 60)
        : count {count}, emit_frequency_in_milliseconds {
                             emit_frequency_in_milliseconds} {}

    void operator()(const InputType &counts, Shipper<RankingsTuple> &shipper) {
        update_rankings(counts, rankings);

        if (first_parent.id == 0 && first_parent.timestamp == 0) {
            first_parent = counts.metadata;
        }

        auto time_diff = current_time_msecs() - last_shipping_time;
        while (time_diff >= emit_frequency_in_milliseconds) {
            shipper.push({counts.metadata, rankings});
            last_shipping_time = current_time_msecs();
            time_diff -= emit_frequency_in_milliseconds;
        }
        first_parent = {0};
    }
};

void update_intermediate_rankings(const Counts &    counts,
                                  Rankings<string> &rankings) {
    // XXX: Is this field relevant?
    // const auto  window_length = counts.window_length;
    Rankable<string> rankable {counts.word, counts.count};
    rankings.update_with(rankable);
}

using IntermediateRankerFunctor =
    RankerFunctor<Counts, update_intermediate_rankings>;

void update_total_rankings(const RankingsTuple &partial_rankings,
                           Rankings<string> &   total_rankings) {
    total_rankings.update_with(partial_rankings.rankings);
}

using TotalRankerFunctor = RankerFunctor<RankingsTuple, update_total_rankings>;

class SinkFunctor {
#ifndef NDEBUG
    inline static mutex print_mutex {};
#endif
    vector<unsigned long> latency_samples {};
    vector<unsigned long> interdeparture_samples {};
    vector<unsigned long> service_time_samples {};
    unsigned long         tuples_received {0};
    unsigned long         last_sampling_time {current_time()};
    unsigned long         last_arrival_time {last_sampling_time};
    unsigned              sampling_rate;

    bool is_time_to_sample(unsigned long arrival_time) {
        const auto time_since_last_sampling =
            difference(arrival_time, last_sampling_time);
        const auto time_between_samples =
            (1.0 / sampling_rate) * timeunit_scale_factor;
        return sampling_rate == 0
               || time_since_last_sampling >= time_between_samples;
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
                 << " ts:" << input->metadata.timestamp
                 << " latency: " << latency << '\n';
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

    TopicExtractorFunctor topic_extractor_functor;
    auto                  topic_extractor_node =
        FlatMap_Builder {topic_extractor_functor}
            .withParallelism(parameters.topic_extractor_parallelism)
            .withName("topic extractor")
            .withOutputBatchSize(parameters.batch_size)
            .build();

    RollingCounterFunctor rolling_counter_functor;
    auto                  rolling_counter_node =
        FlatMap_Builder {rolling_counter_functor}
            .withParallelism(parameters.rolling_counter_parallelism)
            .withName("rolling counter")
            .withOutputBatchSize(parameters.batch_size)
            .withKeyBy([](const Topic &topic) -> string { return topic.word; })
            .build();

    IntermediateRankerFunctor intermediate_ranker_functor;
    auto                      intermediate_ranker_node =
        FlatMap_Builder {intermediate_ranker_functor}
            .withParallelism(parameters.intermediate_ranker_parallelism)
            .withName("intermediate ranker")
            .withOutputBatchSize(parameters.batch_size)
            .withKeyBy(
                [](const Counts &count) -> string { return count.word; })
            .build();

    TotalRankerFunctor total_ranker_functor;
    auto               total_ranker_node =
        FlatMap_Builder {total_ranker_functor}
            .withParallelism(parameters.total_ranker_parallelism)
            .withName("total ranker")
            .withOutputBatchSize(parameters.batch_size)
            .build();

    SinkFunctor sink_functor {parameters.sampling_rate};
    auto        sink = Sink_Builder {sink_functor}
                    .withParallelism(parameters.sink_parallelism)
                    .withName("sink")
                    .build();

    if (parameters.use_chaining) {
        graph.add_source(source)
            .chain(topic_extractor_node)
            .chain(rolling_counter_node)
            .chain(intermediate_ranker_node)
            .chain(total_ranker_node)
            .chain_sink(sink);
    } else {
        graph.add_source(source)
            .add(topic_extractor_node)
            .add(rolling_counter_node)
            .add(intermediate_ranker_node)
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

    PipeGraph graph {"tt-trending-topics", Execution_Mode_t::DEFAULT,
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