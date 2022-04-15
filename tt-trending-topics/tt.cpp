#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <wf/windflow.hpp>

using namespace std;
using namespace wf;

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
    size_t        window_length;
};

struct IRankings {
    TupleMetadata  metadata;
    vector<double> rankings;
};

struct TRankings {
    TupleMetadata  metadata;
    vector<double> rankings;
};

inline uint64_t current_time_msecs() __attribute__((always_inline));
inline uint64_t current_time_msecs() {
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return (t.tv_sec) * 1000L + t.tv_nsec;
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

class TopicExtractorFunctor {
    void operator()(const Tweet &tweet, Shipper<Topic> &shipper) {
        const auto words = string_split(tweet.text, " \n\t");
        for (const auto &word : words) {
            if (!word.empty() && word[0] == '#') {
                shipper.push({{0}, word.data()}); // Use empty metadata for now
            }
        }
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
    size_t              windw_length_in_slots;

    size_t slot_after(size_t slot) {
        assert(slot < windw_length_in_slots);
        return (slot + 1) % windw_length_in_slots;
    }

    void advance_head() {
        head_slot = tail_slot;
        tail_slot = slot_after(tail_slot);
    }

public:
    SlidingWindowCounter(size_t windw_length_in_slots)
        : obj_counter {windw_length_in_slots}, head_slot {0},
          tail_slot {slot_after(head_slot)}, windw_length_in_slots {
                                                 windw_length_in_slots} {
        if (windw_length_in_slots < 2) {
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

class RollingCounterFunctor {
    unsigned                     window_length_in_seconds {300};
    unsigned                     emit_frequency_in_seconds {60};
    SlidingWindowCounter<string> counter;
    NthLastModifiedTimeTracker   last_modified_tracker;

    TupleMetadata first_parent {0};

public:
    RollingCounterFunctor()
        : counter {window_length_in_seconds / emit_frequency_in_seconds},
          last_modified_tracker {window_length_in_seconds
                                 / emit_frequency_in_seconds} {}

    void operator()(const Topic &topic, Shipper<Counts> &shipper) {
        const auto obj = topic.word;
        counter.increment_count(obj);

        if (first_parent.id == 0 && first_parent.timestamp == 0) {
            first_parent = topic.metadata;
        }

        // TODO: Need to actually send tuples!!!
    }
};

class IntermediateRankerFunctor {
    // TODO
};

class TotalRankerFunctor {
    // TODO
};
