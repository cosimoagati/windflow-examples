/*
 * Copyright (C) 2021-2022 Cosimo Agati
 *
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * You should have received a copy of the GNU AGPLv3 with this software,
 * if not, please visit <https://www.gnu.org/licenses/>
 */

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <fstream>
#include <getopt.h>
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <rapidjson/prettywriter.h>
#include <string>
#include <string_view>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>
#include <wf/windflow.hpp>

using namespace std;
using namespace nlohmann;
using namespace rapidjson;
using namespace wf;

enum class Sentiment { Positive, Negative, Neutral };

struct SentimentResult {
    Sentiment sentiment;
    int       score;
};

struct Tuple {
    string          tweet;
    SentimentResult result;
    unsigned long   timestamp;
};

template<typename T>
class AtomicVector {
    vector<T> internal_vector;
    mutex     vector_mutex;

public:
    void push_back(const T &element) {
        const lock_guard lock {vector_mutex};
        internal_vector.push_back(element);
    }

    void push_back(T &&element) {
        const lock_guard lock {vector_mutex};
        internal_vector.push_back(element);
    }

    const vector<T> &data() const {
        return internal_vector;
    }
};

constexpr auto current_time = current_time_nsecs;

const auto timeunit_string = current_time == current_time_usecs ? "microsecond"
                             : current_time == current_time_nsecs ? "nanosecond"
                                                                  : "time unit";

const auto timeunit_scale_factor =
    current_time == current_time_usecs   ? 1000000ul
    : current_time == current_time_nsecs ? 1000000000ul
                                         : 1ul;

const struct option long_opts[] = {
    {"help", 0, 0, 'h'},        {"rate", 1, 0, 'r'},  {"sampling", 1, 0, 's'},
    {"parallelism", 1, 0, 'p'}, {"batch", 1, 0, 'b'}, {"chaining", 1, 0, 'c'},
    {"duration", 1, 0, 'd'},    {0, 0, 0, 0}};

/*
 * Return an appropriate Sentiment value based on its numerical score.
 */
static inline Sentiment score_to_sentiment(int score) {
    return score > 0   ? Sentiment::Positive
           : score < 0 ? Sentiment::Negative
                       : Sentiment::Neutral;
}

#ifndef NDEBUG
/*
 * Return a string literal representation of a tweet sentiment.
 */
static inline const char *sentiment_to_string(Sentiment sentiment) {
    return sentiment == Sentiment::Positive   ? "Positive"
           : sentiment == Sentiment::Negative ? "Negative"
                                              : "Neutral";
}
#endif

static inline vector<size_t> get_parallelism_degrees(const char *degrees) {
    const string   degree_string {degrees};
    stringstream   stream {degree_string};
    vector<size_t> parallelism_degrees;
    for (size_t i; stream >> i;) {
        parallelism_degrees.push_back(i);
        if (stream.peek() == ',') {
            stream.ignore();
        }
    }
    return parallelism_degrees;
}

/*
 * Return a vector of strings each containing a line from the file found in
 * path.
 */
static inline vector<string> read_strings_from_file(const char *path) {
    ifstream       input_file {path};
    vector<string> strings;

    for (string line; getline(input_file, line);) {
        strings.emplace_back(move(line));
    }
    return strings;
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

template<typename T>
static inline vector<T> concatenate_vectors(const vector<vector<T>> &vectors) {
    vector<T> merged_vector;
    size_t    total_size {0};
    for (const auto &v : vectors) {
        total_size += v.size();
    }
    merged_vector.reserve(total_size);
    for (const auto &v : vectors) {
        merged_vector.insert(merged_vector.end(), v.begin(), v.end());
    }
    return merged_vector;
}

static inline bool is_punctuation(char c) {
    return c == '.' || c == ',' || c == '?' || c == '!' || c == ':';
}

/*
 * Replaces punctuation characters with a space. The input string s itself is
 * modified.
 * Return a reference to s.
 */
static inline string &replace_punctuation_with_spaces_in_place(string &s) {
    replace_if(s.begin(), s.end(), is_punctuation, ' ');
    return s;
}

/*
 * Convert all characters of string s to lowercase, modifying s in place.
 * Return a reference to s.
 */
static inline string &lowercase_in_place(string &s) {
    for (auto &c : s) {
        c = tolower(c);
    }
    return s;
}

/*
 * Return a std::vector of std::string_views, each representing the "words" in a
 * tweet.  The input string may be modified.
 */
static inline vector<string_view> split_in_words_in_place(string &text) {
    replace_punctuation_with_spaces_in_place(text);
    lowercase_in_place(text);
    return string_split(text, ' ');
}

template<typename Map>
static inline Map get_sentiment_map(const char *path) {
    const hash<string> gethash;
    ifstream           input_file {path};
    Map                sentiment_map;
    string             word;
    int                sentiment;

    while (input_file >> word >> sentiment) {
        sentiment_map[gethash(word)] = sentiment;
    }
    return sentiment_map;
}

static inline void
parse_and_validate_args(int argc, char **argv, unsigned &duration,
                        unsigned &tuple_rate, unsigned &sampling_rate,
                        unsigned &source_parallelism, unsigned &map_parallelism,
                        unsigned &sink_parallelism, unsigned &batch_size,
                        bool &use_chaining) {
    int option;
    int index;

    while (
        (option = getopt_long(argc, argv, "r:s:p:b:c:d:h", long_opts, &index))
        != -1) {
        switch (option) {
        case 'r':
            tuple_rate = atoi(optarg);
            break;
        case 's':
            sampling_rate = atoi(optarg);
            break;
        case 'b':
            batch_size = atoi(optarg);
            break;
        case 'p': {
            const auto degrees = get_parallelism_degrees(optarg);
            if (degrees.size() != 3) {
                cerr << "Error in parsing the input arguments.  Parallelism "
                        "degree string requires exactly three elements.\n";
                exit(EXIT_FAILURE);
            } else {
                source_parallelism = degrees[0];
                map_parallelism    = degrees[1];
                sink_parallelism   = degrees[2];
            }
            break;
        }
        case 'c':
            use_chaining = atoi(optarg) > 0 ? true : false;
            break;
        case 'd':
            duration = atoi(optarg);
            break;
        case 'h':
            cout << "Parameters: --rate <value> --sampling "
                    "<value> --batch <size> --parallelism "
                    "<nSource,nPredictor,nSink> [--duration <seconds>] "
                    "[--chaining <value>]\n";
            exit(EXIT_SUCCESS);
        default:
            cerr << "Error in parsing the input arguments.  Use the --help "
                    "(-h) option for usage information.\n";
            exit(EXIT_FAILURE);
        }

        if (duration == 0) {
            cerr << "Error: duration must be positive\n";
            exit(EXIT_FAILURE);
        }
        if (source_parallelism == 0 || map_parallelism == 0
            || sink_parallelism == 0) {
            cerr << "Error: parallelism degree must be positive\n";
            exit(EXIT_FAILURE);
        }
    }
}

void print_initial_parameters(unsigned source_parallelism,
                              unsigned map_parallelism,
                              unsigned sink_parallelism, unsigned batch_size,
                              unsigned duration, unsigned tuple_rate,
                              unsigned sampling_rate, bool use_chaining) {
    cout << "Running graph with the following parameters:\n"
         << "Source parallelism: " << source_parallelism << '\n'
         << "Classifier parallelism: " << map_parallelism << '\n'
         << "Sink parallelism: " << sink_parallelism << '\n'
         << "Batching: ";
    if (batch_size > 0) {
        cout << batch_size << '\n';
    } else {
        cout << "None\n";
    }

    cout << "Duration: " << duration << " seconds\n"
         << "Tuple generation rate: ";
    if (tuple_rate > 0) {
        cout << tuple_rate << " tuples per second\n";
    } else {
        cout << "unlimited (BEWARE OF QUEUE CONGESTION)\n";
    }

    cout << "Sampling rate: ";
    if (sampling_rate > 0) {
        cout << sampling_rate << " measurements per second\n";
    } else {
        cout << "unlimited (sample every incoming tuple)\n";
    }

    cout << "Chaining: " << (use_chaining ? "enabled" : "disabled") << '\n';
}

static inline void
print_statistics(unsigned long elapsed_time, unsigned long duration,
                 unsigned long sent_tuples, unsigned long cumulative_latency,
                 unsigned long received_tuples, unsigned long sampled_tuples) {
    const auto elapsed_time_in_seconds =
        elapsed_time / (double) timeunit_scale_factor;

    const auto throughput =
        elapsed_time > 0 ? sent_tuples / (double) elapsed_time : sent_tuples;

    const auto throughput_in_seconds   = throughput * timeunit_scale_factor;
    const auto service_time            = 1 / throughput;
    const auto service_time_in_seconds = service_time / timeunit_scale_factor;
    const auto average_latency = cumulative_latency / (double) sampled_tuples;
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

void dump_metric(const char *name, vector<unsigned long> &samples,
                 unsigned long total_measurements) {
    StringBuffer                          buffer;
    PrettyWriter<rapidjson::StringBuffer> writer(buffer);

    writer.StartObject();

    writer.Key("name");
    writer.String(name);

    writer.Key("time unit");
    const auto plural_timeunit_string = string {timeunit_string} + 's';
    writer.String(plural_timeunit_string.c_str());

    writer.Key("sampled measurements");
    writer.Uint(samples.size());

    writer.Key("total measurements");
    writer.Uint(total_measurements);

    if (!samples.empty()) {
        writer.Key("mean");
        const auto cumulative_latency =
            accumulate(samples.begin(), samples.end(), 0.0);
        writer.Double(cumulative_latency / (double) samples.size());

        const auto   minmax = minmax_element(samples.begin(), samples.end());
        const double min    = *minmax.first;
        const double max    = *minmax.second;

        writer.Key("0");
        writer.Double(min);

        // add percentiles
        for (const auto percentile : {0.05, 0.25, 0.5, 0.75, 0.95}) {
            const auto pointer = samples.begin() + samples.size() * percentile;
            nth_element(samples.begin(), pointer, samples.end());
            const auto label = to_string(static_cast<int>(percentile * 100));
            writer.Key(label.c_str());
            writer.Double((double) *pointer);
        }
        writer.Key("100");
        writer.Double(max);
    } else {
        writer.Key("mean");
        writer.Double(0.0);

        for (const auto percentile : {"0", "25", "50", "75", "95", "100"}) {
            writer.Key(percentile);
            writer.Double(0.0);
        }
    }
    writer.EndObject();

    const auto filename = string {"metric-"} + name + ".json";
    ofstream   fs {filename};
    fs << buffer.GetString() << '\n';
}

/*
 * Suspend execution for an amount of time units specified by duration.
 */
void busy_wait(unsigned long duration) {
    const auto start_time = current_time();
    while (current_time() - start_time < duration)
        ;
}

/* Global variables */
atomic_ulong                        global_sent_tuples {0};
atomic_ulong                        global_cumulative_latency {0};
atomic_ulong                        global_received_tuples {0};
AtomicVector<vector<unsigned long>> global_latency_samples;

class SourceFunctor {
    static constexpr auto default_path = "example-dataset.txt";
    vector<string>        dataset;
    unsigned long         duration;
    unsigned              tuple_rate_per_second;

public:
    SourceFunctor(unsigned long d = 60, unsigned rate = 1000,
                  const char *path = default_path)
        : dataset {read_strings_from_file(path)},
          tuple_rate_per_second {rate}, duration {d * timeunit_scale_factor} {}

    void operator()(Source_Shipper<Tuple> &shipper) {
        const auto end_time    = current_time() + duration;
        auto       sent_tuples = 0ul;
        size_t     index       = 0;

        while (current_time() < end_time) {
            auto       tweet     = dataset[index];
            const auto timestamp = current_time();
            shipper.push({move(tweet), SentimentResult {}, timestamp});
            ++sent_tuples;
            index = (index + 1) % dataset.size();

            if (tuple_rate_per_second > 0) {
                const unsigned long delay =
                    (1.0 / tuple_rate_per_second) * timeunit_scale_factor;
                busy_wait(delay);
            }
        }
        global_sent_tuples.fetch_add(sent_tuples);
    }
};

class JsonSourceFunctor {
    static constexpr auto default_path = "twitterexample.json";

    json          json_map;
    unsigned long duration;

public:
    JsonSourceFunctor(unsigned long d = 60, const char *path = default_path)
        : duration {d * timeunit_scale_factor} {
        ifstream file {path};
        file >> json_map;
    }
    void operator()(Source_Shipper<Tuple> &shipper) {
        const auto end_time    = current_time() + duration;
        auto       sent_tuples = 0ul;

        while (current_time() < end_time) {
            auto       tweet     = json_map["text"];
            const auto timestamp = current_time();
            shipper.push({move(tweet), SentimentResult {}, timestamp});
            ++sent_tuples;
        }
        global_sent_tuples.fetch_add(sent_tuples);
    }
};

class BasicClassifier {
    static constexpr auto             default_path = "AFINN-111.txt";
    hash<string_view>                 gethash;
    unordered_map<unsigned long, int> sentiment_map;

public:
    BasicClassifier(const char *path = default_path)
        : sentiment_map {get_sentiment_map<decltype(sentiment_map)>(path)} {}

    SentimentResult classify(string &tweet) {
        const auto words                   = split_in_words_in_place(tweet);
        auto       current_tweet_sentiment = 0;

        for (const auto &word : words) {
            const auto word_hash = gethash(word);
            if (sentiment_map.find(word_hash) != sentiment_map.end()) {
                current_tweet_sentiment += sentiment_map[word_hash];
            }
        }
        return {score_to_sentiment(current_tweet_sentiment),
                current_tweet_sentiment};
    }
};

class CachingClassifier {
    static constexpr auto                  default_path = "AFINN-111.txt";
    hash<string_view>                      gethash;
    unordered_map<unsigned long, int>      sentiment_map;
    unordered_map<string, SentimentResult> result_cache;

public:
    CachingClassifier(const char *path = default_path)
        : sentiment_map {get_sentiment_map<decltype(sentiment_map)>(path)} {}

    SentimentResult classify(string &tweet) {
        const auto cached_result = result_cache.find(tweet);

        if (cached_result != result_cache.end()) {
            return cached_result->second;
        }
        auto &     result_cache_entry      = result_cache[tweet];
        const auto words                   = split_in_words_in_place(tweet);
        auto       current_tweet_sentiment = 0;

        for (const auto &word : words) {
            const auto word_hash = gethash(word);
            if (sentiment_map.find(word_hash) != sentiment_map.end()) {
                current_tweet_sentiment += sentiment_map[word_hash];
            }
        }
        result_cache_entry = {score_to_sentiment(current_tweet_sentiment),
                              current_tweet_sentiment};
        return result_cache_entry;
    }
};

template<typename Classifier>
class MapFunctor {
    Classifier classifier;

public:
    MapFunctor() = default;
    MapFunctor(const char *path) : classifier {path} {}

    void operator()(Tuple &tuple) {
        tuple.result = classifier.classify(tuple.tweet);
    }
};

class SinkFunctor {
#ifndef NDEBUG
    inline static mutex print_mutex {};
#endif
    vector<unsigned long> latency_samples;
    unsigned long         tuples_received;
    unsigned long         cumulative_latency;
    unsigned long         last_sampling_time;
    unsigned              latency_sampling_rate;

    bool is_time_to_sample(unsigned long arrival_time) {
        const auto time_since_last_sampling = arrival_time - last_sampling_time;
        const auto time_between_samples =
            (1.0 / latency_sampling_rate) * timeunit_scale_factor;
        return latency_sampling_rate == 0
               || time_since_last_sampling >= time_between_samples;
    }

public:
    SinkFunctor(unsigned rate = 100)
        : latency_samples {}, tuples_received {0}, cumulative_latency {0},
          last_sampling_time {current_time()}, latency_sampling_rate {rate} {}

    void operator()(optional<Tuple> &input) {
        if (input) {
            const auto arrival_time = current_time();
            const auto latency      = arrival_time - input->timestamp;
            ++tuples_received;

            if (is_time_to_sample(arrival_time)) {
                cumulative_latency += latency;
                latency_samples.push_back(latency);
                last_sampling_time = arrival_time;
            }
#ifndef NDEBUG
            const lock_guard lock {print_mutex};
            cout << "arrival time: " << arrival_time
                 << " ts:" << input->timestamp << " latency: " << latency
                 << '\n'
                 << "Received tweet with score " << input->result.score
                 << " and classification "
                 << sentiment_to_string(input->result.sentiment) << endl;
#endif
        } else {
            global_cumulative_latency.fetch_add(cumulative_latency);
            global_received_tuples.fetch_add(tuples_received);
            global_latency_samples.push_back(move(latency_samples));
        }
    }
};

static inline PipeGraph &
build_graph(bool use_chaining, unsigned duration, unsigned tuple_rate,
            unsigned sampling_rate, unsigned source_parallelism,
            unsigned map_parallelism, unsigned sink_parallelism,
            unsigned batch_size, PipeGraph &graph) {
    SourceFunctor source_functor {duration, tuple_rate};
    auto          source = Source_Builder {source_functor}
                      .withParallelism(source_parallelism)
                      .withName("source")
                      .withOutputBatchSize(batch_size)
                      .build();

    MapFunctor<BasicClassifier> map_functor;
    auto                        classifier_node = Map_Builder {map_functor}
                               .withParallelism(map_parallelism)
                               .withName("classifier")
                               .withOutputBatchSize(batch_size)
                               .build();

    SinkFunctor sink_functor {sampling_rate};
    auto        sink = Sink_Builder {sink_functor}
                    .withParallelism(sink_parallelism)
                    .withName("sink")
                    .build();

    if (use_chaining) {
        graph.add_source(source).chain(classifier_node).chain_sink(sink);
    } else {
        graph.add_source(source).add(classifier_node).add_sink(sink);
    }
    return graph;
}

int main(int argc, char *argv[]) {
    auto source_parallelism = 1u;
    auto map_parallelism    = 1u;
    auto sink_parallelism   = 1u;
    auto batch_size         = 0u;
    auto duration           = 60u;
    auto tuple_rate         = 1000u;
    auto sampling_rate      = 100u;
    auto use_chaining       = false;

    parse_and_validate_args(argc, argv, duration, tuple_rate, sampling_rate,
                            source_parallelism, map_parallelism,
                            sink_parallelism, batch_size, use_chaining);
    print_initial_parameters(source_parallelism, map_parallelism,
                             sink_parallelism, batch_size, duration, tuple_rate,
                             sampling_rate, use_chaining);

    PipeGraph graph {"sa-sentiment-analysis", Execution_Mode_t::DEFAULT,
                     Time_Policy_t::INGRESS_TIME};
    build_graph(use_chaining, duration, tuple_rate, sampling_rate,
                source_parallelism, map_parallelism, sink_parallelism,
                batch_size, graph);

    const auto start_time = current_time();
    graph.run();
    const auto elapsed_time = current_time() - start_time;

    auto latency_samples = concatenate_vectors(global_latency_samples.data());
    print_statistics(elapsed_time, duration, global_sent_tuples.load(),
                     global_cumulative_latency.load(),
                     global_received_tuples.load(), latency_samples.size());
    dump_metric("latency", latency_samples, global_received_tuples.load());
    return 0;
}
