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
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <getopt.h>
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
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
    source_id     = 0,
    classifier_id = 1,
    sink_id       = 2,
    num_nodes     = 3
};

struct Parameters {
    const char *     metric_output_directory   = ".";
    Execution_Mode_t execution_mode            = Execution_Mode_t::DEFAULT;
    Time_Policy_t    time_policy               = Time_Policy_t::INGRESS_TIME;
    unsigned         parallelism[num_nodes]    = {1, 1, 1};
    unsigned         batch_size[num_nodes - 1] = {0, 0};
    unsigned         duration                  = 60;
    unsigned         tuple_rate                = 0;
    unsigned         sampling_rate             = 100;
    bool             use_chaining              = false;
};

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
    switch (sentiment) {
    case Sentiment::Positive:
        return "Positive";
    case Sentiment::Negative:
        return "Negative";
    case Sentiment::Neutral:
        return "Neutral";
    default:
        cerr << "sentiment_to_string:  invalid sentiment value\n";
        exit(EXIT_FAILURE);
        break;
    }
    return "UNREACHABLE"; // Make the compiler happy
}
#endif

/*
 * Replaces non-alphanumeric characters with a space. The input string s
 * itself is modified. Return a reference to s.
 */
static inline string &replace_non_alnum_with_spaces_in_place(string &s) {
    constexpr auto is_not_alnum = [](char c) { return !isalnum(c); };
    replace_if(s.begin(), s.end(), is_not_alnum, ' ');
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
 * Return a std::vector of std::string_views, each representing the "words"
 * in a tweet.  The input string may be modified.
 */
static inline vector<string_view> split_in_words_in_place(string &text) {
    replace_non_alnum_with_spaces_in_place(text);
    lowercase_in_place(text);
    return string_split(text, ' ');
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

template<typename Map>
static inline Map get_sentiment_map(const char *path) {
    const hash<string_view> gethash;
    ifstream                input_file {path};
    Map                     sentiment_map;
    string                  line;

    while (input_file.good() && getline(input_file, line)) {
        const auto line_fields = string_split(line, '\t');
        assert(line_fields.size() == 2);

        const auto sentiment     = stoi(string {line_fields.back()});
        const auto word_hash     = gethash(line_fields.front());
        sentiment_map[word_hash] = sentiment;
    }
    return sentiment_map;
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
                    "<nSource,nClassifier,nSink> [--duration <seconds>] "
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

    const unsigned max_threads = thread::hardware_concurrency();

    for (size_t i = 0; i < num_nodes; ++i) {
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
         << "Classifier parallelism:\t"
         << parameters.parallelism[classifier_id] << '\n'
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

/*
 * Global variables
 */
static atomic_ulong          global_sent_tuples {0};
static atomic_ulong          global_received_tuples {0};
static Metric<unsigned long> global_latency_metric {"sa-latency"};
#ifndef NDEBUG
static mutex print_mutex;
#endif

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

    void operator()(Source_Shipper<Tuple> &shipper) {
        const unsigned long end_time    = current_time() + duration;
        unsigned long       sent_tuples = 0;
        size_t              index       = 0;

        while (current_time() < end_time) {
            const auto &tweet = tweets[index];
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[SOURCE] Sending the following tweet: " << tweet
                     << '\n';
            }
#endif
            const unsigned long timestamp = current_time();
            shipper.push({tweet, SentimentResult {}, timestamp});
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

class BasicClassifier {
    static constexpr auto             default_path = "AFINN-111.txt";
    hash<string_view>                 gethash;
    unordered_map<unsigned long, int> sentiment_map;

public:
    BasicClassifier(const char *path = default_path)
        : sentiment_map {get_sentiment_map<decltype(sentiment_map)>(path)} {}

    SentimentResult classify(string &tweet) {
        const auto words                   = split_in_words_in_place(tweet);
        int        current_tweet_sentiment = 0;

        for (const auto &word : words) {
            const unsigned long word_hash = gethash(word);
            const auto sentiment_entry    = sentiment_map.find(word_hash);
            if (sentiment_entry != sentiment_map.end()) {
#ifndef NDEBUG
                {
                    lock_guard lock {print_mutex};
                    clog << "[BASIC CLASSIFIER] Current word: "
                         << sentiment_entry->first
                         << ", with score: " << sentiment_entry->second
                         << '\n';
                }
#endif
                current_tweet_sentiment += sentiment_entry->second;
            }
        }
        return {score_to_sentiment(current_tweet_sentiment),
                current_tweet_sentiment};
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
    vector<unsigned long> latency_samples;
    vector<unsigned long> service_time_samples;
    unsigned long         tuples_received    = 0;
    unsigned long         last_sampling_time = current_time();
    unsigned long         last_arrival_time  = last_sampling_time;
    unsigned              sampling_rate;

    bool is_time_to_sample(unsigned long arrival_time) {
        if (sampling_rate == 0) {
            return true;
        }
        const unsigned long time_since_last_sampling =
            difference(arrival_time, last_sampling_time);
        const unsigned long time_between_samples =
            (1.0 / sampling_rate) * timeunit_scale_factor;
        return time_since_last_sampling >= time_between_samples;
    }

public:
    SinkFunctor(unsigned rate) : sampling_rate {rate} {}

    void operator()(optional<Tuple> &input) {
        if (input) {
            const unsigned long arrival_time = current_time();
            const unsigned long latency =
                difference(arrival_time, input->timestamp);

            ++tuples_received;
            last_arrival_time = arrival_time;
            if (is_time_to_sample(arrival_time)) {
                latency_samples.push_back(latency);
                last_sampling_time = arrival_time;
            }
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[SINK] arrival time: " << arrival_time
                     << " ts:" << input->timestamp << " latency: " << latency
                     << ", received tweet with score " << input->result.score
                     << " and classification "
                     << sentiment_to_string(input->result.sentiment)
                     << "with contents after trimming: " << input->tweet
                     << '\n';
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
    const auto    source =
        Source_Builder {source_functor}
            .withParallelism(parameters.parallelism[source_id])
            .withName("source")
            .withOutputBatchSize(parameters.batch_size[source_id])
            .build();

    MapFunctor<BasicClassifier> map_functor;
    const auto                  classifier_node =
        Map_Builder {map_functor}
            .withParallelism(parameters.parallelism[classifier_id])
            .withName("classifier")
            .withOutputBatchSize(parameters.batch_size[classifier_id])
            .build();

    SinkFunctor sink_functor {parameters.sampling_rate};
    const auto  sink = Sink_Builder {sink_functor}
                          .withParallelism(parameters.parallelism[sink_id])
                          .withName("sink")
                          .build();

    if (parameters.use_chaining) {
        graph.add_source(source).chain(classifier_node).chain_sink(sink);
    } else {
        graph.add_source(source).add(classifier_node).add_sink(sink);
    }
    return graph;
}

int main(int argc, char *argv[]) {
    Parameters parameters;
    parse_args(argc, argv, parameters);
    validate_args(parameters);
    print_initial_parameters(parameters);

    PipeGraph graph {"sa-sentiment-analysis", parameters.execution_mode,
                     parameters.time_policy};
    build_graph(parameters, graph);

    const unsigned long start_time = current_time();
    graph.run();
    const unsigned long elapsed_time = difference(current_time(), start_time);
    const double        throughput =
        elapsed_time > 0
                   ? (global_sent_tuples.load() / static_cast<double>(elapsed_time))
                   : global_sent_tuples.load();
    const double service_time = 1 / throughput;

    const auto latency_stats = get_distribution_stats(
        global_latency_metric, parameters, global_received_tuples);
    serialize_json(latency_stats, "sa-latency",
                   parameters.metric_output_directory);

    const auto throughput_stats = get_single_value_stats(
        throughput, "throughput", parameters, global_sent_tuples.load());
    serialize_json(throughput_stats, "sa-throughput",
                   parameters.metric_output_directory);

    const auto service_time_stats = get_single_value_stats(
        service_time, "service time", parameters, global_sent_tuples.load());
    serialize_json(service_time_stats, "sa-service-time",
                   parameters.metric_output_directory);

    const double average_latency =
        accumulate(global_latency_metric.begin(), global_latency_metric.end(),
                   0.0)
        / (!global_latency_metric.empty() ? global_latency_metric.size()
                                          : 1.0);
    print_statistics(elapsed_time, parameters.duration, global_sent_tuples,
                     average_latency, global_received_tuples);
    return 0;
}
