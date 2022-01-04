#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>
#include <string_view>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>
#include <wf/windflow.hpp>

using namespace std;
using namespace nlohmann;
using namespace wf;

constexpr auto current_time = current_time_usecs;

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

/*
 * Return a string representation of the current time unit.
 */
constexpr const char *timeunit_string() {
    return current_time == current_time_usecs   ? "microsecond"
           : current_time == current_time_nsecs ? "nanosecond"
                                                : "time unit";
}

/*
 * Return how many of the chosen time units fit in a second.
 */
constexpr unsigned long timeunit_scale_factor() {
    return current_time == current_time_usecs   ? 1000000
           : current_time == current_time_nsecs ? 1000000000
                                                : 1;
}

/*
 * Return an appropriate Sentiment value based on its numerical score.
 */
static inline Sentiment score_to_sentiment(int score) {
    return score > 0   ? Sentiment::Positive
           : score < 0 ? Sentiment::Negative
                       : Sentiment::Neutral;
}

/*
 * Return a string literal representation of a tweet sentiment.
 */
static inline const char *sentiment_to_string(Sentiment sentiment) {
    return sentiment == Sentiment::Positive   ? "Positive"
           : sentiment == Sentiment::Negative ? "Negative"
                                              : "Neutral";
}

/*
 * Return a vector of strings each containing a line from the file found in
 * path.
 */
static inline vector<string> read_strings_from_file(const string &path) {
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
    constexpr auto      max_words_in_tweet = 280;
    auto                word_begin         = 0;
    vector<string_view> words;

    words.reserve(max_words_in_tweet);
    for (auto i = 0; i < s.length(); ++i) {
        if (s[word_begin] == delim) {
            ++word_begin;
        } else if (s[i] == delim) {
            words.emplace_back(s.data() + word_begin, i - word_begin);
            word_begin = i + 1;
        }
    }
    if (s[word_begin] != delim) {
        words.emplace_back(s.data() + word_begin, s.length() - word_begin);
    }
    return words;
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
 * Return a std::vector of std::strings, each representing the "words" in a
 * tweet.
 * The input string may be modified.
 */
static inline vector<string_view> split_in_words_in_place(string &text) {
    replace_punctuation_with_spaces_in_place(text);
    lowercase_in_place(text);
    return string_split(text, ' ');
}

template<typename Map>
static inline Map get_sentiment_map(const string &path) {
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

static inline void parse_and_validate_args(int argc, char **argv,
                                           unsigned long &duration,
                                           unsigned &     source_parallelism,
                                           unsigned int & map_parallelism,
                                           unsigned &     sink_parallelism,
                                           bool &         use_chaining) {
    int option;
    while ((option = getopt(argc, argv, "t:m:c:s:k:")) != -1) {
        switch (option) {
        case 't':
            duration = atol(optarg);
            break;
        case 's':
            source_parallelism = atoi(optarg);
            break;
        case 'm':
            map_parallelism = atoi(optarg);
            break;
        case 'k':
            sink_parallelism = atoi(optarg);
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
                 << " [-c true|false] -t <duration> -s <source parallelism> -m "
                    "<map parallelism>\n";
            exit(EXIT_FAILURE);
        }
    }

    if (duration <= 0) {
        cerr << "Error: duration is not positive\n";
        exit(EXIT_FAILURE);
    }

    if (source_parallelism <= 0) {
        cerr << "Error: Source parallelism degree is not positive\n";
        exit(EXIT_FAILURE);
    }

    if (map_parallelism <= 0) {
        cerr << "Error: Map parallelism degree is not positive\n";
        exit(EXIT_FAILURE);
    }

    if (sink_parallelism <= 0) {
        cerr << "Error: Sink parallelism degree is not positive\n";
        exit(EXIT_FAILURE);
    }
}

static inline void print_statistics(unsigned long elapsed_time,
                                    unsigned long sent_tuples,
                                    unsigned long latency) {
    const auto elapsed_time_in_seconds =
        elapsed_time / (double) timeunit_scale_factor();

    const auto throughput =
        elapsed_time > 0 ? sent_tuples / (double) elapsed_time : sent_tuples;

    const auto throughput_in_seconds   = throughput * timeunit_scale_factor();
    const auto service_time            = 1 / throughput;
    const auto service_time_in_seconds = service_time / timeunit_scale_factor();
    const auto latency_in_seconds = latency / (double) timeunit_scale_factor();

    cout << "Elapsed time: " << elapsed_time << ' ' << timeunit_string()
         << "s (" << elapsed_time_in_seconds << " seconds)\n";
    cout << "Total number of tuples sent: " << sent_tuples << " \n";
    cout << "Processed about " << throughput << " tuples per "
         << timeunit_string() << " (" << throughput_in_seconds
         << " tuples per second)\n";
    cout << "Service time: " << service_time << ' ' << timeunit_string()
         << "s (" << service_time_in_seconds << " seconds)\n";
    cout << "Average latency: " << latency << ' ' << timeunit_string() << "s ("
         << latency_in_seconds << " seconds)\n";
}

/* Global variables */
atomic_ulong   g_sent_tuples;
atomic<double> g_average_latency;

class SourceFunctor {
    static constexpr auto default_path = "example-dataset.txt";

    vector<string> dataset;
    unsigned long  duration;

public:
    SourceFunctor(const string &path, unsigned long d)
        : dataset {read_strings_from_file(path)},
          duration {d * timeunit_scale_factor()} {}

    SourceFunctor(unsigned long d) : SourceFunctor {default_path, d} {}

    SourceFunctor() : SourceFunctor {default_path, 60} {}

    void operator()(Source_Shipper<Tuple> &shipper) {
        const auto    end_time = current_time() + duration;
        size_t        index {0};
        unsigned long sent_tuples {0};

        while (current_time() < end_time) {
            auto       tweet     = dataset[index];
            const auto timestamp = current_time();
            shipper.push({move(tweet), SentimentResult {}, timestamp});
            ++sent_tuples;
            index = (index + 1) % dataset.size();
        }
        g_sent_tuples.fetch_add(sent_tuples);
    }
};

class JsonSourceFunctor {
    static constexpr auto default_path = "twitterexample.json";

    json          json_map;
    unsigned long duration;

public:
    JsonSourceFunctor(const string &path, unsigned long d)
        : duration {d * timeunit_scale_factor()} {
        ifstream file {path};
        file >> json_map;
    }

    JsonSourceFunctor(unsigned long d) : JsonSourceFunctor {default_path, d} {}

    JsonSourceFunctor() : JsonSourceFunctor {default_path, 60} {}

    void operator()(Source_Shipper<Tuple> &shipper) {
        const auto    end_time = current_time() + duration;
        unsigned long sent_tuples {0};

        while (current_time() < end_time) {
            auto       tweet     = json_map["text"];
            const auto timestamp = current_time();
            shipper.push({move(tweet), SentimentResult {}, timestamp});
            ++sent_tuples;
        }
        g_sent_tuples.store(sent_tuples);
    }
};

class BasicClassifier {
    static constexpr auto             default_path = "AFINN-111.txt";
    hash<string_view>                 gethash;
    unordered_map<unsigned long, int> sentiment_map;

public:
    BasicClassifier(const string &path)
        : sentiment_map {get_sentiment_map<decltype(sentiment_map)>(path)} {}
    BasicClassifier() : BasicClassifier {default_path} {}

    SentimentResult classify(string &tweet) {
        const auto words                   = split_in_words_in_place(tweet);
        auto       current_tweet_sentiment = 0;

        for (const auto &word : words) {
            if (sentiment_map.find(gethash(word)) != sentiment_map.end()) {
                current_tweet_sentiment += sentiment_map[gethash(word)];
            }
        }
        return {score_to_sentiment(current_tweet_sentiment),
                current_tweet_sentiment};
    }
};

class CachingClassifier {
    static constexpr auto             default_path = "AFINN-111.txt";
    hash<string_view>                 gethash;
    unordered_map<unsigned long, int> sentiment_map;
    unordered_map<unsigned long, SentimentResult> result_cache;

public:
    CachingClassifier(const string &path)
        : sentiment_map {get_sentiment_map<decltype(sentiment_map)>(path)} {}
    CachingClassifier() : CachingClassifier {default_path} {}

    SentimentResult classify(string &tweet) {
        const auto cached_result = result_cache.find(gethash(tweet));
        if (cached_result != result_cache.end()) {
            return cached_result->second;
        }
        auto &     result_cache_entry      = result_cache[gethash(tweet)];
        const auto words                   = split_in_words_in_place(tweet);
        auto       current_tweet_sentiment = 0;

        for (const auto &word : words) {
            if (sentiment_map.find(gethash(word)) != sentiment_map.end()) {
                current_tweet_sentiment += sentiment_map[gethash(word)];
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
    MapFunctor(const string &path) : classifier {path} {}

    void operator()(Tuple &tuple) {
        tuple.result = classifier.classify(tuple.tweet);
    }
};

class SinkFunctor {
    static constexpr auto verbose_output = false;
    unsigned long         tuples_received {0};
    unsigned long         cumulative_latency {0};

public:
    void operator()(optional<Tuple> &input) {
        if (input) {
            ++tuples_received;
            cumulative_latency += current_time() - input->timestamp;

            if constexpr (verbose_output) {
                cout << "Received tweet with score " << input->result.score
                     << " and classification "
                     << sentiment_to_string(input->result.sentiment) << "\n";
            }
        } else {
            g_average_latency.store(cumulative_latency
                                    / (double) tuples_received);
        }
    }
};

static inline PipeGraph &build_graph(bool use_chaining, unsigned long duration,
                                     unsigned   source_parallelism,
                                     unsigned   map_parallelism,
                                     unsigned   sink_parallelism,
                                     PipeGraph &graph) {
    SourceFunctor source_functor {duration};
    auto          source = Source_Builder {source_functor}
                      .withParallelism(source_parallelism)
                      .withName("source")
                      .build();

    MapFunctor<BasicClassifier> map_functor;
    auto                        classifier_node = Map_Builder {map_functor}
                               .withParallelism(map_parallelism)
                               .withName("classifier")
                               .build();

    SinkFunctor sink_functor;
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
    auto use_chaining       = false;
    auto source_parallelism = 1u;
    auto map_parallelism    = 1u;
    auto sink_parallelism   = 1u;
    auto duration           = 60ul;

    parse_and_validate_args(argc, argv, duration, source_parallelism,
                            map_parallelism, sink_parallelism, use_chaining);

    PipeGraph graph {"sa-sentiment-analysis", Execution_Mode_t::DEFAULT,
                     Time_Policy_t::INGRESS_TIME};
    build_graph(use_chaining, duration, source_parallelism, map_parallelism,
                sink_parallelism, graph);
    const auto start_time = current_time();
    graph.run();
    const auto elapsed_time = current_time() - start_time;

    print_statistics(elapsed_time, g_sent_tuples.load(),
                     g_average_latency.load());
    return 0;
}
