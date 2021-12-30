#include <atomic>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>
#include <wf/windflow.hpp>

using namespace std;
using namespace wf;

constexpr auto current_time = current_time_usecs;

enum class Sentiment { Positive, Negative, Neutral };

using SentimentResult = pair<Sentiment, int>;

struct SourceTuple {
    string        tweet;
    unsigned long timestamp;
};

struct MapOutputTuple {
    string          tweet;
    SentimentResult result;
    unsigned long   latency;
};

constexpr const char *timeunit_string() {
    return current_time == current_time_usecs   ? "microsecond"
           : current_time == current_time_nsecs ? "nanosecond"
                                                : "time unit";
}

constexpr unsigned long seconds_to_timeunit(unsigned long seconds) {
    return current_time == current_time_usecs   ? seconds * 1000000
           : current_time == current_time_nsecs ? seconds * 1000000000
                                                : seconds;
}

constexpr double timeunit_to_seconds(unsigned long timeunits) {
    return current_time == current_time_usecs   ? timeunits / 1000000.0
           : current_time == current_time_nsecs ? timeunits / 1000000000.0
                                                : timeunits;
}

static inline Sentiment score_to_sentiment(int score) {
    return score > 0   ? Sentiment::Positive
           : score < 0 ? Sentiment::Negative
                       : Sentiment::Neutral;
}

static inline vector<string> read_strings_from_file(const string &path) {
    ifstream       input_file {path};
    vector<string> strings;

    for (string line; getline(input_file, line);) {
        strings.emplace_back(move(line));
    }
    return strings;
}

static inline string string_trim(const string &s) {
    string trimmed_string;

    for (const auto &c : s) {
        if (c != ' ') {
            trimmed_string.push_back(c);
        }
    }
    return trimmed_string;
}

static inline string remove_punctuation(const string &s) {
    string output_string;

    for (const auto &c : s) {
        if (c != '.' && c != ',' && c != '?' && c != '!') {
            output_string.push_back(c);
        }
    }
    return output_string;
}

static inline vector<string> string_split_on_space(const string &s,
                                                   char          delim) {
    const auto     length = s.size();
    vector<string> words;

    for (size_t i = 0; i < length;) {
        string word;
        size_t j = i;

        for (; s[j] != delim && j < length; ++j) {
            word.push_back(s[j]);
        }
        words.emplace_back(move(word));
        i = j + 1;
    }
    return words;
}

static inline string &string_trim_in_place(string &s) {
    for (size_t i = 0; s[i] != '\0';) {
        if (s[i] == ' ') {
            s.erase(i, 1);
        } else {
            ++i;
        }
    }
    return s;
}

static inline vector<string> split_in_words(const string &input) {
    auto text = remove_punctuation(input);

    for (auto &c : text) {
        c = tolower(c);
    }

    auto words = string_split_on_space(text, ' ');
    for (auto &word : words) {
        string_trim_in_place(word);
    }

    return words;
}

void wait(unsigned long duration) {
    auto start_time = current_time();
    auto done       = false;
    while (!done) {
        done = (current_time() - start_time) >= duration;
    }
}

/* Global variables */
atomic_ulong   g_sent_tuples;
atomic<double> g_average_latency;

template<typename Map>
static inline Map get_sentiment_map(const string &path) {
    ifstream                  input_file {path};
    Map                       sentiment_map;
    typename Map::key_type    word;
    typename Map::mapped_type sentiment;

    while (input_file >> word >> sentiment) {
        sentiment_map[word] = sentiment;
    }
    return sentiment_map;
}

class SourceFunctor {
    static constexpr auto default_path = "example-dataset.txt";

    vector<string> dataset;
    unsigned long  duration;

public:
    SourceFunctor(const string &path, unsigned long d)
        : dataset {read_strings_from_file(path)}, duration {
                                                      seconds_to_timeunit(d)} {}

    SourceFunctor(unsigned long d) : SourceFunctor {default_path, d} {}

    SourceFunctor() : SourceFunctor {default_path, 60} {}

    void operator()(Source_Shipper<SourceTuple> &shipper) {
        const auto    end_time = current_time() + duration;
        size_t        index {0};
        unsigned long sent_tuples {0};

        while (current_time() < end_time) {
            shipper.push({dataset[index], current_time()});
            ++sent_tuples;
            index = (index + 1) % dataset.size();
        }
        g_sent_tuples.store(sent_tuples);
    }
};

class BasicClassifier {
    static constexpr auto      default_path = "AFINN-111.txt";
    unordered_map<string, int> sentiment_map;

public:
    BasicClassifier(const string &path)
        : sentiment_map {get_sentiment_map<decltype(sentiment_map)>(path)} {}
    BasicClassifier() : BasicClassifier {default_path} {}

    SentimentResult classify(const string &tweet) {
        const auto words                   = split_in_words(tweet);
        auto       current_tweet_sentiment = 0;

        for (const auto &word : words) {
            if (sentiment_map.find(word) != sentiment_map.end()) {
                current_tweet_sentiment += sentiment_map[word];
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
    MapFunctor(const string &path) : classifier {path} {}

    MapOutputTuple operator()(const SourceTuple &tuple) {
        const auto result = classifier.classify(tuple.tweet);
        return {tuple.tweet, result, current_time() - tuple.timestamp};
    }
};

static inline const char *sentiment_to_string(Sentiment sentiment) {
    return sentiment == Sentiment::Positive   ? "Positive"
           : sentiment == Sentiment::Negative ? "Negative"
                                              : "Neutral";
}

class SinkFunctor {
    static constexpr auto verbose_output = false;
    unsigned long         tuples_received {0};
    unsigned long         total_average {0};

public:
    void operator()(optional<MapOutputTuple> &input) {
        if (input) {
            ++tuples_received;
            total_average += input->latency;

            if constexpr (verbose_output) {
                cout << "Received tweet \"" << input->tweet << "\" with score "
                     << input->result.second << " and classification "
                     << sentiment_to_string(input->result.first) << "\n";
            }
        } else {
            g_average_latency.store(total_average / (double) tuples_received);
        }
    }
};

static inline void parse_and_validate_args(int argc, char **argv,
                                           unsigned long &duration,
                                           unsigned int & map_parallelism,
                                           bool &         use_chaining) {
    int option;
    while ((option = getopt(argc, argv, "t:m:c:")) != -1) {
        switch (option) {
        case 't':
            duration = atol(optarg);
            break;
        case 'm':
            map_parallelism = atoi(optarg);
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
                 << " [-c true|false] -t <duration> -m <parallelism>\n";
            exit(EXIT_FAILURE);
        }
    }

    if (duration <= 0) {
        cerr << "Error: duration is not positive\n";
        exit(EXIT_FAILURE);
    }

    if (map_parallelism <= 0) {
        cerr << "Error: Map parallelism degree is not positive\n";
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char *argv[]) {
    auto use_chaining    = false;
    auto map_parallelism = 0u;
    auto duration        = 0ul;

    parse_and_validate_args(argc, argv, duration, map_parallelism,
                            use_chaining);
    SourceFunctor source_functor {duration};
    auto          source = Source_Builder {source_functor}
                      .withParallelism(1)
                      .withName("source")
                      .build();

    MapFunctor<BasicClassifier> map_functor;
    auto                        classifier_node = Map_Builder {map_functor}
                               .withParallelism(map_parallelism)
                               .withName("counter")
                               .build();

    SinkFunctor sink_functor;
    auto        sink =
        Sink_Builder {sink_functor}.withParallelism(1).withName("sink").build();

    PipeGraph graph {"sa-sentiment-analysis", Execution_Mode_t::DEFAULT,
                     Time_Policy_t::INGRESS_TIME};
    if (use_chaining) {
        graph.add_source(source).chain(classifier_node).chain_sink(sink);
    } else {
        graph.add_source(source).add(classifier_node).add_sink(sink);
    }
    const auto start_time = current_time();
    graph.run();

    const auto elapsed_time = current_time() - start_time;
    const auto sent_tuples  = g_sent_tuples.load();
    const auto throughput =
        elapsed_time > 0 ? sent_tuples / (double) elapsed_time : sent_tuples;
    const auto service_time = 1 / throughput;

    cout << "Elapsed time: " << elapsed_time << ' ' << timeunit_string()
         << "s (" << timeunit_to_seconds(elapsed_time) << " seconds)\n";
    cout << "Total number of tuples sent: " << sent_tuples << " \n";
    cout << "Processed about " << throughput << " tuples per "
         << timeunit_string() << '\n';
    cout << "Service time: " << service_time << ' ' << timeunit_string()
         << "s\n";
    cout << "Average latency: " << g_average_latency.load() << ' '
         << timeunit_string() << "s\n";
    return 0;
}
