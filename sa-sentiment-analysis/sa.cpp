#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <regex>
#include <string>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>
#include <wf/windflow.hpp>

using namespace std;
using namespace std::chrono;
using namespace wf;

atomic_ulong g_sent_tuples;
atomic_ulong g_average;

template<typename TimeUnit>
constexpr auto timeunit_to_string = "time unit";
template<>
constexpr auto timeunit_to_string<milliseconds> = "millisecond";
template<>
constexpr auto timeunit_to_string<seconds> = "second";
template<>
constexpr auto timeunit_to_string<microseconds> = "microsecond";
template<>
constexpr auto timeunit_to_string<nanoseconds> = "nanosecond";

enum class Sentiment { Positive, Negative, Neutral };

using SentimentResult = pair<Sentiment, int>;

struct SourceTuple {
    string                   tweet;
    time_point<steady_clock> timestamp;
};

template<typename TimeUnit>
struct MapOutputTuple {
    string          tweet;
    SentimentResult result;
    TimeUnit        latency;
};

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

static inline vector<string> split_in_words(const string &input) {
    // auto text = regex_replace(input, regex {"\\p{Punct}|\\n"}, " ");
    auto text = input;

    transform(text.begin(), text.end(), text.begin(),
              [](char c) { return tolower(c); });

    const auto is_not_space = [](char c) { return !isspace(c); };
    text.erase(text.begin(), find_if(text.begin(), text.end(), is_not_space));
    text.erase(find_if(text.rbegin(), text.rend(), is_not_space).base(),
               text.end());

    const regex space_regex {" "};
    return {sregex_token_iterator {text.begin(), text.end(), space_regex, -1},
            sregex_token_iterator {}};
}

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
    unsigned long  total_tuples;

public:
    SourceFunctor(const string &path, unsigned long t)
        : dataset {read_strings_from_file(path)}, total_tuples {t} {}

    SourceFunctor(unsigned long t) : SourceFunctor {default_path, t} {}

    SourceFunctor() : SourceFunctor {default_path, 1000} {}

    void operator()(Source_Shipper<SourceTuple> &shipper) {
        size_t        index {0};
        unsigned long sent_tuples {0};

        while (sent_tuples < total_tuples) {
            shipper.push({dataset[index], steady_clock::now()});
            ++sent_tuples;
            index = (index + 1) % dataset.size();
        }
        g_sent_tuples.store(sent_tuples);
    }
};

class BasicClassifier {
    static constexpr auto default_path = "AFINN-111.txt";
    map<string, int>      sentiment_map;

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

template<typename Classifier, typename TimeUnit>
class MapFunctor {
    Classifier classifier;

public:
    MapFunctor() = default;
    MapFunctor(const string &path) : classifier {path} {}

    MapOutputTuple<TimeUnit> operator()(const SourceTuple &tuple) {
        const auto result = classifier.classify(tuple.tweet);
        return {tuple.tweet, result,
                duration_cast<TimeUnit>(steady_clock::now() - tuple.timestamp)};
    }
};

static inline const char *sentiment_to_string(Sentiment sentiment) {
    return sentiment == Sentiment::Positive   ? "Positive"
           : sentiment == Sentiment::Negative ? "Negative"
                                              : "Neutral";
}

template<typename TimeUnit>
class SinkFunctor {
    unsigned long          tuples_received {0};
    typename TimeUnit::rep total {0};
    typename TimeUnit::rep average;

public:
    void operator()(optional<MapOutputTuple<TimeUnit>> &input) {
        if (input) {
            ++tuples_received;
            total += input->latency.count();
            average = total / tuples_received;

            // cout << "Received tweet \"" << input->first << "\" with score "
            //      << input->second.second << " and classification "
            //      << sentiment_to_string(input->second.first) << "\n";
        } else {
            cout << "End of stream\n\n";
            cout << "Average latency is " << average << ' '
                 << timeunit_to_string<TimeUnit> << "s\n";
        }
    }
};

static inline void parse_and_validate_args(int argc, char **argv,
                                           unsigned long &total_tuples,
                                           unsigned int & map_parallelism,
                                           bool &         use_chaining) {
    int option;
    while ((option = getopt(argc, argv, "t:m:c:")) != -1) {
        switch (option) {
        case 't':
            total_tuples = atol(optarg);
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
            cerr << "Error: invalid option\n";
            exit(EXIT_FAILURE);
        }
    }

    if (total_tuples <= 0) {
        cerr << "Error: number of tuples is not positive\n";
        exit(EXIT_FAILURE);
    }

    if (map_parallelism <= 0) {
        cerr << "Error: Map parallelism degree is not positive\n";
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char *argv[]) {
    using TimeUnit = microseconds;

    const auto start_time      = steady_clock::now();
    auto       use_chaining    = false;
    auto       map_parallelism = 0u;
    auto       total_tuples    = 0ul;

    parse_and_validate_args(argc, argv, total_tuples, map_parallelism,
                            use_chaining);
    SourceFunctor source_functor {total_tuples};
    auto          source = Source_Builder {source_functor}
                      .withParallelism(1)
                      .withName("source")
                      .build();

    MapFunctor<BasicClassifier, TimeUnit> map_functor;
    auto classifier_node = Map_Builder {map_functor}
                               .withParallelism(map_parallelism)
                               .withName("counter")
                               .build();

    SinkFunctor<TimeUnit> sink_functor;
    auto                  sink =
        Sink_Builder {sink_functor}.withParallelism(1).withName("sink").build();

    PipeGraph graph {"sa-sentiment-analysis", Execution_Mode_t::DEFAULT,
                     Time_Policy_t::INGRESS_TIME};
    if (use_chaining) {
        graph.add_source(source).chain(classifier_node).chain_sink(sink);
    } else {
        graph.add_source(source).add(classifier_node).add_sink(sink);
    }
    graph.run();

    const auto elapsed_time =
        duration_cast<TimeUnit>(steady_clock::now() - start_time);
    const double throughput =
        elapsed_time.count() > 0
            ? g_sent_tuples.load() / (double) elapsed_time.count()
            : g_sent_tuples.load();

    cout << "Elapsed time: " << elapsed_time.count() << ' '
         << timeunit_to_string<TimeUnit> << "s\n";
    cout << "Processed " << throughput << " tuples per "
         << timeunit_to_string<TimeUnit> << '\n';
    return 0;
}
