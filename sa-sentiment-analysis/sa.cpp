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
#include <unistd.h>
#include <utility>
#include <vector>
#include <wf/windflow.hpp>

using namespace std;
using namespace std::chrono;
using namespace wf;

atomic_ulong g_sent_tuples;

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

struct SentimentResult {
    Sentiment sentiment;
    int       score;
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
    using NumTuples                    = decltype(g_sent_tuples.load());
    static constexpr auto default_path = "example-dataset.txt";

    vector<string> dataset;
    NumTuples      total_tuples;

public:
    SourceFunctor(const string &path, NumTuples t)
        : dataset {read_strings_from_file(path)}, total_tuples {t} {}

    SourceFunctor(NumTuples t) : SourceFunctor {default_path, t} {}

    SourceFunctor() : SourceFunctor {default_path, 1000} {}

    void operator()(Source_Shipper<string> &shipper) {
        size_t index {0};
        while (g_sent_tuples < total_tuples) {
            shipper.push(dataset[index]);
            ++g_sent_tuples;
            index = (index + 1) % dataset.size();
        }
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

template<typename Classifier>
class MapFunctor {
    Classifier classifier;

public:
    MapFunctor() = default;
    MapFunctor(const string &path) : classifier {path} {}

    pair<string, SentimentResult> operator()(const string &input_string) {
        const auto result = classifier.classify(input_string);
        return make_pair(input_string, result);
    }
};

static const char *sentiment_to_string(Sentiment sentiment) {
    return sentiment == Sentiment::Positive   ? "Positive"
           : sentiment == Sentiment::Negative ? "Negative"
                                              : "Neutral";
}

static void do_sink(optional<pair<string, SentimentResult>> &input) {
    if (input) {
        // cout << "Received tweet \"" << input->first << "\" with score "
        //      << input->second.score << " and classification "
        //      << sentiment_to_string(input->second.sentiment) << "\n";
    } else {
        cout << "End of stream\n\n";
    }
}

template<typename TimeUnit>
static inline void parse_and_validate_args(int argc, char **argv,
                                           TimeUnit &duration) {
    int option;
    while ((option = getopt(argc, argv, "d:")) != -1) {
        switch (option) {
        case 'd':
            duration = TimeUnit {atoi(optarg)};
            break;
        default:
            cerr << "Error: invalid option\n";
            exit(EXIT_FAILURE);
        }
    }

    if (duration <= TimeUnit {0}) {
        cerr << "Error: duration not provided or not positive\n";
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char *argv[]) {
    const auto start_time   = steady_clock::now();
    const auto use_chaining = false;
    seconds    duration {0};

    parse_and_validate_args(argc, argv, duration);

    SourceFunctor source_functor {duration};
    auto          source = Source_Builder {source_functor}
                      .withParallelism(1)
                      .withName("source")
                      .build();

    MapFunctor<BasicClassifier> map_functor;
    auto                        classifier_node =
        Map_Builder {map_functor}
            .withParallelism(1)
            .withName("counter")
            .withKeyBy([](const string &word) { return word; })
            .build();

    auto sink =
        Sink_Builder {do_sink}.withParallelism(1).withName("sink").build();

    PipeGraph graph {"sa-sentiment-analysis", Execution_Mode_t::DEFAULT,
                     Time_Policy_t::INGRESS_TIME};
    if (use_chaining) {
        graph.add_source(source).chain(classifier_node).chain_sink(sink);
    } else {
        graph.add_source(source).add(classifier_node).add_sink(sink);
    }
    graph.run();

    const auto elapsed_time =
        duration_cast<decltype(duration)>(steady_clock::now() - start_time);
    cout << elapsed_time.count() << '\n';
    const auto throughput = g_sent_tuples / elapsed_time.count();

    cout << "Sent " << throughput << " tuples per "
         << timeunit_to_string<decltype(duration)>;
    return 0;
}
