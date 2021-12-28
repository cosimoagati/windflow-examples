#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <iostream>
#include <map>
#include <regex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <wf/windflow.hpp>

using namespace std;
using namespace wf;

enum class Sentiment { Positive, Negative, Neutral };

static inline Sentiment score_to_sentiment(int score) {
    return score > 0   ? Sentiment::Positive
           : score < 0 ? Sentiment::Negative
                       : Sentiment::Neutral;
}

static inline vector<string> split_in_words(const string &input) {
    auto text = regex_replace(input, regex {"\\p{Punct}|\\n"}, " ");

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

struct SentimentResult {
    Sentiment sentiment;
    int       score;

    SentimentResult() {}
    SentimentResult(Sentiment sentiment, int score)
        : sentiment {sentiment}, score {score} {}
};

class SourceFunctor {
    vector<string> dataset;

public:
    SourceFunctor(const vector<string> &dataset) : dataset {dataset} {}

    void operator()(Source_Shipper<string> &shipper) {
        for (const auto &line : dataset) {
            shipper.push(line);
        }
    }
};

class BasicClassifier {
    static constexpr auto default_path = "sentimentanalysis/AFINN-111.txt";
    map<string, int>      sentiment_map;

public:
    BasicClassifier(const char *path) {}
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
        cout << "Received word " << input->first << " with score "
             << input->second.score << " and classification "
             << sentiment_to_string(input->second.sentiment) << "\n";
    } else {
        cout << "End of stream\n" << endl;
    }
}

int main(int argc, char *argv[]) {
    const auto           use_chaining = false;
    const vector<string> dataset;

    SourceFunctor source_functor {dataset};
    auto          source = Source_Builder(source_functor)
                      .withParallelism(1)
                      .withName("source")
                      .build();

    MapFunctor<BasicClassifier> map_functor;
    auto                        classifier_node =
        Map_Builder(map_functor)
            .withParallelism(1)
            .withName("counter")
            .withKeyBy(
                [](const std::string &word) -> std::string { return word; })
            .build();

    auto sink =
        Sink_Builder(do_sink).withParallelism(1).withName("sink").build();

    PipeGraph graph {"filtered_wc", Execution_Mode_t::DEFAULT,
                     Time_Policy_t::INGRESS_TIME};
    if (use_chaining) {
        graph.add_source(source).add(classifier_node).chain_sink(sink);
    } else {
        graph.add_source(source).add(classifier_node).add_sink(sink);
    }
    graph.run();

    return 0;
}
