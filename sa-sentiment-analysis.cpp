/*
 * The dataset is formed by the strings passed as input to the program.  Run as
 * ./example1 <string1> <string2> ...
 */
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

struct SentimentResult {
    Sentiment sentiment;
    double    score;

    SentimentResult() {}
    SentimentResult(Sentiment sentiment, double score)
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
    // TODO: Should take a configuration in input as well...
    BasicClassifier() {}

    SentimentResult classify(const string &input_string) {
        auto text = regex_replace(input_string, regex {"\\p{Punct}|\\n"}, " ");
        transform(text.begin(), text.end(), text.begin(),
                  [](char c) { return tolower(c); });

        const regex          space_regex {" "};
        const vector<string> words {
            sregex_token_iterator(text.begin(), text.end(), space_regex, -1),
            sregex_token_iterator()};

        auto current_tweet_sentiment = 0;

        for (const auto &word : words) {
            if (sentiment_map.find(word) != sentiment_map.end()) {
                current_tweet_sentiment += sentiment_map[word];
            }
        }

        SentimentResult result;
        result.score     = current_tweet_sentiment;
        result.sentiment = current_tweet_sentiment > 0   ? Sentiment::Positive
                           : current_tweet_sentiment < 0 ? Sentiment::Negative
                                                         : Sentiment::Neutral;
        return result;
    }
};

int main(int argc, char *argv[]) {
    return 0;
}
