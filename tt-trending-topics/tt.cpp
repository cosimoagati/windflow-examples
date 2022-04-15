#include <algorithm>
#include <string>
#include <string_view>
#include <vector>
#include <wf/windflow.hpp>

using namespace std;
using namespace wf;

struct Tweet {
    string        id;
    string        text;
    unsigned long timestamp;
};

struct Topic {
    string word;
};

struct Counts {
    string word;
    size_t window_length;
};

struct IRankings {
    vector<double> rankings;
};

struct TRankings {
    vector<double> rankings;
};

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
                shipper.push({word.data()});
            }
        }
    }
};

class RollingCounterFunctor {
    // TODO
};

class IntermediateRankerFunctor {
    // TODO
};

class TotalRankerFunctor {
    // TODO
};
