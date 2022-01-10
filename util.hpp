#ifndef UTIL_HPP
#define UTIL_HPP

#include <algorithm>
#include <string>
#include <vector>
#include <windflow.hpp>

/* util.hpp
 *
 * This header file contains some miscellaneous reusable utilities.
 */

/*
 * Suspend execution for an amount of time units specified by duration.
 */
template<unsigned long current_time()>
void wait(unsigned long duration) {
    auto start_time = current_time();
    auto done       = false;
    while (!done) {
        done = (current_time() - start_time) >= duration;
    }
}

/*
 * Remove punctuation from the given std::string.
 * A new std::string object is returned, the original string is unmodified.
 */
static inline std::string remove_punctuation(const std::string &s) {
    std::string output_string;

    for (const auto &c : s) {
        if (c != '.' && c != ',' && c != '?' && c != '!' && c != ':') {
            output_string.push_back(c);
        }
    }
    return output_string;
}

static inline std::string &string_trim_in_place(std::string &s) {
    for (size_t i = 0; s[i] != '\0';) {
        if (s[i] == ' ') {
            s.erase(i, 1);
        } else {
            ++i;
        }
    }
    return s;
}

/*
 * Return a std::vector of std::strings, obtained from splitting the original
 * string by the delim character.
 * The original string is unmodifierd.
 */
static inline std::vector<std::string> string_split(const std::string &s,
                                                    char               delim) {
    const auto               length = s.size();
    std::vector<std::string> words;

    for (size_t i = 0; i < length;) {
        std::string word;
        size_t      j = i;

        for (; s[j] != delim && j < length; ++j) {
            word.push_back(s[j]);
        }
        if (!word.empty()) {
            words.emplace_back(move(word));
        }
        i = j + 1;
    }
    return words;
}

/*
 * Return a std::vector of std::strings each representing the "words" in a
 * tweet.
 * The input string is unmodified.
 */
static inline std::vector<std::string>
split_in_words(const std::string &input) {
    auto text = remove_punctuation(input);

    for (auto &c : text) {
        c = tolower(c);
    }

    auto words = string_split(text, ' ');
    for (auto &word : words) {
        string_trim_in_place(word);
    }

    return words;
}
/*
 * Return a vector of strings each containing a line from the file found in
 * path.
 */
static inline std::vector<std::string>
read_strings_from_file(const char *path) {
    std::ifstream            input_file {path};
    std::vector<std::string> strings;

    for (std::string line; getline(input_file, line);) {
        strings.emplace_back(move(line));
    }
    return strings;
}

/*
 * Removes space characters from a string, in place.
 * Return a reference to the string itself.
 */
static inline std::string &string_trim_in_place_algorithm(std::string &s) {
    s.erase(remove(s.begin(), s.end(), ' '), s.end());
    return s;
}

/*
 * Return a std::vector containing all the elements in the vectors contained in
 * the "vectors" parameters.  The order is exactly the one obtained by
 * concatenating the elements in every individual vector.
 */
template<typename T>
static inline std::vector<T>
concatenate_vectors(const std::vector<std::vector<T>> &vectors) {
    std::vector<T> merged_vector;
    size_t         total_size {0};
    for (const auto &v : vectors) {
        total_size += v.size();
    }
    merged_vector.reserve(total_size);
    for (const auto &v : vectors) {
        merged_vector.insert(merged_vector.end(), v.begin(), v.end());
    }
    return merged_vector;
}

#endif // #ifndef UTIL_HPP
