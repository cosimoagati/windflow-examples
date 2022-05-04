#ifndef UTIL_HPP
#define UTIL_HPP

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <ctime>
#include <dirent.h>
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <sys/types.h>
#include <system_error>
#include <vector>

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wreorder"
#pragma GCC diagnostic ignored "-Wextra"
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wsign-compare"
#if defined(__clang__) || !defined(__GNUC__)
#pragma GCC diagnostic ignored "-Wmismatched-tags"
#endif
#pragma GCC diagnostic ignored "-Woverloaded-virtual"
#endif

#include <wf/windflow.hpp>

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif

/* util.hpp
 *
 * This header file contains some miscellaneous reusable utilities.
 */

static constexpr auto current_time = wf::current_time_nsecs;

static const auto timeunit_string =
    current_time == wf::current_time_usecs   ? "microsecond"
    : current_time == wf::current_time_nsecs ? "nanosecond"
                                             : "time unit";

static const unsigned long timeunit_scale_factor =
    current_time == wf::current_time_usecs   ? 1000000
    : current_time == wf::current_time_nsecs ? 1000000000
                                             : 1;

/*
 * Return difference between a and b, accounting for unsigned arithmetic
 * wraparound.
 */
static unsigned long difference(unsigned long a, unsigned long b) {
    return std::max(a, b) - std::min(a, b);
}

/*
 * Suspend execution for an amount of time units specified by duration.
 */
static inline void busy_wait(unsigned long duration) {
    const auto start_time = current_time();
    auto       now        = start_time;
    while (now - start_time < duration) {
        now = current_time();
    }
}

/*
 * Return a std::vector of std::string_views, obtained from splitting the
 * original string_view. by the delim character.
 */
static inline std::vector<std::string_view>
string_split(const std::string_view &s, char delim) {
    const auto is_delim   = [=](char c) { return c == delim; };
    auto       word_begin = std::find_if_not(s.begin(), s.end(), is_delim);
    std::vector<std::string_view> words;

    while (word_begin < s.end()) {
        const auto word_end = std::find_if(word_begin + 1, s.end(), is_delim);
        words.emplace_back(word_begin, word_end - word_begin);
        word_begin = std::find_if_not(word_end, s.end(), is_delim);
    }
    return words;
}

static inline std::vector<size_t>
get_nums_split_by_commas(const char *degrees) {
    std::vector<std::size_t> parallelism_degrees;
    for (const auto &s : string_split(degrees, ',')) {
        parallelism_degrees.push_back(atoi(s.data()));
    }
    return parallelism_degrees;
}

static inline std::string get_datetime_string() {
    const auto  current_date = std::time(nullptr);
    std::string date_string  = std::asctime(localtime(&current_date));
    if (!date_string.empty()) {
        date_string.pop_back(); // needed to remove trailing newline
    }
    return date_string;
}

static const std::unordered_map<std::string, wf::Execution_Mode_t>
    string_to_execution_mode_map {
        {"default", wf::Execution_Mode_t::DEFAULT},
        {"deterministic", wf::Execution_Mode_t::DETERMINISTIC},
        {"probabilistic", wf::Execution_Mode_t::PROBABILISTIC}};

static const std::unordered_map<std::string, wf::Time_Policy_t>
    string_to_time_policy_map {
        {"ingress_time", wf::Time_Policy_t::INGRESS_TIME},
        {"event_time", wf::Time_Policy_t::EVENT_TIME}};

static const std::unordered_map<wf::Execution_Mode_t, std::string>
    execution_mode_to_string_map {
        {wf::Execution_Mode_t::DEFAULT, "default"},
        {wf::Execution_Mode_t::DETERMINISTIC, "deterministic"},
        {wf::Execution_Mode_t::PROBABILISTIC, "probabilistic"}};

static const std::unordered_map<wf::Time_Policy_t, std::string>
    time_policy_to_string_map {
        {wf::Time_Policy_t::INGRESS_TIME, "ingress time"},
        {wf::Time_Policy_t::EVENT_TIME, "event time"}};

template<typename T>
class Metric {
    std::vector<T> sorted_samples;
    std::string    metric_name;
    std::mutex     metric_mutex;

public:
    Metric(const char *name = "name") : metric_name {name} {}

    Metric &merge(const std::vector<T> &new_samples) {
        std::lock_guard guard {metric_mutex};
        sorted_samples.insert(sorted_samples.begin(), new_samples.begin(),
                              new_samples.end());
        sort(sorted_samples.begin(), sorted_samples.end());
        return *this;
    }

    std::size_t size() const {
        return sorted_samples.size();
    }

    std::size_t length() const {
        return sorted_samples.length();
    }

    bool empty() const {
        return sorted_samples.empty();
    }

    typename std::vector<T>::const_iterator begin() const {
        return sorted_samples.begin();
    }

    typename std::vector<T>::const_iterator end() const {
        return sorted_samples.end();
    }

    const char *name() const {
        return metric_name.c_str();
    }
};

static inline void create_directory_if_not_exists(const char *path) noexcept {
    const auto dir = opendir(path);
    if (dir) {
        const int status = closedir(dir);
        if (status != 0) {
            std::cerr << "Error closing directory " << path << '\n';
            std::exit(EXIT_FAILURE);
        }
    } else {
        const int status = mkdir(path, S_IRWXU | S_IRGRP | S_IROTH);
        if (status != 0) {
            std::cerr << "Error creating directory\n";
            exit(EXIT_FAILURE);
        }
    }
}

static inline void serialize_to_json(const Metric<unsigned long> &metric,
                                     const char *  output_directory,
                                     unsigned long total_measurements) {
    nlohmann::ordered_json json_stats;
    json_stats["date"]                 = get_datetime_string();
    json_stats["name"]                 = metric.name();
    json_stats["time unit"]            = std::string {timeunit_string} + 's';
    json_stats["sampled measurements"] = metric.size();
    json_stats["total measurements"]   = total_measurements;

    if (!metric.empty()) {
        const auto mean =
            accumulate(metric.begin(), metric.end(), 0.0) / metric.size();
        json_stats["mean"] = mean;

        for (const auto percentile : {0.0, 0.05, 0.25, 0.5, 0.75, 0.95, 1.0}) {
            const auto percentile_value_position =
                metric.begin() + (metric.size() - 1) * percentile;
            const auto label =
                std::to_string(static_cast<int>(percentile * 100))
                + "th percentile ";
            json_stats[label] = *percentile_value_position;
        }
    } else {
        json_stats["mean"] = 0;
        for (const auto percentile : {"0", "25", "50", "75", "95", "100"}) {
            const auto label  = std::string {percentile} + "th percentile";
            json_stats[label] = 0;
        }
    }
    create_directory_if_not_exists(output_directory);
    std::ofstream fs {std::string {output_directory} + std::string {"/metric-"}
                      + metric.name() + ".json"};
    fs << json_stats.dump(4) << '\n';
}

#endif // #ifndef UTIL_HPP
