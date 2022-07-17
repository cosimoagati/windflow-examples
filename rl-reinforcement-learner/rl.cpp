/*
 * Copyright (C) 2021-2022 Cosimo Agati
 *
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the Affero GNU General Public License
 * as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * You should have received a copy of the GNU AGPLv3 with this software,
 * if not, please visit <https://www.gnu.org/licenses/>
 */

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <getopt.h>
#include <initializer_list>
#include <mutex>
#include <nlohmann/json.hpp>
#include <numeric>
#include <optional>
#include <random>
#include <stduuid/uuid.h>
#include <string>
#include <thread>
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

enum NodeID : unsigned {
    ctr_generator_id         = 0,
    reward_source_id         = 1,
    reinforcement_learner_id = 2,
    sink_id                  = 3,
    num_nodes                = 4
};

struct Parameters {
    const char *     metric_output_directory    = ".";
    const char *     reinforcement_learner_type = "interval-estimator";
    Execution_Mode_t execution_mode             = Execution_Mode_t::DEFAULT;
    Time_Policy_t    time_policy                = Time_Policy_t::INGRESS_TIME;
    unsigned         parallelism[num_nodes]     = {1, 1, 1, 1};
    unsigned         batch_size[num_nodes - 1]  = {0, 0, 0};
    unsigned         duration                   = 60;
    unsigned         tuple_rate                 = 0;
    unsigned         sampling_rate              = 100;
    bool             use_chaining               = false;
};

struct InputTuple {
    enum { Event, Reward } tag;
    string        id;
    unsigned long value;
    unsigned long timestamp;
    unsigned      reinforcement_learner_target_replica;
};

struct OutputTuple {
    vector<string> actions;
    string         event_id;
    unsigned long  timestamp;
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
                                          {"reinforcementlearner", 1, 0, 'R'},
                                          {0, 0, 0, 0}};

static const vector<string> default_available_actions {"page1", "page2",
                                                       "page3"};
template<typename T>
class BlockingQueue {
private:
    mutex              internal_mutex;
    condition_variable cv;
    queue<T>           internal_queue;

public:
    void push(const T &value) {
        {
            lock_guard<mutex> lock {internal_mutex};
            internal_queue.push(value);
        }
        cv.notify_one();
    }

    T pop() {
        unique_lock<mutex> lock {internal_mutex};
        while (internal_queue.empty()) {
            cv.wait(lock);
        }
        const auto element = move(internal_queue.front());
        internal_queue.pop();
        return element;
    }
};

template<typename T>
class NonBlockingQueue {
private:
    mutex    internal_mutex;
    queue<T> internal_queue;

public:
    void push(const T &value) {
        lock_guard lock {internal_mutex};
        internal_queue.push(value);
    }

    optional<T> pop() {
        lock_guard lock {internal_mutex};
        if (internal_queue.empty()) {
            return {};
        }
        const auto element = move(internal_queue.front());
        internal_queue.pop();
        return element;
    }
};

static inline void parse_args(int argc, char **argv, Parameters &parameters) {
    int option;
    int index;

    while ((option = getopt_long(argc, argv, "r:s:p:b:c:d:o:e:t:R:h",
                                 long_opts, &index))
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
        case 'R':
            parameters.reinforcement_learner_type = optarg;
            break;
        case 'h':
            cout << "Parameters: --rate <value> --sampling "
                    "<value> --batch <size> --parallelism "
                    "<nEventSource,nRewardSource,nReinforcementLearner,nSink> "
                    "[--duration <seconds>] "
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

static void validate_args(const Parameters &parameters) {
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

    const auto max_threads = thread::hardware_concurrency();

    for (unsigned i = 0; i < num_nodes; ++i) {
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

static void print_initial_parameters(const Parameters &parameters) {
    cout << "Running graph with the following parameters:\n"
         << "Event source parallelism:\t"
         << parameters.parallelism[ctr_generator_id] << '\n'
         << "Reward source parallelism:\t"
         << parameters.parallelism[reward_source_id] << '\n'
         << "Reinforcement learner parallelism:\t"
         << parameters.parallelism[reinforcement_learner_id] << '\n'
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
         << '\n'
         << "Reinforcement Learner type: "
         << parameters.reinforcement_learner_type << '\n';
}

/*
 * Global variables
 */
static atomic_ulong             global_sent_tuples {0};
static atomic_ulong             global_received_tuples {0};
static Metric<unsigned long>    global_latency_metric {"rl-latency"};
static NonBlockingQueue<string> global_action_queue;
#ifndef NDEBUG
static mutex print_mutex;
#endif

class CTRGeneratorFunctor {
    unsigned long duration;
    unsigned      tuple_rate_per_second;

    unsigned long round_num   = 1;
    unsigned long event_count = 0;
    unsigned long max_rounds; // is this needed?

    mt19937                      mt {random_device {}()};
    uuids::uuid_random_generator uuid_gen {mt};

    InputTuple get_new_tuple() {
        const auto session_id = uuids::to_string(uuid_gen());
        ++round_num;
        ++event_count;

#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[EVENT SOURCE] Generated event with ID: " << session_id
                 << '\n';
            if (event_count % 1000 == 0) {
                clog << "[EVENT SOURCE] Generated " << event_count
                     << " events\n";
            }
        }
#endif
        const auto timestamp = current_time();
        return {InputTuple::Event, session_id, round_num, timestamp, 0};
    }

public:
    CTRGeneratorFunctor(unsigned long d, unsigned rate,
                        unsigned long max_rounds = 10000)
        : duration {d * timeunit_scale_factor}, tuple_rate_per_second {rate},
          max_rounds {max_rounds} {}

    void operator()(Source_Shipper<InputTuple> &shipper) {
        const auto    end_time    = current_time() + duration;
        unsigned long sent_tuples = 0;

        while (current_time() < end_time) {
            shipper.push(get_new_tuple());
            ++sent_tuples;
            if (tuple_rate_per_second > 0) {
                const unsigned long delay =
                    (1.0 / tuple_rate_per_second) * timeunit_scale_factor;
                busy_wait(delay);
            }
        }
        global_sent_tuples.fetch_add(sent_tuples);
    }
};

class RewardSourceFunctor {
    unordered_map<string, int>         action_selection_map;
    int                                action_selection_count_threshold = 50;
    unordered_map<string, vector<int>> action_ctr_distr {
        {"page1", {30, 12}}, {"page2", {60, 30}}, {"page3", {80, 10}}};
    mt19937                       mt {random_device {}()};
    uniform_int_distribution<int> rand {1, 101};
    unsigned long                 duration;
    unsigned                      tuple_rate_per_second;
    unsigned                      reinforcement_learner_replicas;

    void send_new_reward(Source_Shipper<InputTuple> &shipper) {
        optional<string> action_handle;
        unsigned         tries = 0;
        do {
            action_handle = global_action_queue.pop();
            ++tries;
        } while (!action_handle && tries < 1000);

        if (!action_handle) {
            return;
        }

        const auto action = *action_handle;
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[REWARD SOURCE] Received action " << action
                 << " from queue\n";
        }
#endif
        if (action_selection_map.find(action) == action_selection_map.end()) {
            action_selection_map.insert({action, 1});
        } else {
            action_selection_map[action] += 1;
        }

        if (action_selection_map[action] == action_selection_count_threshold) {
            assert(action_ctr_distr.find(action) != action_ctr_distr.end());
            const auto distr = action_ctr_distr[action];
            int        sum   = 0;

            for (int i = 0; i < 12; ++i) {
                sum += rand(mt);
                const double r = (sum - 100) / 100.0;

                assert(distr.size() >= 2);
                int r2 = static_cast<int>(r) * distr[1] + distr[0];
                if (r2 < 0) {
                    r2 = 0;
                }
                action_selection_map[action] = 0;
#ifndef NDEBUG
                {
                    lock_guard lock {print_mutex};
                    clog << "[REWARD SOURCE] Sending action " << action
                         << " with reward " << r2 << '\n';
                }
#endif
                for (unsigned i = 0; i < reinforcement_learner_replicas; ++i) {
                    const auto timestamp = current_time();
                    shipper.push({InputTuple::Reward, action,
                                  static_cast<unsigned>(r2), timestamp, i});
                }
            }
        }
    }

public:
    RewardSourceFunctor(unsigned d, unsigned rate, unsigned rl_replicas)
        : duration {d * timeunit_scale_factor}, tuple_rate_per_second {rate},
          reinforcement_learner_replicas {rl_replicas} {
        assert(rl_replicas != 0);
    }

    void operator()(Source_Shipper<InputTuple> &shipper) {
        const auto    end_time    = current_time() + duration;
        unsigned long sent_tuples = 0;

        while (current_time() < end_time) {
            send_new_reward(shipper);
            ++sent_tuples;
            if (tuple_rate_per_second > 0) {
                const unsigned long delay =
                    (1.0 / tuple_rate_per_second) * timeunit_scale_factor;
                busy_wait(delay);
            }
        }
        global_sent_tuples.fetch_add(sent_tuples);
    }
};

class Bin {
    unsigned index;
    unsigned count;

public:
    Bin(unsigned index, unsigned count = 0) : index {index}, count {count} {}

    void add_count(unsigned count) {
        this->count += count;
    }

    bool operator<(const Bin &other) const {
        return index < other.index;
    }

    unsigned get_index() const {
        return index;
    }

    unsigned get_count() const {
        return count;
    }
};

class HistogramStat {
    unsigned                     bin_width;
    unordered_map<unsigned, Bin> bin_map;
    unsigned                     count = 0;
    double                       sum   = 0;

public:
    HistogramStat(unsigned bin_width) : bin_width {bin_width} {}

    void add(unsigned value, unsigned count) {
        const unsigned index {value / bin_width};
        if (bin_map.find(index) == bin_map.end()) {
            bin_map.insert_or_assign(index, Bin {index});
        }
        bin_map.find(index)->second.add_count(count);
        this->count += count;
        sum += value * count;
    }

    void add(unsigned value) {
        add(value, 1);
    }

    unsigned get_mean() const {
        assert(count > 0);
        return sum / count;
    }

    unsigned get_count() const {
        return count;
    }

    vector<unsigned> get_confidence_bounds(unsigned confidence_limit_percent) {
        vector<unsigned> confidence_bounds(2, 0);

        const unsigned mean = get_mean();
        const unsigned mean_index {mean / bin_width};

        const unsigned confidence_limit =
            (count * confidence_limit_percent) / 100;
        unsigned conf_cont = 0;
        unsigned bin_count = 0;
        auto     bin_entry = bin_map.find(mean_index);

        if (bin_entry != bin_map.end()) {
            conf_cont += bin_entry->second.get_count();
            ++bin_count;
        }

        unsigned offset = 1;
        for (; bin_count < bin_map.size(); ++offset) {
            bin_entry = bin_map.find(mean_index + offset);
            if (bin_entry != bin_map.end()) {
                conf_cont += bin_entry->second.get_count();
                ++bin_count;
            }
            assert(offset <= mean_index);
            bin_entry = bin_map.find(mean_index - offset);
            if (bin_entry != bin_map.end()) {
                conf_cont += bin_entry->second.get_count();
                ++bin_count;
            }
            if (conf_cont >= confidence_limit) {
                break;
            }
        }

        double av_bin_width = bin_width > 1 ? 0.5 : 0.0;

        assert(confidence_bounds.size() >= 2);
        confidence_bounds[0] = static_cast<unsigned>(
            (static_cast<double>(mean_index - offset) + av_bin_width)
            * bin_width);
        confidence_bounds[1] = static_cast<unsigned>(
            (static_cast<double>(mean_index + offset) + av_bin_width)
            * bin_width);
        return confidence_bounds;
    }

    vector<Bin> get_sorted_bins() const {
        vector<Bin> bins;
        for (const auto &kv : bin_map) {
            const auto &bin = kv.second;
            bins.push_back(bin);
        }
        sort(bins.begin(), bins.end());
        return bins;
    }
};

class ActionBatch {
    vector<string> available_actions;
    vector<string> selected_actions;

public:
    ActionBatch(const vector<string> &actions,
                size_t                selected_actions_batch_size = 1)
        : available_actions {actions} {
        if (selected_actions_batch_size == 0) {
            selected_actions = vector<string>(1);
        } else {
            selected_actions = vector<string>(selected_actions_batch_size);
        }
    }

    ActionBatch &with_batch_size(size_t batch_size) {
        selected_actions.resize(batch_size);
        return *this;
    }

    const string &operator[](size_t i) const {
        return available_actions[i];
    }

    size_t size() const {
        return available_actions.size();
    }

    void push_new_selected_action(const string &action) {
        selected_actions[0] = action;
    }

    const vector<string> &get_selected_actions() const {
        return selected_actions;
    }

    const vector<string>::const_iterator begin() const {
        return available_actions.begin();
    }

    const vector<string>::const_iterator end() const {
        return available_actions.end();
    }
};

class IntervalEstimator {
    ActionBatch action_batch;

    unsigned confidence_limit; // is this needed?
    unsigned min_confidence_limit;
    unsigned current_confidence_limit;
    unsigned confidence_limit_reduction_step;
    unsigned confidence_limit_reduction_round_interval;
    unsigned min_distribution_sample;

    unordered_map<string, HistogramStat> reward_distr;
    unsigned long                        last_round_num = 1;
    unsigned long                        random_select_count;
    unsigned long                        intv_est_select_count;
    unsigned long                        log_counter;
    unsigned long                        round_counter;
    bool                                 is_low_sample = true;
    mt19937                              mt {random_device {}()};
    uniform_real_distribution<double>    rand;

public:
    IntervalEstimator(const vector<string> &actions, unsigned batch_size = 1,
                      unsigned bin_width = 1, unsigned confidence_limit = 95,
                      unsigned min_confidence_limit                      = 50,
                      unsigned confidence_limit_reduction_step           = 5,
                      unsigned confidence_limit_reduction_round_interval = 50,
                      unsigned min_distribution_sample                   = 30)
        : action_batch {actions, batch_size},
          confidence_limit {confidence_limit},
          min_confidence_limit {min_confidence_limit},
          confidence_limit_reduction_step {confidence_limit_reduction_step},
          confidence_limit_reduction_round_interval {
              confidence_limit_reduction_round_interval},
          min_distribution_sample {min_distribution_sample} {
        for (const auto &action : actions) {
            reward_distr.insert({action, HistogramStat {bin_width}});
        }
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[INTERVAL ESTIMATOR] confidence_limit: "
                 << confidence_limit
                 << " min_confidence_limit: " << min_confidence_limit
                 << " confidence_limit_reduction_step: "
                 << confidence_limit_reduction_step
                 << "confidence_limit_reduction_round_interval: "
                 << confidence_limit_reduction_round_interval << '\n';
        }
#endif
    }

    void adjust_conf_limit(unsigned long round_num) {
        if (current_confidence_limit > min_confidence_limit) {
            assert(last_round_num <= round_num);
            unsigned long red_step {
                (round_num - last_round_num)
                / confidence_limit_reduction_round_interval};
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[INTERVAL ESTIMATOR] red_step: " << red_step
                     << " round_num: " << round_num
                     << " last_round_num: " << last_round_num << '\n';
            }

#endif
            if (red_step > 0) {
                assert(current_confidence_limit
                       >= red_step * confidence_limit_reduction_step);
                current_confidence_limit -=
                    (red_step * confidence_limit_reduction_step);
                if (current_confidence_limit < min_confidence_limit) {
                    current_confidence_limit = min_confidence_limit;
                }
#ifndef NDEBUG
                {
                    lock_guard lock {print_mutex};
                    clog << "reduce conf limit round_num: " << round_num
                         << "last_round_num " << last_round_num
                         << last_round_num << '\n';
                }
#endif
                last_round_num = round_num;
            }
        }
    }

public:
    const vector<string> &next_actions(unsigned long round_num) {
        string selected_action;
        ++log_counter;
        ++round_counter;

        if (is_low_sample) {
            is_low_sample = false;
            for (const auto &kv : reward_distr) {
                const auto sample_count = kv.second.get_count();
#ifndef NDEBUG
                if (log_counter % 100 == 0) {
                    {
                        lock_guard lock {print_mutex};
                        clog << "[INTERVAL ESTIMATOR] action: " << kv.first
                             << " sample_count" << sample_count << '\n';
                    }
                }
#endif
                if (sample_count < min_distribution_sample) {
                    is_low_sample = true;
                    break;
                }
            }

            if (!is_low_sample) {
#ifndef NDEBUG
                {
                    lock_guard lock {print_mutex};
                    clog << "[INTERVAL ESTIMATOR] Obtained full sample\n";
                }
                last_round_num = round_num; // Move outside the #ifndef?
#endif
            }
        }

        if (is_low_sample) {
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[INTERVAL ESTIMATOR] Using random index in interval "
                        "estimator\n";
            }
#endif
            const auto random_index =
                static_cast<unsigned>(rand(mt) * action_batch.size());
            assert(random_index < action_batch.size());

            selected_action = action_batch[random_index];
            ++random_select_count;
        } else {
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[INTERVAL ESTIMATOR] NOT using random index in "
                        "interval estimator\n";
            }
#endif
            adjust_conf_limit(round_num);

            unsigned max_upper_conf_bound = 0;
            for (auto &kv : reward_distr) {
                auto &     stat = kv.second;
                const auto conf_bounds =
                    stat.get_confidence_bounds(current_confidence_limit);
                assert(conf_bounds.size() >= 2);
#ifndef NDEBUG
                {
                    lock_guard lock {print_mutex};
                    clog << "[INTERVAL ESTIMATOR] current_confidence_limit: "
                         << current_confidence_limit << " action: " << kv.first
                         << " conf_bounds: " << conf_bounds[0] << " "
                         << conf_bounds[1] << '\n';
                }
#endif
                if (conf_bounds[1] > max_upper_conf_bound) {
                    max_upper_conf_bound = conf_bounds[1];
                    selected_action      = kv.first;
                }
            }
            ++intv_est_select_count;
        }

        assert(selected_action != "");
        action_batch.push_new_selected_action(selected_action);
        return action_batch.get_selected_actions();
    }

    void set_reward(const string &action, unsigned reward) {
        if (reward_distr.find(action) == reward_distr.end()) {
            cerr << "Invalid action: " << action << '\n';
            exit(EXIT_FAILURE);
        }
        reward_distr.insert_or_assign(action, reward);
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[INTERVAL ESTIMATOR] random_select_count: "
                 << random_select_count
                 << " intv_est_select_count: " << intv_est_select_count
                 << '\n';
        }
#endif
    }
};

class SampsonSampler {
    ActionBatch action_batch;

    unordered_map<string, vector<unsigned>> reward_distr;
    unsigned                                min_sample_size;
    unsigned                                max_reward;
    mt19937                                 mt {random_device {}()};
    uniform_real_distribution<double>       rand;

public:
    SampsonSampler(const vector<string> &actions, size_t batch_size = 1,
                   unsigned min_sample_size = 10, unsigned max_reward = 100)
        : action_batch {actions, batch_size},
          min_sample_size {min_sample_size}, max_reward {max_reward} {}

    const vector<string> &next_actions(unsigned long) {
        string   selected_action_id;
        unsigned max_reward_current = 0;
        unsigned index              = 0;
        unsigned reward             = 0;

        for (const auto &kv : reward_distr) {
            const auto &action_id = kv.first;
            const auto &rewards   = kv.second;
            if (rewards.size() > min_sample_size) {
                index = static_cast<unsigned>(rand(mt) * max_reward);

                assert(index < rewards.size());
                reward = rewards[index];
                reward = enforce(action_id, reward);
            } else {
                reward = static_cast<unsigned>(rand(mt) * max_reward);
            }

            if (reward > max_reward_current) {
                selected_action_id = action_id;
                max_reward_current = reward;
            }
        }

        action_batch.push_new_selected_action(selected_action_id);
        return action_batch.get_selected_actions();
    }

    unsigned enforce(const string &, unsigned reward) const {
        return reward;
    }

    void set_reward(const string &action_id, unsigned reward) {
        if (reward_distr.find(action_id) == reward_distr.end()) {
            reward_distr.insert_or_assign(action_id, vector<unsigned> {});
        }
        reward_distr.find(action_id)->second.push_back(reward);
    }

    const unordered_map<string, vector<unsigned>> &get_reward_distr() const {
        return reward_distr;
    }
};

class OptimisticSampsonSampler {
    SampsonSampler             sampson_sampler;
    unordered_map<string, int> mean_rewards;

public:
    OptimisticSampsonSampler(const vector<string> &actions,
                             size_t                batch_size      = 1,
                             unsigned              min_sample_size = 10,
                             unsigned              max_reward      = 100)
        : sampson_sampler {actions, batch_size, min_sample_size, max_reward} {}

    const vector<string> &next_actions(unsigned long round_num) {
        return sampson_sampler.next_actions(round_num);
    }

    void set_reward(const string &action_id, unsigned reward) {
        sampson_sampler.set_reward(action_id, reward);
    }

    void compute_reward_mean(const string &action_id) {
        const auto &reward_distr = sampson_sampler.get_reward_distr();
        const auto &entry        = reward_distr.find(action_id);

        if (entry != reward_distr.end()) {
            const auto &rewards = entry->second;
            int         sum     = 0;
            int         count   = 0;
            for (const int reward : rewards) {
                sum += reward;
                ++count;
            }
            mean_rewards.insert_or_assign(action_id, sum / count);
        }
    }

    unsigned enforce(const string &action_id, int reward) {
        const auto &entry = mean_rewards.find(action_id);
        assert(entry != mean_rewards.end());

        const int mean_reward = entry->second;
        return reward > mean_reward ? reward : mean_reward;
    }
};

class SimpleStat {
    double   sum   = 0;
    unsigned count = 0;

public:
    void add(double value) {
        sum += value;
        ++count;
    }

    double get_mean() {
        return sum / count;
    }
};

class RandomGreedyLearner {
    static constexpr auto linear_algorithm     = "linear";
    static constexpr auto log_linear_algorithm = "log_linear";

    ActionBatch action_batch;

    unordered_map<string, SimpleStat> reward_stats;
    double                            random_selection_probability;
    double                            probability_reduction_constant;
    string                            probability_reduction_algorithm;

    mt19937                           mt {random_device {}()};
    uniform_real_distribution<double> rand {0.0, 1.0};

public:
    RandomGreedyLearner(
        const vector<string> &actions, const size_t batch_size = 1,
        double        random_selection_probability    = 0.5,
        double        probability_reduction_constant  = 1.0,
        const string &probability_reduction_algorithm = "linear")
        : action_batch {actions, batch_size},
          random_selection_probability {random_selection_probability},
          probability_reduction_constant {probability_reduction_constant},
          probability_reduction_algorithm {probability_reduction_algorithm} {}

    vector<string> next_actions(unsigned long round_num) {
        double current_probability = 0.0;
        string next_action;

        if (probability_reduction_algorithm == linear_algorithm) {
            current_probability = random_selection_probability
                                  * probability_reduction_constant / round_num;
        } else if (probability_reduction_algorithm == log_linear_algorithm) {
            current_probability = random_selection_probability
                                  * probability_reduction_constant
                                  * log(round_num) / round_num;
        } else {
            cerr << "Error in RandomGreedyLearner: unknown algorithm\n";
            exit(EXIT_FAILURE);
        }

        current_probability =
            current_probability <= random_selection_probability
                ? current_probability
                : random_selection_probability;

        if (current_probability < rand(mt)) {
            next_action = action_batch[static_cast<size_t>(
                rand(mt) * action_batch.size())];
        } else {
            int best_reward = 0;

            for (const auto &action : action_batch) {
                const auto entry = reward_stats.find(action);
                assert(entry != reward_stats.end());

                int reward = static_cast<int>(entry->second.get_mean());
                if (reward > best_reward) {
                    best_reward = reward;
                    next_action = action;
                }
            }
        }
        action_batch.push_new_selected_action(next_action);
        return action_batch.get_selected_actions();
    }

    void set_reward(const string &action, unsigned reward) {
        auto entry = reward_stats.find(action);
        assert(entry != reward_stats.end());
        entry->second.add(reward);
    }
};

template<typename ReinforcementLearner>
class ReinforcementLearnerFunctor {
    ReinforcementLearner reinforcement_learner;

public:
    ReinforcementLearnerFunctor(const vector<string> &actions)
        : reinforcement_learner {actions} {}

    void operator()(const InputTuple &tuple, Shipper<OutputTuple> &shipper) {
        switch (tuple.tag) {
        case InputTuple::Event: {
            const auto &event_id = tuple.id;
            const auto  actions =
                reinforcement_learner.next_actions(tuple.value);
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[REINFORCEMENT LEARNER] Received event " << event_id
                     << ", possible actions are: ";
                for (size_t i = 0; i < actions.size(); ++i) {
                    clog << actions[i];
                    if (i != actions.size() - 1) {
                        clog << ", ";
                    }
                }
                clog << '\n';
            }
#endif
            shipper.push({actions, event_id, tuple.timestamp});
        } break;
        case InputTuple::Reward: {
            const auto &action_id = tuple.id;
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[REINFORCEMENT LEARNER] Received action with ID: "
                     << action_id << ", setting reward " << tuple.value
                     << '\n';
            }
#endif
            reinforcement_learner.set_reward(action_id, tuple.value);
        } break;
        default:
            assert(false);
            break;
        }
    }
};

class SinkFunctor {
    vector<unsigned long> latency_samples;
    unsigned long         tuples_received    = 0;
    unsigned long         last_sampling_time = current_time();
    unsigned long         last_arrival_time  = last_sampling_time;
    unsigned              sampling_rate;

    bool is_time_to_sample(unsigned long arrival_time) {
        if (sampling_rate == 0) {
            return true;
        }
        const auto time_since_last_sampling =
            difference(arrival_time, last_sampling_time);
        const auto time_between_samples =
            (1.0 / sampling_rate) * timeunit_scale_factor;
        return time_since_last_sampling >= time_between_samples;
    }

public:
    SinkFunctor(unsigned rate) : sampling_rate {rate} {}

    void operator()(optional<OutputTuple> &input) {
        if (input) {
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                if (!input->actions.empty()) {
                    clog << "[SINK] Received actions: ";
                    for (const auto &action : input->actions) {
                        clog << action << ", ";
                    }
                    clog << "for event: " << input->event_id << ". ";
                }
                clog << "Adding element " << (input->actions[0])
                     << " to the queue\n";
            }
#endif
            global_action_queue.push(input->actions[0]);

            const auto arrival_time = current_time();
            const auto latency = difference(arrival_time, input->timestamp);

            ++tuples_received;
            last_arrival_time = arrival_time;
            if (is_time_to_sample(arrival_time)) {
                latency_samples.push_back(latency);
                last_sampling_time = arrival_time;
            }
        } else {
            global_received_tuples.fetch_add(tuples_received);
            global_latency_metric.merge(latency_samples);
        }
    }
};

static MultiPipe &get_reinforcement_learner_pipe(const Parameters &parameters,
                                                 MultiPipe &       pipe) {
    const string name         = parameters.reinforcement_learner_type;
    const bool   use_chaining = parameters.use_chaining;

    if (name == "interval-estimator") {
        ReinforcementLearnerFunctor<IntervalEstimator>
                   reinforcement_learner_functor {default_available_actions};
        const auto reinforcement_learner_node =
            FlatMap_Builder {reinforcement_learner_functor}
                .withParallelism(
                    parameters.parallelism[reinforcement_learner_id])
                .withName("reinforcement learner")
                .withKeyBy([](const InputTuple &tuple) {
                    return tuple.reinforcement_learner_target_replica;
                })
                .withOutputBatchSize(
                    parameters.batch_size[reinforcement_learner_id])
                .build();
        return use_chaining ? pipe.chain(reinforcement_learner_node)
                            : pipe.add(reinforcement_learner_node);
    } else if (name == "sampson") {
        ReinforcementLearnerFunctor<SampsonSampler>
                   reinforcement_learner_functor {default_available_actions};
        const auto reinforcement_learner_node =
            FlatMap_Builder {reinforcement_learner_functor}
                .withParallelism(
                    parameters.parallelism[reinforcement_learner_id])
                .withName("reinforcement learner")
                .withKeyBy([](const InputTuple &tuple) {
                    return tuple.reinforcement_learner_target_replica;
                })
                .withOutputBatchSize(
                    parameters.batch_size[reinforcement_learner_id])
                .build();
        return use_chaining ? pipe.chain(reinforcement_learner_node)
                            : pipe.add(reinforcement_learner_node);
    } else if (name == "optimistic-sampson") {
        ReinforcementLearnerFunctor<OptimisticSampsonSampler>
                   reinforcement_learner_functor {default_available_actions};
        const auto reinforcement_learner_node =
            FlatMap_Builder {reinforcement_learner_functor}
                .withParallelism(
                    parameters.parallelism[reinforcement_learner_id])
                .withName("reinforcement learner")
                .withKeyBy([](const InputTuple &tuple) {
                    return tuple.reinforcement_learner_target_replica;
                })
                .withOutputBatchSize(
                    parameters.batch_size[reinforcement_learner_id])
                .build();
        return use_chaining ? pipe.chain(reinforcement_learner_node)
                            : pipe.add(reinforcement_learner_node);
    } else if (name == "random" || name == "random-greedy") {
        ReinforcementLearnerFunctor<RandomGreedyLearner>
                   reinforcement_learner_functor {default_available_actions};
        const auto reinforcement_learner_node =
            FlatMap_Builder {reinforcement_learner_functor}
                .withParallelism(
                    parameters.parallelism[reinforcement_learner_id])
                .withName("reinforcement learner")
                .withKeyBy([](const InputTuple &tuple) {
                    return tuple.reinforcement_learner_target_replica;
                })
                .withOutputBatchSize(
                    parameters.batch_size[reinforcement_learner_id])
                .build();
        return use_chaining ? pipe.chain(reinforcement_learner_node)
                            : pipe.add(reinforcement_learner_node);
    } else {
        cerr << "Error while building graph: unknown Reinforcement Learner "
                "type: "
             << name << '\n';
        exit(EXIT_FAILURE);
    }
}

static inline PipeGraph &build_graph(const Parameters &parameters,
                                     PipeGraph &       graph) {
    CTRGeneratorFunctor ctr_generator_functor {parameters.duration,
                                               parameters.tuple_rate};
    const auto          ctr_generator_node =
        Source_Builder {ctr_generator_functor}
            .withParallelism(parameters.parallelism[ctr_generator_id])
            .withName("ctr generator")
            .withOutputBatchSize(parameters.batch_size[ctr_generator_id])
            .build();

    RewardSourceFunctor reward_source_functor {
        parameters.duration, parameters.tuple_rate,
        parameters.parallelism[reinforcement_learner_id]};
    const auto reward_source_node =
        Source_Builder {reward_source_functor}
            .withParallelism(parameters.parallelism[reward_source_id])
            .withName("reward source")
            .withOutputBatchSize(parameters.batch_size[reward_source_id])
            .build();

    SinkFunctor sink_functor {parameters.sampling_rate};
    const auto  sink = Sink_Builder {sink_functor}
                          .withParallelism(parameters.parallelism[sink_id])
                          .withName("sink")
                          .build();

    auto &ctr_generator_pipe = graph.add_source(ctr_generator_node);
    auto &reward_source_pipe = graph.add_source(reward_source_node);
    auto &merged_source_pipe = ctr_generator_pipe.merge(reward_source_pipe);
    auto &reinforcement_learner_pipe =
        get_reinforcement_learner_pipe(parameters, merged_source_pipe);

    if (parameters.use_chaining) {
        reinforcement_learner_pipe.chain_sink(sink);
    } else {
        reinforcement_learner_pipe.add_sink(sink);
    }
    return graph;
}

int main(int argc, char *argv[]) {
    Parameters parameters;
    parse_args(argc, argv, parameters);
    validate_args(parameters);

    PipeGraph graph {"rl-reinforcement-learner", parameters.execution_mode,
                     parameters.time_policy};
    build_graph(parameters, graph);
    print_initial_parameters(parameters);

    const auto start_time = current_time();
    graph.run();
    const auto   elapsed_time = difference(current_time(), start_time);
    const double throughput =
        elapsed_time > 0
            ? (global_sent_tuples.load() / static_cast<double>(elapsed_time))
            : global_sent_tuples.load();
    const double service_time = 1 / throughput;

    const auto latency_stats = get_distribution_stats(
        global_latency_metric, parameters, global_received_tuples);
    serialize_json(latency_stats, "rl-latency",
                   parameters.metric_output_directory);

    const auto throughput_stats = get_single_value_stats(
        throughput, "throughput", parameters, global_sent_tuples.load());
    serialize_json(throughput_stats, "rl-throughput",
                   parameters.metric_output_directory);

    const auto service_time_stats = get_single_value_stats(
        service_time, "service time", parameters, global_sent_tuples.load());
    serialize_json(service_time_stats, "rl-service-time",
                   parameters.metric_output_directory);

    const auto average_latency =
        accumulate(global_latency_metric.begin(), global_latency_metric.end(),
                   0.0)
        / (!global_latency_metric.empty() ? global_latency_metric.size()
                                          : 1.0);
    print_statistics(elapsed_time, parameters.duration, global_sent_tuples,
                     average_latency, global_received_tuples);
    return 0;
}
