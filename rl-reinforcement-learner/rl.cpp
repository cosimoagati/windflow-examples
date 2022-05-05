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
#include <condition_variable>
#include <cstdint>
#include <getopt.h>
#include <initializer_list>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <stduuid/uuid.h>
#include <string>
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
    const char *     metric_output_directory = ".";
    Execution_Mode_t execution_mode          = Execution_Mode_t::DEFAULT;
    Time_Policy_t    time_policy             = Time_Policy_t::INGRESS_TIME;
    unsigned         parallelism[num_nodes]  = {1, 1, 1, 1};
    unsigned         output_batch_sizes[num_nodes - 1] = {0, 0, 0};
    unsigned         batch_size                        = 0;
    unsigned         duration                          = 60;
    unsigned         tuple_rate                        = 1000;
    unsigned         sampling_rate                     = 100;
    bool             use_chaining                      = false;
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
    void push(T const &value) {
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

static inline void parse_args(int argc, char **argv, Parameters &parameters) {
    int option;
    int index;

    while ((option = getopt_long(argc, argv, "r:s:p:b:c:d:o:e:t:h", long_opts,
                                 &index))
           != -1) {
        switch (option) {
        case 'r':
            parameters.tuple_rate = atoi(optarg);
            break;
        case 's':
            parameters.sampling_rate = atoi(optarg);
            break;
        case 'b':
            parameters.batch_size = atoi(optarg);
            break;
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
            parameters.use_chaining = atoi(optarg) > 0 ? true : false;
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
         << "Event source parallelism: "
         << parameters.parallelism[ctr_generator_id] << '\n'
         << "Reward source parallelism: "
         << parameters.parallelism[reward_source_id] << '\n'
         << "Reinforcement learner parallelism: "
         << parameters.parallelism[reinforcement_learner_id] << '\n'
         << "Sink parallelism: " << parameters.parallelism[sink_id] << '\n'
         << "Batching: ";
    if (parameters.batch_size > 0) {
        cout << parameters.batch_size << '\n';
    } else {
        cout << "None\n";
    }

    cout << "Execution mode: "
         << get_string_from_execution_mode(parameters.execution_mode) << '\n';
    cout << "Time policy: "
         << get_string_from_time_policy(parameters.time_policy) << '\n';

    cout << "Duration: " << parameters.duration << " second"
         << (parameters.duration == 1 ? "" : "s") << '\n'
         << "Tuple generation rate: ";
    if (parameters.tuple_rate > 0) {
        cout << parameters.tuple_rate << " tuple"
             << (parameters.tuple_rate == 1 ? "" : "s") << " per second\n";
    } else {
        cout << "unlimited (BEWARE OF QUEUE CONGESTION)\n";
    }

    cout << "Sampling rate: ";
    if (parameters.sampling_rate > 0) {
        cout << parameters.sampling_rate << " measurement"
             << (parameters.sampling_rate == 1 ? "" : "s") << " per second\n";
    } else {
        cout << "unlimited (sample every incoming tuple)\n";
    }

    cout << "Chaining: " << (parameters.use_chaining ? "enabled" : "disabled")
         << '\n';
}

static inline void print_statistics(unsigned long elapsed_time,
                                    unsigned long duration,
                                    unsigned long sent_tuples,
                                    double        average_latency,
                                    unsigned long received_tuples) {
    const auto elapsed_time_in_seconds =
        elapsed_time / static_cast<double>(timeunit_scale_factor);

    const auto throughput =
        elapsed_time > 0 ? sent_tuples / static_cast<double>(elapsed_time)
                         : sent_tuples;

    const auto throughput_in_seconds   = throughput * timeunit_scale_factor;
    const auto service_time            = 1 / throughput;
    const auto service_time_in_seconds = service_time / timeunit_scale_factor;
    const auto latency_in_seconds = average_latency / timeunit_scale_factor;

    cout << "Elapsed time: " << elapsed_time << ' ' << timeunit_string << "s ("
         << elapsed_time_in_seconds << " seconds)\n"
         << "Excess time after source stopped: "
         << elapsed_time - duration * timeunit_scale_factor << ' '
         << timeunit_string << "s\n"
         << "Total number of tuples sent: " << sent_tuples << '\n'
         << "Total number of tuples recieved: " << received_tuples << '\n'
         << "Processed about " << throughput << " tuples per "
         << timeunit_string << " (" << throughput_in_seconds
         << " tuples per second)\n"
         << "Service time: " << service_time << ' ' << timeunit_string << "s ("
         << service_time_in_seconds << " seconds)\n"
         << "Average latency: " << average_latency << ' ' << timeunit_string
         << "s (" << latency_in_seconds << " seconds)\n";
}

/*
 * Global variables
 */
static atomic_ulong          global_sent_tuples {0};
static atomic_ulong          global_received_tuples {0};
static Metric<unsigned long> global_latency_metric {"latency"};
static Metric<unsigned long> global_interdeparture_metric {
    "interdeparture-time"};
static Metric<unsigned long> global_service_time_metric {"service-time"};
static BlockingQueue<string> global_action_queue;
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
        const auto action = global_action_queue.pop();
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

class IntervalEstimator {
    vector<string> actions;
    unsigned       batch_size;
    vector<string> selected_actions;

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

    void init_selected_actions() {
        if (batch_size == 0) {
            selected_actions = vector<string>(1);
        } else {
            selected_actions = vector<string>(batch_size);
        }
    }

public:
    IntervalEstimator(const vector<string> &actions, unsigned batch_size = 1,
                      unsigned bin_width = 1, unsigned confidence_limit = 95,
                      unsigned min_confidence_limit                      = 50,
                      unsigned confidence_limit_reduction_step           = 5,
                      unsigned confidence_limit_reduction_round_interval = 50,
                      unsigned min_distribution_sample                   = 30)
        : actions {actions}, batch_size {batch_size},
          confidence_limit {confidence_limit},
          min_confidence_limit {min_confidence_limit},
          confidence_limit_reduction_step {confidence_limit_reduction_step},
          confidence_limit_reduction_round_interval {
              confidence_limit_reduction_round_interval},
          min_distribution_sample {min_distribution_sample} {
        for (const auto &action : actions) {
            reward_distr.insert({action, HistogramStat {bin_width}});
        }
        init_selected_actions();

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
                static_cast<unsigned>(rand(mt) * actions.size());
            assert(random_index < actions.size());

            selected_action = actions[random_index];
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

        assert(!selected_actions.empty());
        assert(selected_action != "");

        selected_actions[0] = selected_action;
        return selected_actions;
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
    vector<string> actions;
    unsigned       batch_size;
    vector<string> selected_actions;

    unordered_map<string, vector<unsigned>> reward_distr;
    unsigned                                min_sample_size;
    unsigned                                max_reward;
    mt19937                                 mt {random_device {}()};
    uniform_real_distribution<double>       rand;

    void init_selected_actions() {
        if (batch_size == 0) {
            selected_actions = vector<string>(1);
        } else {
            selected_actions = vector<string>(batch_size);
        }
    }

public:
    SampsonSampler(const vector<string> &actions,
                   unsigned min_sample_size = 10, unsigned max_reward = 100)
        : actions {actions}, min_sample_size {min_sample_size},
          max_reward {max_reward} {}

    SampsonSampler &with_batch_size(unsigned batch_size) {
        this->batch_size = batch_size;
        return *this;
    }

    SampsonSampler &with_actions(const vector<string> &actions) {
        this->actions = actions;
        return *this;
    }

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

        assert(!selected_actions.empty());
        selected_actions[0] = selected_action_id;
        return selected_actions;
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
    vector<unsigned long> interdeparture_samples;
    vector<unsigned long> service_time_samples;
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

    void operator()(optional<OutputTuple> &input, RuntimeContext &context) {
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
            const auto interdeparture_time =
                difference(arrival_time, last_arrival_time);

            ++tuples_received;
            last_arrival_time = arrival_time;

            if (is_time_to_sample(arrival_time)) {
                latency_samples.push_back(latency);
                interdeparture_samples.push_back(interdeparture_time);

                const double service_time =
                    interdeparture_time
                    / static_cast<double>(context.getParallelism());
                service_time_samples.push_back(service_time);
                last_sampling_time = arrival_time;
            }
        } else {
            global_received_tuples.fetch_add(tuples_received);
            global_latency_metric.merge(latency_samples);
            global_interdeparture_metric.merge(interdeparture_samples);
            global_service_time_metric.merge(service_time_samples);
        }
    }
};

static inline PipeGraph &build_graph(const Parameters &parameters,
                                     PipeGraph &       graph) {
    CTRGeneratorFunctor ctr_generator_functor {parameters.duration,
                                               parameters.tuple_rate};

    auto ctr_generator_node =
        Source_Builder {ctr_generator_functor}
            .withParallelism(parameters.parallelism[ctr_generator_id])
            .withName("ctr generator")
            .withOutputBatchSize(parameters.batch_size)
            .build();

    RewardSourceFunctor reward_source_functor {
        parameters.duration, parameters.tuple_rate,
        parameters.parallelism[reinforcement_learner_id]};
    auto reward_source_node =
        Source_Builder {reward_source_functor}
            .withParallelism(parameters.parallelism[reward_source_id])
            .withName("reward source")
            .withOutputBatchSize(parameters.batch_size)
            .build();

    ReinforcementLearnerFunctor<IntervalEstimator>
         reinforcement_learner_functor {default_available_actions};
    auto reinforcement_learner_node =
        FlatMap_Builder {reinforcement_learner_functor}
            .withParallelism(parameters.parallelism[reinforcement_learner_id])
            .withName("reinforcement learner")
            .withKeyBy([](const InputTuple &tuple) {
                return tuple.reinforcement_learner_target_replica;
            })
            .withOutputBatchSize(parameters.batch_size)
            .build();

    SinkFunctor sink_functor {parameters.sampling_rate};
    auto        sink = Sink_Builder {sink_functor}
                    .withParallelism(parameters.parallelism[sink_id])
                    .withName("sink")
                    .build();

    auto &ctr_generator_pipe = graph.add_source(ctr_generator_node);
    auto &reward_source_pipe = graph.add_source(reward_source_node);
    auto &reinforcement_learner_pipe =
        ctr_generator_pipe.merge(reward_source_pipe);

    if (parameters.use_chaining) {
        reinforcement_learner_pipe.chain(reinforcement_learner_node)
            .chain_sink(sink);
    } else {
        reinforcement_learner_pipe.add(reinforcement_learner_node)
            .add_sink(sink);
    }
    return graph;
}

int main(int argc, char *argv[]) {
    Parameters parameters;
    parse_args(argc, argv, parameters);
    validate_args(parameters);
    print_initial_parameters(parameters);

    PipeGraph graph {"rl-reinforcement-learner", parameters.execution_mode,
                     parameters.time_policy};
    build_graph(parameters, graph);

    const auto start_time = current_time();
    graph.run();
    const auto elapsed_time = difference(current_time(), start_time);

    serialize_to_json(global_latency_metric,
                      parameters.metric_output_directory,
                      global_received_tuples);
    serialize_to_json(global_interdeparture_metric,
                      parameters.metric_output_directory,
                      global_received_tuples);
    serialize_to_json(global_service_time_metric,
                      parameters.metric_output_directory,
                      global_received_tuples);

    const auto average_latency =
        accumulate(global_latency_metric.begin(), global_latency_metric.end(),
                   0.0)
        / (!global_latency_metric.empty() ? global_latency_metric.size()
                                          : 1.0);
    print_statistics(elapsed_time, parameters.duration, global_sent_tuples,
                     average_latency, global_received_tuples);
    return 0;
}
