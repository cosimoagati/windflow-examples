#!/usr/bin/env python3

import json
import os
import matplotlib.pyplot as plt

DEBUG = True


def get_json_objs_from_directory(directory):
    file_list = [
        name for name in os.listdir(directory) if name.endswith('.json')
    ]
    json_list = []
    for name in file_list:
        with open(os.path.join(directory, name)) as f:
            json_list.append(json.load(f))
    return json_list


def filter_jsons_by_name(json_list, name):
    return [j for j in json_list if j['name'] == name]


def filter_jsons_by_parallelism(json_list, parallelism):
    return [j for j in json_list if j['parallelism'][0] == parallelism]


def filter_jsons_by_batch_size(json_list, batch_size):
    return [j for j in json_list if j['batch size'][0] == batch_size]


def filter_jsons_by_chaining(json_list, chaining):
    return [j for j in json_list if j['chaining enabled'] == chaining]


def filter_jsons_by_sampling_rate(json_list, sampling_rate):
    return [
        j for j in json_list
        if ('sampling_rate' in j and j['sampling_rate'] == sampling_rate) or (
            'sampling rate' in j and j['sampling rate'] == sampling_rate)
    ]


def filter_jsons_by_tuple_rate(json_list, tuple_rate):
    return [j for j in json_list if j['tuple_rate'] == tuple_rate]


def percentile_to_dictkey(kind):
    if kind in ['0th', '5th', '25th', '50th', '75th', '95th', '100th']:
        return kind + ' percentile'
    if kind in ['0', '5', '25', '50', '75', '95', '100']:
        return kind + 'th percentile'
    if kind in [0, 5, 25, 50, 75, 95, 100]:
        return str(kind) + 'th percentile'
    return kind


def get_y_label(name, time_unit):
    unit_to_abbrev = {
        'microseconds': 'us',
        'microsecond': 'us',
        'us': 'us',
        'nanoseconds': 'ns',
        'nanosecond': 'ns',
        'ns': 'ns',
        'milliseconds': 'ms',
        'millisecnod': 'ms',
        'ms': 'ms',
        'seconds': 's',
        'second': 's',
        's': 's'
    }
    return name.replace('-', ' ').capitalize() + ' (' + (
        unit_to_abbrev[time_unit]
        if time_unit in unit_to_abbrev else 'unknown unit') + ')'


def show_graphs(x_axis, y_axis, title, xlabel, ylabel):
    plt.figure()
    plt.title(title, loc='right', y=1.08)
    plt.grid(True)
    plt.plot(x_axis, y_axis, color='maroon', marker='o')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    plt.figure()
    plt.title(title, loc='right', y=1.08)
    plt.grid(False)
    plt.bar(x_axis, y_axis, color='maroon')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.show()
    plt.close('all')


def save_graph_images(x_axis, y_axis, title, xlabel, ylabel):
    plt.figure()
    plt.title(title, loc='right', y=1.08)
    plt.grid(True)
    plt.plot(x_axis, y_axis, color='maroon', marker='o')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.savefig(title + ' (plot).png')

    plt.figure()
    plt.grid(False)
    plt.title(title, loc='right', y=1.08)
    plt.bar(x_axis, y_axis, color='maroon')
    plt.savefig(title + ' (bar).png')
    plt.close('all')


def plot_by_parallelism(percentile,
                        directory,
                        name,
                        batchsize,
                        chaining,
                        sampling_rate=100,
                        tuple_rate=1000,
                        json_list=[],
                        save_images=False):
    if not json_list:
        json_list = get_json_objs_from_directory(directory, name)
    json_list = filter_jsons_by_chaining(json_list, chaining)
    json_list = filter_jsons_by_batch_size(json_list, batchsize)
    json_list = filter_jsons_by_sampling_rate(json_list, sampling_rate)
    json_list = filter_jsons_by_tuple_rate(json_list, tuple_rate)

    json_list.sort(key=lambda j: j['parallelism'][0])

    if not json_list:
        print('No data found with the specified parameters, not plotting...')
        return

    time_unit = json_list[0]['time unit']
    x_axis = [j['parallelism'][0] for j in json_list]
    y_axis = [j[percentile_to_dictkey(percentile)] for j in json_list]
    if DEBUG:
        print(x_axis)
        print(y_axis)
    title = (name.capitalize() + '(' + percentile + ') ' + '(batch size: ' +
             str(batchsize) + ') ' + '(chaining: ' + str(chaining) + ') ')
    xlabel = 'Parallelism degree for each node'
    ylabel = get_y_label(name, time_unit)
    if save_images:
        save_graph_images(x_axis,
                          y_axis,
                          title=title,
                          xlabel=xlabel,
                          ylabel=ylabel)
    else:
        show_graphs(x_axis, y_axis, title=title, xlabel=xlabel, ylabel=ylabel)


def plot_by_batch_size(percentile,
                       directory,
                       name,
                       parallelism,
                       chaining,
                       sampling_rate=100,
                       tuple_rate=1000,
                       json_list=[],
                       save_images=False):
    if not json_list:
        json_list = get_json_objs_from_directory(directory, name)
    json_list = filter_jsons_by_parallelism(json_list, parallelism)
    json_list = filter_jsons_by_chaining(json_list, chaining)
    json_list = filter_jsons_by_sampling_rate(json_list, sampling_rate)
    json_list = filter_jsons_by_tuple_rate(json_list, tuple_rate)

    json_list.sort(key=lambda j: j['batch size'][0])

    if not json_list:
        print('No data found with the specified parameters, not plotting...')
        return
    time_unit = json_list[0]['time unit']
    x_axis = [j['batch size'][0] for j in json_list]
    y_axis = [j[percentile_to_dictkey(percentile)] for j in json_list]
    if DEBUG:
        print(x_axis)
        print(y_axis)

    title = (name.capitalize() + '(' + percentile + ') ' + '(parallelism: ' +
             str(parallelism) + ') ' + '(chaining: ' + str(chaining) + ') ')
    xlabel = 'Batch size for each node'
    ylabel = get_y_label(name, time_unit)
    if save_images:
        save_graph_images(x_axis,
                          y_axis,
                          title=title,
                          xlabel=xlabel,
                          ylabel=ylabel)
    else:
        show_graphs(x_axis, y_axis, title=title, xlabel=xlabel, ylabel=ylabel)


def plot_by_chaining(percentile,
                     directory,
                     name,
                     parallelism,
                     batchsize,
                     sampling_rate=100,
                     tuple_rate=1000,
                     json_list=[],
                     save_images=False):
    CHAINING = 'chaining enabled'

    if not json_list:
        json_list = get_json_objs_from_directory(directory, name)
    json_list = filter_jsons_by_parallelism(json_list, parallelism)
    json_list = filter_jsons_by_batch_size(json_list, batchsize)
    json_list = filter_jsons_by_sampling_rate(json_list, sampling_rate)
    json_list = filter_jsons_by_tuple_rate(json_list, tuple_rate)

    for j in json_list:
        j[CHAINING] = int(j[CHAINING])
    json_list.sort(key=lambda j: j[CHAINING])

    if not json_list:
        print('No data found with the specified parameters, not plotting...')
        return
    time_unit = json_list[0]['time unit']
    x_axis = [j[CHAINING] for j in json_list]
    y_axis = [j[percentile_to_dictkey(percentile)] for j in json_list]
    if DEBUG:
        print(x_axis)
        print(y_axis)

    title = (name.capitalize() + '(' + percentile + ') ' + '(parallelism: ' +
             str(parallelism) + ') ' + '(batch size: ' + str(batchsize) + ') ')
    xlabel = 'Chaining enabled ?'
    ylabel = get_y_label(name, time_unit)

    if save_images:
        save_graph_images(x_axis,
                          y_axis,
                          title=title,
                          xlabel=xlabel,
                          ylabel=ylabel)
    else:
        show_graphs(x_axis, y_axis, title=title, xlabel=xlabel, ylabel=ylabel)
