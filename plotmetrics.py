#!/usr/bin/env python3

import json
import os
import matplotlib.pyplot as plt
import sys

DEBUG = True

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

time_unit_scale_factor = {'ns': 1000000000, 'us': 1000000, 'ms': 1000, 's': 1}

default_batch_sizes = [0, 2, 4, 8, 16, 32, 64, 128]


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
    return [j for j in json_list if j['name'].endswith(name)]


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
    return [
        j for j in json_list
        if ('tuple_rate' in j and j['tuple_rate'] == tuple_rate) or (
            'tuple rate' in j and j['tuple rate'] == tuple_rate)
    ]


def percentile_to_dictkey(kind):
    if kind in ['0th', '5th', '25th', '50th', '75th', '95th', '100th']:
        return kind + ' percentile'
    if kind in ['0', '5', '25', '50', '75', '95', '100']:
        return kind + 'th percentile'
    if kind in [0, 5, 25, 50, 75, 95, 100]:
        return str(kind) + 'th percentile'
    return kind


def get_y_label(name, time_unit):
    time_unit_string = (unit_to_abbrev[time_unit]
                        if time_unit in unit_to_abbrev else 'unknown unit')
    unit_string = (time_unit_string if 'throughput' not in name.lower() else
                   'tuples per second')
    return name.replace('-', ' ').capitalize() + ' (' + unit_string + ')'


def show_graphs(x_axis, y_axis, title, xlabel, ylabel):
    plt.figure()
    plt.title(title, loc='right', y=1.00)
    plt.grid(True)
    plt.plot(x_axis, y_axis, color='maroon', marker='o')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    plt.figure()
    plt.title(title, loc='right', y=1.00)
    plt.grid()
    plt.bar(x_axis, y_axis, color='maroon')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.show()
    plt.close('all')


def save_graph_images(x_axis, y_axis, title, xlabel, ylabel, directory=''):
    filename_title = title.replace('\n', ' ')

    plt.figure()
    plt.title(title, loc='right', y=1.00)
    plt.grid(True)
    plt.plot(x_axis, y_axis, color='maroon', marker='o')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.savefig(os.path.join(directory, filename_title + ' (plot).png'))

    plt.figure()
    plt.grid(False)
    plt.title(title, loc='right', y=1.00)
    plt.bar(x_axis, y_axis, color='maroon')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.savefig(os.path.join(directory, filename_title + ' (bar).png'))
    plt.close('all')


def title_by_parallelism(name, percentile, batchsize, chaining, tuple_rate):
    return (name.capitalize() + ' (' + percentile + ') ' + '(batch size: ' +
            str(batchsize) + ') ' + '\n(chaining: ' + str(chaining) +
            ') (generation rate: ' +
            (str(tuple_rate) if tuple_rate > 0 else 'unlimited') + ')')


def get_y_axis(name, json_list, percentile, time_unit):
    if 'throughput' not in name:
        return [j[percentile_to_dictkey(percentile)] for j in json_list]
    else:
        return [
            time_unit_scale_factor[unit_to_abbrev[time_unit]] *
            j[percentile_to_dictkey(percentile)] for j in json_list
        ]


def plot_by_parallelism(name,
                        batchsize,
                        chaining,
                        directory='',
                        sampling_rate=100,
                        tuple_rate=0,
                        percentile='mean',
                        json_list=None,
                        image_path=None):
    if not json_list:
        json_list = get_json_objs_from_directory(directory)
    json_list = filter_jsons_by_name(json_list, name)
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
    y_axis = get_y_axis(name, json_list, percentile, time_unit)
    if DEBUG:
        print('x_axis: ' + str(x_axis))
        print('y_axis: ' + str(y_axis))

    title = title_by_parallelism(name, percentile, batchsize, chaining,
                                 tuple_rate)
    xlabel = 'Parallelism degree for each node'
    ylabel = get_y_label(name, time_unit)
    if image_path:
        save_graph_images(x_axis,
                          y_axis,
                          title=title,
                          xlabel=xlabel,
                          ylabel=ylabel,
                          directory=image_path)
    else:
        show_graphs(x_axis, y_axis, title=title, xlabel=xlabel, ylabel=ylabel)


def plot_by_batch_size(percentile,
                       name,
                       parallelism,
                       chaining,
                       directory='',
                       sampling_rate=100,
                       tuple_rate=0,
                       json_list=None,
                       image_path=None):
    if not json_list:
        json_list = get_json_objs_from_directory(directory)
    json_list = filter_jsons_by_name(json_list, name)
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
        print('x_axis: ' + str(x_axis))
        print('y_axis: ' + str(y_axis))

    title = (name.capitalize() + '(' + percentile + ') ' + '(parallelism: ' +
             str(parallelism) + ') ' + '\n(chaining: ' + str(chaining) + ')')
    xlabel = 'Batch size for each node'
    ylabel = get_y_label(name, time_unit)
    if image_path:
        save_graph_images(x_axis,
                          y_axis,
                          title=title.replace('\n', ' '),
                          xlabel=xlabel,
                          ylabel=ylabel,
                          directory=image_path)
    else:
        show_graphs(x_axis, y_axis, title=title, xlabel=xlabel, ylabel=ylabel)


def plot_by_chaining(percentile,
                     name,
                     parallelism,
                     batchsize,
                     directory='',
                     sampling_rate=100,
                     tuple_rate=0,
                     json_list=None,
                     image_path=None):
    CHAINING = 'chaining enabled'

    if not json_list:
        json_list = get_json_objs_from_directory(directory)
    json_list = filter_jsons_by_name(json_list, name)
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
        print('x_axis: ' + str(x_axis))
        print('y_axis: ' + str(y_axis))

    title = (name.capitalize() + '(' + percentile + ') ' + '(parallelism: ' +
             str(parallelism) + ') ' + '(batch size: ' + str(batchsize) + ') ')
    xlabel = 'Chaining enabled ?'
    ylabel = get_y_label(name, time_unit)

    if image_path:
        save_graph_images(x_axis,
                          y_axis,
                          title=title,
                          xlabel=xlabel,
                          ylabel=ylabel,
                          directory=image_path)
    else:
        show_graphs(x_axis, y_axis, title=title, xlabel=xlabel, ylabel=ylabel)


def get_percentile_values(percentile_dict, percentile_list):
    percentile_values = []
    for percentile in percentile_list:
        index = str(percentile) + 'th percentile'
        percentile_values.append(percentile_dict[index])
    return percentile_values


def boxplot_latency_by_parallelism(batchsize,
                                   chaining,
                                   directory='',
                                   sampling_rate=100,
                                   tuple_rate=1000,
                                   percentile_list=None,
                                   json_list=None,
                                   image_path=None):
    if not json_list:
        json_list = get_json_objs_from_directory(directory)
    json_list = filter_jsons_by_name(json_list, 'latency')
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
    if not percentile_list:
        percentile_list = [0, 5, 25, 50, 75, 95, 100]
    y_axis = [get_percentile_values(j, percentile_list) for j in json_list]
    if DEBUG:
        print('x_axis: ' + str(x_axis))
        print('y_axis: ' + str(y_axis))

    title = ('Latency (batch size: ' + str(batchsize) + ') ' + '(chaining: ' +
             str(chaining) + ')\nPercentiles: ' + str(percentile_list))
    xlabel = 'Parallelism degree for each node'
    ylabel = get_y_label('latency', time_unit)

    plt.figure()
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title, loc='right', y=1.00)
    plt.grid(True)
    plt.boxplot(y_axis, positions=x_axis)
    if image_path:
        plt.savefig(os.path.join(image_path, title + ' (boxplot).png'))
    else:
        plt.show()
    plt.close('all')


def plot_by_parallelism_comparing_batch_sizes(name,
                                              directory='',
                                              chaining=False,
                                              batch_sizes=None,
                                              sampling_rate=100,
                                              tuple_rate=0,
                                              percentile='mean',
                                              json_list=None,
                                              image_path=None):
    if not json_list:
        json_list = get_json_objs_from_directory(directory)
    json_list = filter_jsons_by_name(json_list, name)
    json_list = filter_jsons_by_chaining(json_list, chaining)
    json_list = filter_jsons_by_sampling_rate(json_list, sampling_rate)
    json_list = filter_jsons_by_tuple_rate(json_list, tuple_rate)

    json_list.sort(key=lambda j: j['parallelism'][0])

    if not json_list:
        print('No data found with the specified parameters, not plotting...')
        return

    time_unit = json_list[0]['time unit']
    title = (name.capitalize() + ' (' + percentile + ') ' + ' (chaining: ' +
             str(chaining) + ') (generation rate: ' +
             (str(tuple_rate if tuple_rate > 0 else 'unlimited') + ')'))

    xlabel = 'Parallelism degree for each node'
    ylabel = get_y_label(name, time_unit)

    plt.figure()
    plt.title(title, y=1.08)
    plt.grid(True)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    if not batch_sizes:
        batch_sizes = default_batch_sizes
    for batch_size in batch_sizes:
        current_json_list = filter_jsons_by_batch_size(json_list, batch_size)
        x_axis = [j['parallelism'][0] for j in current_json_list]
        y_axis = get_y_axis(name, current_json_list, percentile, time_unit)
        if DEBUG:
            print('x_axis: ' + str(x_axis))
            print('y_axis: ' + str(y_axis))
        batch_size_label = str(current_json_list[0]['batch size'])
        plt.plot(x_axis, y_axis, label='Batch size: ' + batch_size_label)
    plt.legend()
    if image_path:
        plt.savefig(
            os.path.join(image_path, title + ' (batch size comparison).png'))
    else:
        plt.show()
    plt.close('all')


def plot_by_parallelism_comparing_chaining(name,
                                           directory='',
                                           batch_size=0,
                                           sampling_rate=100,
                                           tuple_rate=0,
                                           percentile='mean',
                                           json_list=None,
                                           image_path=None):
    if not json_list:
        json_list = get_json_objs_from_directory(directory)
    json_list = filter_jsons_by_name(json_list, name)
    json_list = filter_jsons_by_batch_size(json_list, batch_size)
    json_list = filter_jsons_by_sampling_rate(json_list, sampling_rate)
    json_list = filter_jsons_by_tuple_rate(json_list, tuple_rate)

    json_list.sort(key=lambda j: j['parallelism'][0])

    if not json_list:
        print('No data found with the specified parameters, not plotting...')
        return

    time_unit = json_list[0]['time unit']
    title = (name.capitalize() + ' (' + percentile + ') (batch size: ' +
             str(batch_size) + ') (generation rate: ' +
             (str(tuple_rate if tuple_rate > 0 else 'unlimited') + ')'))

    xlabel = 'Parallelism degree for each node'
    ylabel = get_y_label(name, time_unit)

    plt.figure()
    plt.title(title, y=1.08)
    plt.grid(True)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    for chaining in [False, True]:
        current_json_list = filter_jsons_by_chaining(json_list, chaining)
        x_axis = [j['parallelism'][0] for j in current_json_list]
        y_axis = get_y_axis(name, current_json_list, percentile, time_unit)
        if DEBUG:
            print('x_axis: ' + str(x_axis))
            print('y_axis: ' + str(y_axis))
        plt.plot(x_axis, y_axis, label='Chaining: ' + str(chaining))
    plt.legend()
    if image_path:
        plt.savefig(
            os.path.join(image_path, title + ' (batch size comparison).png'))
    else:
        plt.show()
    plt.close('all')


def plot_by_batch_size_comparing_parallelism(name,
                                             directory='',
                                             chaining=False,
                                             parallelism_degrees=None,
                                             sampling_rate=100,
                                             tuple_rate=0,
                                             json_list=None,
                                             image_path=None,
                                             percentile='mean'):
    if not json_list:
        json_list = get_json_objs_from_directory(directory)
    json_list = filter_jsons_by_name(json_list, name)
    json_list = filter_jsons_by_chaining(json_list, chaining)
    json_list = filter_jsons_by_sampling_rate(json_list, sampling_rate)
    json_list = filter_jsons_by_tuple_rate(json_list, tuple_rate)

    json_list.sort(key=lambda j: j['batch size'][0])
    if not json_list:
        print('No data found with the specified parameters, not plotting...')
        return

    time_unit = json_list[0]['time unit']
    title = (name.capitalize() + ' (' + percentile + ') ' + ' (chaining: ' +
             str(chaining) + ') (generation rate: ' +
             (str(tuple_rate if tuple_rate > 0 else 'unlimited') + ')'))
    xlabel = 'Batch size for each node'
    ylabel = get_y_label(name, time_unit)

    plt.figure()
    plt.title(title, y=1.08)
    plt.grid(True)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    if not parallelism_degrees:
        parallelism_degrees = range(1, 48)
    for degree in parallelism_degrees:
        current_json_list = filter_jsons_by_parallelism(json_list, degree)
        x_axis = [j['batch size'][0] for j in current_json_list]
        y_axis = get_y_axis(name, current_json_list, percentile, time_unit)
        if DEBUG:
            print('x_axis: ' + str(x_axis))
            print('y_axis: ' + str(y_axis))
        plt.plot(x_axis, y_axis, label='Parallelism degree: ' + str(degree))
    plt.legend()
    if image_path:
        plt.savefig(
            os.path.join(image_path,
                         title + ' (parallelism degree comparison).png'))
    else:
        plt.show()
    plt.close('all')


def generate_all_images(directory,
                        by_parallelism=False,
                        by_batch_size=False,
                        by_chaining=False,
                        metrics=None):
    parallelism_degrees = range(1, 25)
    batchsizes = [0, 2, 4, 8, 16, 32, 64, 128]
    chaining_vals = [False, True]
    if not metrics:
        metrics = ['throughput', 'service-time', 'latency']

    json_list = get_json_objs_from_directory(directory)
    if by_parallelism:
        for batchsize in batchsizes:
            for chaining in chaining_vals:
                for metric in metrics:
                    if DEBUG:
                        print('Plotting by parallelism for batchsize=' +
                              str(batchsize) + ', chaining=' + str(chaining) +
                              ' and metric=' + metric)
                    plot_by_parallelism(metric,
                                        batchsize,
                                        chaining,
                                        percentile='mean',
                                        json_list=json_list,
                                        image_path=directory,
                                        tuple_rate=0)
    if by_batch_size:
        for parallelism in parallelism_degrees:
            for chaining in chaining_vals:
                for metric in metrics:
                    plot_by_batch_size(metric,
                                       parallelism,
                                       chaining,
                                       percentile='mean',
                                       json_list=json_list,
                                       image_path=directory,
                                       tuple_rate=0)
    if by_chaining:
        for parallelism in parallelism_degrees:
            for batchsize in batchsizes:
                for metric in metrics:
                    plot_by_chaining(metric,
                                     parallelism,
                                     batchsize,
                                     percentile='mean',
                                     json_list=json_list,
                                     image_path=directory,
                                     tuple_rate=0)


def save_images_by_parallelism_comparing_batch_sizes(directory,
                                                     metric='throughput'):
    for chaining in [False, True]:
        plot_by_parallelism_comparing_batch_sizes('mean',
                                                  metric,
                                                  directory=directory,
                                                  chaining=chaining,
                                                  image_path=directory)


def generate_boxplots(directory):
    # parallelism_degrees = range(1, 25)
    batchsizes = [0, 10, 100, 1000, 10000]
    chaining_vals = [False, True]

    json_list = get_json_objs_from_directory(directory)

    for batchsize in batchsizes:
        for chaining in chaining_vals:
            boxplot_latency_by_parallelism(batchsize,
                                           chaining,
                                           percentile_list=[25, 50, 75],
                                           json_list=json_list,
                                           image_path=directory)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('Run as ' + sys.argv[0] + ' <test result directory>')
    directory = sys.argv[1]
    if DEBUG:
        print('Generating graphs and saving images to' + directory + '...')
    generate_all_images(directory)
