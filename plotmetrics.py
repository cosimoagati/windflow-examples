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


def json_name_match(entry, name):
    return entry.endswith(name) or entry.replace(
        '-', ' ').endswith(name) or entry.replace(' ', '-').endswith(name)


def filter_jsons_by_name(json_list, name):
    return [j for j in json_list if json_name_match(j['name'], name)]


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


def get_y_axis(name, json_list, percentile, time_unit):
    if 'throughput' not in name:
        return [j[percentile_to_dictkey(percentile)] for j in json_list]
    else:
        return [
            time_unit_scale_factor[unit_to_abbrev[time_unit]] *
            j[percentile_to_dictkey(percentile)] for j in json_list
        ]


def scale_by_base_value(base_value, y, measure):
    return y / base_value if measure == 'throughput' else base_value / y


def get_scaled_y_axis(name, json_list, percentile, base_value):
    unscaled_y_axis = [j[percentile_to_dictkey(percentile)] for j in json_list]
    return [scale_by_base_value(base_value, y, name) for y in unscaled_y_axis]


def get_efficiency_y_axis(name, json_list, percentile, base_value):
    scaled_y_axis = get_scaled_y_axis(name, json_list, percentile, base_value)
    for i in range(len(scaled_y_axis)):
        scaled_y_axis[i] = scaled_y_axis[i] / (i + 1)
    return scaled_y_axis


def get_percentile_values(percentile_dict, percentiles):
    percentile_values = []
    for percentile in percentiles:
        index = str(percentile) + 'th percentile'
        percentile_values.append(percentile_dict[index])
    return percentile_values


def boxplot_latency_by_parallelism(metric='latency',
                                   batch_size=0,
                                   chaining=False,
                                   directory='',
                                   sampling_rate=100,
                                   tuple_rate=0,
                                   percentiles=None,
                                   json_list=None,
                                   image_path=None):
    if not json_list:
        json_list = get_json_objs_from_directory(directory)
    json_list = filter_jsons_by_name(json_list, metric)
    json_list = filter_jsons_by_chaining(json_list, chaining)
    json_list = filter_jsons_by_batch_size(json_list, batch_size)
    json_list = filter_jsons_by_sampling_rate(json_list, sampling_rate)
    json_list = filter_jsons_by_tuple_rate(json_list, tuple_rate)

    json_list.sort(key=lambda j: j['parallelism'][0])

    if not json_list:
        print('No data found with the specified parameters, not plotting...')
        return

    time_unit = json_list[0]['time unit']
    x_axis = [j['parallelism'][0] for j in json_list]
    if not percentiles:
        percentiles = [0, 5, 25, 50, 75, 95, 100]
    y_axis = [get_percentile_values(j, percentiles) for j in json_list]
    if DEBUG:
        print('x_axis: ', x_axis)
        print('y_axis: ', y_axis)

    title = (metric.capitalize().replace('-', ' ') + ' (batch size: ' +
             str(batch_size) + ') ' + '(chaining: ' + str(chaining) +
             ')\nPercentiles: ' + str(percentiles))
    xlabel = 'Parallelism degree for each node'
    ylabel = get_y_label(metric, time_unit)

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


def plot_by_parallelism_compare_batch_sizes(name,
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
    title = (name.capitalize().replace('-', ' ') + ' (' + percentile +
             ') (chaining: ' + str(chaining) + ') (generation rate: ' +
             (str(tuple_rate if tuple_rate > 0 else 'unlimited') + ')'))

    xlabel = 'Parallelism degree for each node'
    ylabel = get_y_label(name, time_unit)

    plt.figure()
    plt.title(title, y=1.08)
    plt.grid(True)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    batch_sizes = batch_sizes if batch_sizes else default_batch_sizes
    for batch_size in batch_sizes:
        current_json_list = filter_jsons_by_batch_size(json_list, batch_size)
        x_axis = [j['parallelism'][0] for j in current_json_list]
        y_axis = get_y_axis(name, current_json_list, percentile, time_unit)
        if DEBUG:
            print('x_axis: ', x_axis)
            print('y_axis: ', y_axis)
        batch_size_label = str(current_json_list[0]['batch size'])
        plt.plot(x_axis, y_axis, label='Batch size: ' + batch_size_label)
    plt.legend()
    if image_path:
        suffix = ('-batch-sizes-' +
                  str(batch_sizes).removeprefix('[').removesuffix(']').replace(
                      ', ', '-') + '.png')
        plt.savefig(os.path.join(image_path, title + suffix))
    else:
        plt.show()
    plt.close('all')


def plot_by_parallelism_compare_chaining(name,
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
    title = (name.capitalize().replace('-', ' ') + ' (' + percentile +
             ') (batch size: ' + str(batch_size) + ') (generation rate: ' +
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
            print('x_axis: ', x_axis)
            print('y_axis: ', y_axis)
        plt.plot(x_axis, y_axis, label='Chaining: ' + str(chaining))
    plt.legend()
    if image_path:
        plt.savefig(
            os.path.join(image_path, title + '-chaining-comparison.png'))
    else:
        plt.show()
    plt.close('all')


def plot_by_batch_size_compare_parallelism(name,
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
    title = (name.capitalize().replace('-', ' ') + ' (' + percentile + ') ' +
             ' (chaining: ' + str(chaining) + ') (generation rate: ' +
             (str(tuple_rate if tuple_rate > 0 else 'unlimited') + ')'))
    xlabel = 'Batch size for each node'
    ylabel = get_y_label(name, time_unit)

    plt.figure()
    plt.title(title, y=1.08)
    plt.grid(True)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    parallelism_degrees = (parallelism_degrees
                           if parallelism_degrees else range(1, 48))
    for degree in parallelism_degrees:
        current_json_list = filter_jsons_by_parallelism(json_list, degree)
        x_axis = [j['batch size'][0] for j in current_json_list]
        y_axis = get_y_axis(name, current_json_list, percentile, time_unit)
        if DEBUG:
            print('x_axis: ', x_axis)
            print('y_axis: ', y_axis)
        plt.plot(x_axis, y_axis, label='Parallelism degree: ' + str(degree))
    plt.legend()
    if image_path:
        plt.savefig(
            os.path.join(image_path,
                         title + ' (parallelism degree comparison).png'))
    else:
        plt.show()
    plt.close('all')


def plot_by_batch_size_compare_chaining(name,
                                        directory='',
                                        parallelism_degree=1,
                                        sampling_rate=100,
                                        tuple_rate=0,
                                        json_list=None,
                                        image_path=None,
                                        percentile='mean'):
    if not json_list:
        json_list = get_json_objs_from_directory(directory)
    json_list = filter_jsons_by_name(json_list, name)
    json_list = filter_jsons_by_parallelism(json_list, parallelism_degree)
    json_list = filter_jsons_by_sampling_rate(json_list, sampling_rate)
    json_list = filter_jsons_by_tuple_rate(json_list, tuple_rate)

    json_list.sort(key=lambda j: j['batch size'][0])
    if not json_list:
        print('No data found with the specified parameters, not plotting...')
        return

    time_unit = json_list[0]['time unit']
    title = (name.capitalize().replace('-', ' ') + ' (' + percentile + ') ' +
             ' (parallelism degree per node: ' + str(parallelism_degree) +
             ') (generation rate: ' +
             (str(tuple_rate if tuple_rate > 0 else 'unlimited') + ')'))
    xlabel = 'Batch size for each node'
    ylabel = get_y_label(name, time_unit)

    plt.figure()
    plt.title(title, y=1.08)
    plt.grid(True)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    for chaining in [False, True]:
        current_json_list = filter_jsons_by_chaining(json_list, chaining)
        x_axis = [j['batch size'][0] for j in current_json_list]
        y_axis = get_y_axis(name, current_json_list, percentile, time_unit)
        if DEBUG:
            print('x_axis: ', x_axis)
            print('y_axis: ', y_axis)
        plt.plot(x_axis, y_axis, label='Chaining: ' + str(chaining))
    plt.legend()
    if image_path:
        plt.savefig(
            os.path.join(image_path, title + ' (chaining comparison).png'))
    else:
        plt.show()
    plt.close('all')


def plot_scalablity_compare_batch_sizes(name,
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

    title = (name.capitalize().replace('-', ' ') + ' (' + percentile +
             ') (chaining: ' + str(chaining) + ') (generation rate: ' +
             (str(tuple_rate if tuple_rate > 0 else 'unlimited') + ')'))

    xlabel = 'Parallelism degree for each node'
    ylabel = 'Ratio with respect to parallelism degree of one per node'

    plt.figure()
    plt.title(title, y=1.08)
    plt.grid(True)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    batch_sizes = batch_sizes if batch_sizes else default_batch_sizes
    for batch_size in batch_sizes:
        current_json_list = filter_jsons_by_batch_size(json_list, batch_size)
        current_json_list.sort(key=lambda j: j['parallelism'][0])
        base_value = current_json_list[0][percentile_to_dictkey(percentile)]
        x_axis = [j['parallelism'][0] for j in current_json_list]
        y_axis = get_scaled_y_axis(name, current_json_list, percentile,
                                   base_value)
        if DEBUG:
            print('base_value: ', base_value)
            print('x_axis: ', x_axis)
            print('y_axis: ', y_axis)
        batch_size_label = str(current_json_list[0]['batch size'])
        plt.plot(x_axis, y_axis, label='Batch size: ' + batch_size_label)

    plt.legend()
    if image_path:
        suffix = ('-batch-sizes-' +
                  str(batch_sizes).removeprefix('[').removesuffix(']').replace(
                      ', ', '-') + '.png')
        plt.savefig(os.path.join(image_path, title + suffix))
    else:
        plt.show()
    plt.close('all')


def plot_efficiency_compare_batch_sizes(name,
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

    title = (name.capitalize().replace('-', ' ') + ' (' + percentile +
             ') (chaining: ' + str(chaining) + ') (generation rate: ' +
             (str(tuple_rate if tuple_rate > 0 else 'unlimited') + ')'))

    xlabel = 'Parallelism degree for each node'
    ylabel = 'Efficiency with respect to parallelism degree of one per node'

    plt.figure()
    plt.title(title, y=1.08)
    plt.grid(True)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    batch_sizes = batch_sizes if batch_sizes else default_batch_sizes
    for batch_size in batch_sizes:
        current_json_list = filter_jsons_by_batch_size(json_list, batch_size)
        current_json_list.sort(key=lambda j: j['parallelism'][0])
        base_value = current_json_list[0][percentile_to_dictkey(percentile)]
        x_axis = [j['parallelism'][0] for j in current_json_list]
        y_axis = get_efficiency_y_axis(name, current_json_list, percentile,
                                       base_value)
        if DEBUG:
            print('base_value: ', base_value)
            print('x_axis: ', x_axis)
            print('y_axis: ', y_axis)
        batch_size_label = str(current_json_list[0]['batch size'])
        plt.plot(x_axis, y_axis, label='Batch size: ' + batch_size_label)

    plt.legend()
    if image_path:
        suffix = ('-batch-sizes-' +
                  str(batch_sizes).removeprefix('[').removesuffix(']').replace(
                      ', ', '-') + '.png')
        plt.savefig(os.path.join(image_path, title + suffix))
    else:
        plt.show()
    plt.close('all')


def generate_all_images_by_parallelism(directory, metrics):
    batchsizes = [0, 2, 4, 8, 16, 32, 64, 128]
    if not metrics:
        metrics = ['throughput', 'service time', 'latency']

    json_list = get_json_objs_from_directory(directory)
    for chaining in [False, True]:
        for metric in metrics:
            plot_by_parallelism_compare_batch_sizes(metric,
                                                    chaining=chaining,
                                                    percentile='mean',
                                                    json_list=json_list,
                                                    image_path=directory,
                                                    tuple_rate=0)
    for batch_size in batchsizes:
        for metric in metrics:
            plot_by_parallelism_compare_chaining(metric,
                                                 batch_size=batch_size,
                                                 percentile='mean',
                                                 json_list=json_list,
                                                 image_path=directory,
                                                 tuple_rate=0)


def generate_all_images_by_batch_size(directory, metrics):
    pass


def generate_all_images_by_chaining(directory, metrics):
    pass


def generate_all_images(directory,
                        by_parallelism=False,
                        by_batch_size=False,
                        by_chaining=False,
                        metrics=None):
    if by_parallelism:
        generate_all_images_by_parallelism(directory, metrics)
    if by_batch_size:
        generate_all_images_by_batch_size(directory, metrics)
    if by_chaining:
        generate_all_images_by_chaining(directory, metrics)


def generate_boxplots(directory):
    # parallelism_degrees = range(1, 25)
    batchsizes = [0, 10, 100, 1000, 10000]
    chaining_vals = [False, True]

    json_list = get_json_objs_from_directory(directory)

    for batchsize in batchsizes:
        for chaining in chaining_vals:
            boxplot_latency_by_parallelism(batchsize,
                                           chaining,
                                           percentiles=[25, 50, 75],
                                           json_list=json_list,
                                           image_path=directory)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('Run as ' + sys.argv[0] + ' <test result directory>')
    directory = sys.argv[1]
    if DEBUG:
        print('Generating graphs and saving images to' + directory + '...')
    generate_all_images(directory)
