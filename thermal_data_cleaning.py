'''Brandon Hastings'''
'''split species into individual arrays'''
'''eliminate days where over 50% of individuals don't move in temperature, replace whole day's values with nan'''
# import required packages
import pandas as pd
import numpy as np
import argparse
from pathlib import Path
import math

'''import dataset and structure so that header line contains individual ID, remove Date/Time and empty columns 
to prepare for cleaning of data
dataset is path to folder holding data, data_path is where to save cleaned data as a csv
 night is bool that will remove overnight data if True;
distance for nearest neighbor, default is 3
night and distance declared in function call'''


def main(dataset, data_path, night, distance):
    # check if data_path ends in .csv from user input during function call
    if data_path[-4:] == '.csv':
        pass
    # if it doesn't, it will
    else:
        data_path = data_path + '.csv'
    # Path data_path for cross platform use
    Path(data_path)
    # Path input_data for cross platform use
    input_dataset = Path(dataset)
    # import csv holding data starting from line 2 where individual ID headers are, remove corrupted formatting
    dataset = pd.read_csv(input_dataset, delimiter=',', header=1, error_bad_lines=False)
    # remove columns holding any unnamed columns, left with only date and temp of individuals
    dataset = dataset[dataset.columns.drop(list(dataset.filter(regex='Control')))]
    dataset.columns = dataset.iloc[0]
    # drop unnecessary first column
    filtered_data = dataset.drop(0)
    # store date/time column for constructing the cleaned array later
    time = filtered_data.iloc[0:, 0:1]
    # remove columns holding date/time info, left with only temp of individuals
    data = filtered_data[filtered_data.columns.drop(list(filtered_data.filter(regex='Date')))]
    # save as csv and read in new csv from data_path location, will be overwritten later
    data.to_csv(data_path)
    # find column names and use that column on second import, selecting only names
    data = pd.read_csv(data_path, delimiter=',', error_bad_lines=False)
    data = data.iloc[0:, 1:]

    '''input prepared array from above, make key value pairs of headers and columns to iterate through,
    return dictionary'''

    def make_dict(array):
        # store each column as a dictionary entry, with headers as key and column data as values
        array_dict = {}
        for i in range(0, len(array.columns)):
            name = array.columns[i][0:].lower()
            array_dict[name] = array.iloc[:, i].values
        return array_dict

    # will run if user selects to remove overnight data. turns all data points taken between 8pm and 5am to nans
    # returns dictionary for input into find_anomalies_nighttime
    def nighttime(dict_of_array):
        # import dictionary of array with headers as keys and column data as values
        new_arr = []
        # iterate through each column (individual)
        for value in dict_of_array.values():
            new_value = []
            n = 0
            # split each column into chunks of 72 consecutive values, stored as tuples
            list_of_slices = list((zip(*(iter(value),) * 72)))
            np.array(list_of_slices)
            for chunk in list_of_slices:
                day = np.array(list_of_slices[n])
                n = n + 1
                # nan ends of day array
                start = next(idx for idx, val in enumerate(day) if idx > 13)
                end = len(day) - next(idx for idx, val in enumerate(reversed(day)) if idx > 9)
                day[:start] = np.nan
                day[end:] = np.nan
                # build back modified column day by day throughout the iteration
                new_value.extend(day)
            # build back modified array by each column
            new_arr.append(new_value)
        # convert pandas array to numpy, swap axes, and save as dictionary
        new_arr = np.array(new_arr)
        new_arr = np.swapaxes(new_arr, 0, 1)
        new_data = pd.DataFrame(data=new_arr, columns=dict_of_array.keys())
        new_dict = {}
        for i in range(0, len(new_data.columns)):
            name = new_data.columns[i][0:].lower()
            new_dict[name] = new_data.iloc[:, i].values
        return new_dict

    '''will only run if user selects to remove nighttime data
    for each individual (column) if temp +- 1C is the same for entire day (48 cells) return coordinates
    of those days. returns x, y coordinates (axis 1, axis 0)'''

    def find_anomalies_nighttime(dict_of_array):
        # import dictionary of array with headers as keys and column data as values
        # create empty lists for coordinates of inactive days, values are tuples of axis1, axis0 coordinates
        placement = []
        new_arr = []
        active = 0
        inactive = 0
        x = 0
        for value in dict_of_array.values():
            new_value = []
            y = 0
            n = 0
            # split each column into chunks of 72 consecutive values, stored as tuples
            list_of_slices = list((zip(*(iter(value),) * 72)))
            # iterate through each chunk
            for chunk in list_of_slices:
                day = np.array(list_of_slices[n])
                # find average of each chunk. Check each value against average, stored in list "check"
                avg = np.nanmean(day)
                check = [abs(avg - i) for i in chunk[14:62]]
                # if all values in a chunk are within 1 of the average, turn values of chunk to nan (or otherwise
                # revoke the data)
                n = n + 1
                if all(i <= 1 for i in check):
                    new_value.extend(chunk)
                    inactive = inactive + 1
                    coordinates = x, y
                    placement.append(coordinates)
                else:
                    new_value.extend(chunk)
                    active = active + 1
                y = y + 1
            x = x + 1
            new_arr.append(new_value)
        new_arr = np.array(new_arr)
        new_arr = np.swapaxes(new_arr, 0, 1)
        print("Omitting overnight data")
        print("Active: ", active)
        print("Inactive: ", inactive)
        # return coordinates of inactive days as list of tuples
        return placement

    # for each individual (column) if temp +- 1C is the same for entire day (72 cells) return coordinates
    # of those days. returns x, y coordinates (axis 1, axis 0). See previous function for in-function comment notes
    def find_anomalies_full_day(dict_of_array):
        # import dictionary of array with headers as keys and column data as values
        placement = []
        new_arr = []
        active = 0
        inactive = 0
        x = 0
        for value in dict_of_array.values():
            new_value = []
            y = 0
            n = 0
            # split each column into chunks of 72 consecutive values, stored as tuples
            list_of_slices = list((zip(*(iter(value),) * 72)))
            for chunk in list_of_slices:
                day = np.array(list_of_slices[n])
                # find average of each chunk. Check each value against average, stored in list "check"
                avg = np.average(day)
                check = [abs(avg - i) for i in chunk]
                # if all values in a chunk are within 1 of the average, turn values of chunk to nan (or otherwise
                # revoke the data)
                n = n + 1
                if all(i <= 1 for i in check):
                    new_value.extend(chunk)
                    inactive = inactive + 1
                    coordinates = x, y
                    placement.append(coordinates)
                else:
                    new_value.extend(chunk)
                    active = active + 1
                y = y + 1
            x = x + 1
            new_arr.append(new_value)
        new_arr = np.array(new_arr)
        new_arr = np.swapaxes(new_arr, 0, 1)
        print("Using data points from the whole day")
        print("Active: ", active)
        print("Inactive: ", inactive)
        return placement

    # nearest neighbor search:
    # conceptualizing a 2d array of shape(days, individuals), the distance between each coordinate (inactive day)
    # produced in previous function is calculated. if equal to or below distance threshold (default 3).
    # Returns original array, with inactive days below nearest neighbor threshold now holding nans
    def nearest_neighbor(array, placement):
        # below function finds distance between two given coordinates
        def get_distance(data1, data2):
            points = zip(data1, data2)
            diffs_squared_distance = [pow(a - b, 2) for (a, b) in points]
            dist = math.sqrt(sum(diffs_squared_distance))
            # if distance is below threshold, add each to set (no repeats)
            if dist <= distance:
                clusters.add(data1)
                clusters.add(data2)

        # tests the distance of one inactive day to every other
        clusters = set()
        for i, object1 in enumerate(placement):
            for j in range(i + 1, len(placement)):
                object2 = placement[j]
                get_distance(object1, object2)
        # create new array to fill in inactive days in distance of nearest neighbor with nans
        new_arr = pd.DataFrame.to_numpy(array)
        for i, j in clusters:
            new_arr[(j * 72):((j * 72) + 72), i] = np.ma.masked_where(~np.isnan(new_arr[(j * 72):((j * 72) + 72), i]),
                                                                      new_arr[(j * 72):((j * 72) + 72), i], copy=False)
            new_arr[(j * 72):((j * 72) + 72), i] = np.nan
        print("Days removed with nearest neighbor: ", len(clusters))
        return new_arr

    # save modified array as csv with original headers and timestamps. (delete first cell of time data in excel)
    def save_csv(new_arr, datetime, array_dict):
        new_data = pd.DataFrame(data=new_arr, columns=array_dict.keys())
        joined = pd.concat([datetime, new_data], axis=1)
        joined.to_csv(data_path, na_rep='NaN')

    # based on optional arguments of initial function call, run certain functions
    if night is True:
        # if night is true, run nighttime function to remove overnight data
        save_csv(nearest_neighbor(data, find_anomalies_nighttime(nighttime(make_dict(data)))), time, make_dict(data))
    else:
        # else, keep overnight data
        save_csv(nearest_neighbor(data, find_anomalies_full_day(make_dict(data))), time, make_dict(data))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='remove occurrences of no activity')

    parser.add_argument('dataset', type=str, help='input folder holding dataset')
    parser.add_argument('data_path', type=str, help='input desired output path WITH filename at end')
    parser.add_argument('-night', '--night', type=bool, default=False, help='input "-night True" to remove overnight '
                                                                            'data, default is False')
    parser.add_argument('-distance', '--distance', type=int, default=3, help='input distance for nearest neighbor '
                                                                             'analysis, default is 3')

    args = parser.parse_args()

    main(args.dataset, args.data_path, args.night, args.distance)
