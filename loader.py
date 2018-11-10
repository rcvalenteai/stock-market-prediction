import csv
import time
from datetime import datetime
import os
import multiprocessing
import threading


class LabeledDay(object):
    def __init__(self, day, label):
        self.day = day
        self.label = label

    def to_tuple(self):
        return (self.day.day, self.day.month, self.day.year, self.day.weekday, self.day.open_val, self.day.high, self.day.low, self.day.close, self.day.volume, self.label)


class Day(object):
    def __init__(self, day, month, year, weekday, open_val, high, low, close, volume):
        self.day = day
        self.month = month
        self.year = year
        self.weekday = weekday
        self.open_val = open_val
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume


def load_csv(filename):
    days = []
    with open(filename) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        count = 0
        for row in readCSV:
            if count != 0:
                row1 = row[0]
                pattern = '%Y-%m-%d'
                date = datetime.strptime(row1, pattern)

                try:
                    # epoch = int((datetime.strptime(row1, pattern)).timestamp())
                    day = Day(date.day, date.month, date.year, date.weekday(), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5]))
                    days.append(day)
                except OverflowError:
                    print("Windows Blows Dick: " + row1)
            count += 1
    return days


# data = Day object
# k = number of days to average the forecast
def label_data(data, k):
    labeled_days = []
    size = len(data)
    for i, day in enumerate(data):
        label = 0
        day_value = day.close
        average_fwd_val = 0
        temp_count = 0
        for j in range(k):
            if j+i < size:
                average_fwd_val += data[i+j].close
                temp_count += 1

        average_fwd_val = average_fwd_val / temp_count
        if average_fwd_val > day_value:
            label = 1
        labeled_day = LabeledDay(day, label)
        labeled_days.append(labeled_day)
    return labeled_days


def label_data_faster(data, k):
    labeled_days = []
    size = len(data)
    forecast_values = []
    forecast_val = 0
    for j in range(k):
        if j+1 < size:
            forecast_val += data[j+1].close

    forecast_values.append(forecast_val)
    # i is 1 less than the actual day value
    for i, day in enumerate(data[1:]):
        forecast_val = forecast_values[i]
        if i+k+1 < size:
            forecast_val += data[i + k + 1].close
        if i+1 < size:
            forecast_val -= data[i + 1].close
        forecast_values.append(forecast_val)
    worry = size - k - 1

    for i, day in enumerate(data):
        label = 0
        if i < worry:
            forecast_values[i] = forecast_values[i] / k
        else:
            divider = size - i+1
            forecast_values[i] = forecast_values[i] / divider
        if forecast_values[i] > day.close:
            label = 1
        labeled_day = LabeledDay(day, label)
        labeled_days.append(labeled_day)
    return labeled_days


def create_csv(labeled_days, filename):
    labeled_days_tuple = [day.to_tuple() for day in labeled_days]
    csv_name = filename[:-4] + "_" + "labeled" + ".csv"
    with open(csv_name, 'w', newline='') as out:
        csv_out = csv.writer(out)
        for row in labeled_days_tuple:
            csv_out.writerow(row)
    print("Created CSV File: " + csv_name)


def label_folder(input_folder, output_folder, k):

    path = './' + input_folder
    output_path = './' + output_folder
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    files = os.listdir(path)
    for stock in files:
        labeled_days = load_csv(path + '/' + stock)
        labeled_days = label_data_faster(labeled_days, k)
        print("Created CSV: " + output_path + '/' + stock)
        create_csv(labeled_days, output_path + '/' + stock)


class LabelFiles (threading.Thread):

    def __init__(self, threadID, name, filenames, path, output_path, k):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.filenames = filenames
        self.path = path
        self.output_path = output_path
        self.k = k

    def run(self):
        print("Starting " + self.name)
        for stock in self.filenames:
            labeled_days = load_csv(self.path + '/' + stock)
            labeled_days = label_data_faster(labeled_days, self.k)
            print("Created CSV: " + self.output_path + '/' + stock)
            create_csv(labeled_days, self.output_path + '/' + stock)
        print("Exiting " + self.name)


def label_folder_threaded(input_folder, output_folder, k):
    path = './' + input_folder
    output_path = './' + output_folder
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    files = os.listdir(path)
    size = len(files)
    cpu_cores = int(multiprocessing.cpu_count() / 2)
    if size < cpu_cores:
        cpu_cores = size

    thread_list =  [None] * cpu_cores
    thread_list_size = int(size / cpu_cores)

    for i, thread in enumerate(thread_list):
        if i < cpu_cores:
            thread_filenames = files[i*thread_list_size:(i+1)*thread_list_size]
        else:
            thread_filenames = files[i*thread_list_size:]
        thread_list[i] = LabelFiles(i+1, "Thread-" + str(i+1), thread_filenames, path, output_path, k)
        print("Thread-" + str(i+1) + str(type(thread)))
        thread_list[i].start()

    for i, thread in enumerate(thread_list):
        print(type(thread))
        print("Joining Thread " + str(i+1))
        thread.join()


def label_stocks_efts(k):
    label_folder("ETFs", "ETFs_labeled", k)
    label_folder("Stocks", "Stocks_labeled", k)


label_stocks_efts(10)


