import csv
import time


class LabeledDay(object):
    def __init__(self, day, label):
        self.day = day
        self.label = label

    def to_tuple(self):
        return (self.day.date, self.day.open_val, self.day.high, self.day.low, self.day.close, self.day.volume, self.label)


class Day(object):
    def __init__(self, date, open_val, high, low, close, volume):
        self.date = date
        self.open_val = open_val
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume


def load_csv(filename):
    labeled_days = []
    days = []
    with open(filename) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        count = 0
        for row in readCSV:
            if count != 0:
                row1 = row[0]
                pattern = '%Y-%m-%d'
                epoch = int(time.mktime(time.strptime(row1, pattern)))
                day = Day(epoch, float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5]))
                days.append(day)
            count += 1

    # take a day -> take 10 days after, average them and compare them
    # edge case end of the array average whats left
    size = len(days)
    for i, day in enumerate(days):
        label = 0
        day_value = day.close
        average_fwd_val = 0
        temp_count = 0
        for j in range(10):
            if j+i < size:
                average_fwd_val += days[i+j].close
                temp_count += 1

        average_fwd_val = average_fwd_val / temp_count
        if average_fwd_val > day_value:
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


filenam = "aa.us.txt"
labeled_days = load_csv(filenam)
create_csv(labeled_days, filenam)
