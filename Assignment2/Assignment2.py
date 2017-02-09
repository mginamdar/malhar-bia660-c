import csv
from collections import OrderedDict
from dateutil.parser import parse
import datetime,time

class DataFrame(object):

    @classmethod
    def from_csv(cls, file_path, delimiting_character=',', quote_character='"'):
        with open(file_path, 'rU') as infile:
            reader = csv.reader(infile, delimiter=delimiting_character, quotechar=quote_character)
            data = []

            for row in reader:
                data.append(row)

            return cls(list_of_lists=data)

    # --------------------Task 1---------------------
    def getUniqueHeaders(self,iterable):
        seen = set()
        for item in iterable:
            if item not in seen:
                seen.add(item)
            else:
                raise Exception('Duplicate value found')
                break



    def __init__(self, list_of_lists, header=True):
        if header:
            self.header = list_of_lists[0]
            self.data = list_of_lists[1:]
            self.getUniqueHeaders(self.header)

        else:
            self.header = ['column' + str(index + 1) for index, column in enumerate(list_of_lists[0])]
            self.data = list_of_lists
    #------------------Task 2 -----------------------
        self.data = [map(lambda x: x.strip(), row) for row in self.data]   #Task 2
        self.data = [OrderedDict(zip(self.header, row)) for row in self.data]


    def __getitem__(self, item):
        # this is for rows only
        if isinstance(item, (int, slice)):
            return self.data[item]

        # this is for columns only
        elif isinstance(item, (str, unicode)):
            return [row[item] for row in self.data]

        # this is for rows and columns
        elif isinstance(item, tuple):
            if isinstance(item[0], list) or isinstance(item[1], list):

                if isinstance(item[0], list):
                    rowz = [row for index, row in enumerate(self.data) if index in item[0]]
                else:
                    rowz = self.data[item[0]]

                if isinstance(item[1], list):
                    if all([isinstance(thing, int) for thing in item[1]]):
                        return [[column_value for index, column_value in enumerate([value for value in row.itervalues()]) if index in item[1]] for row in rowz]
                    elif all([isinstance(thing, (str, unicode)) for thing in item[1]]):
                        return [[row[column_name] for column_name in item[1]] for row in rowz]
                    else:
                        raise TypeError('What the hell is this?')

                else:
                    return [[value for value in row.itervalues()][item[1]] for row in rowz]
            else:
                if isinstance(item[1], (int, slice)):
                    return [[value for value in row.itervalues()][item[1]] for row in self.data[item[0]]]
                elif isinstance(item[1], (str, unicode)):
                    return [row[item[1]] for row in self.data[item[0]]]
                else:
                    raise TypeError('I don\'t know how to handle this...')

        # only for lists of column names
        elif isinstance(item, list):
            return [[row[column_name] for column_name in item] for row in self.data]

    def transform_type(self, col_name):
        is_time = 0
        try:
            # try if col is numeric
            nums = [float(row[col_name].replace(',', '')) for row in self.data]
            return nums, 1 if is_time else 0
        except:
            try:
                # if not numeric, is time?
                nums = [parse(row[col_name].replace(',', '')) for row in self.data]
                # transform to seconds
                nums = [time.mktime(num.timetuple()) for num in nums]
                is_time = 1
                return nums, 1 if is_time else 0
            except:
                raise TypeError('text values cannot be calculated')

    # --------------Task 3------------

    def min(self, column_name):
        nums, is_time = self.transform_type(column_name)
        result = min(nums)
        return datetime.datetime.fromtimestamp(result) if is_time else result

    def max(self, column_name):
        nums, is_time = self.transform_type(column_name)
        result = max(nums)
        return datetime.datetime.fromtimestamp(result) if is_time else result

    def median(self, column_name):
        nums, is_time = self.transform_type(column_name)
        nums = sorted(nums)
        center = int(len(nums) / 2)
        if len(nums) % 2 == 0:
            result = sum(nums[center - 1:center + 1]) / 2.0
            return datetime.datetime.fromtimestamp(result) if is_time else result
        else:
            result = nums[center]
            return datetime.datetime.fromtimestamp(result) if is_time else result

    def mean(self, column_name):
        nums, is_time = self.transform_type(column_name)
        result = sum(nums) / len(nums)
        return datetime.datetime.fromtimestamp(result) if is_time else result

    def sum(self, column_name):
        nums, is_time = self.transform_type(column_name)
        return sum(nums)

    def std(self, column_name):
        nums, is_time = self.transform_type(column_name)
        mean = sum(nums) / len(nums)
        return (sum([(num - mean) ** 2 for num in nums]) / len(nums)) ** 0.5

    def get_rows_where_column_has_value(self, column_name, value, index_only=False):
        if index_only:
            return [index for index, row_value in enumerate(self[column_name]) if row_value == value]
        else:
            return [row for row in self.data if row[column_name] == value]

    def get_rows_where_column_has_value(self, column_name, value, index_only=False):
        if index_only:
            return [index for index, row_value in enumerate(self[column_name]) if row_value==value]
        else:
            return [row for row in self.data if row[column_name]==value]

    # --------------Task 4------------

    def add_rows(self, list_of_lists):
        col_count = len(self.header)
        # check the length of every new added row equals to len(header)
        if sum([len(row) == col_count for row in list_of_lists]) == len(list_of_lists):
            self.data = self.data + [OrderedDict(zip(self.header, row)) for row in list_of_lists]
            return self
        else:
            raise Exception('Incorrect number of columns')

    # --------------Task 5------------

    def add_columns(self, list_of_values, column_name):
        if len(list_of_values) == len(self.data):
            self.header = self.header + column_name
            self.data = [OrderedDict(zip(list(old_row.keys()) + column_name, list(old_row.values()) + added_values))
                     for old_row, added_values in zip(self.data, list_of_values)]
            return self
        else:
            raise Exception('Incorrect number of rows')
            

infile = open('SalesJan2009.csv')
lines = infile.readlines()
lines = lines[0].split('\r')
data = [l.split(',') for l in lines]
things = lines[559].split('"')
data[559] = things[0].split(',')[:-1] + [things[1]] + things[-1].split(',')[1:]
df = DataFrame(list_of_lists=data)

#TESTING

#TASK 3
print df.max('Price')
print df.min('Price')
#TASK 4
print df.add_rows([['1/5/09 4:10', 'Product1', '1200', 'Mastercard', 'Nicola', 'Roodepoort', 'Gauteng', 'South Africa', '1/5/09 2:33', '1/7/09 5:13', '-26.1666667', '27.8666667' ]])

            
            

