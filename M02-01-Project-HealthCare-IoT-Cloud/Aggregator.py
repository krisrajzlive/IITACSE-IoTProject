# To run this program: python Aggregator.py -s "21/06/12 18:26:00" -e "21/06/12 18:30:00"
# date parameters should be in YY/MM/DD HH:MM:SS format 
# This program is used to compute aggregates for every device, sensor and per minute

import datetime
from datetime import datetime
from decimal import *
#user defined module that has DBHelper, ConfigFileHelper and Utility classes
from Utility import DBHelper, ConfigFileHelper, Utility
import statistics
import itertools

# the class Aggregator contains methods to fetch raw data and compute aggregates for the given duration
class Aggregator:

    def __init__(self):
        self.__latest_error = ''
        self.configFile = 'configs/config.json'
        self.dynamoDBEndPointURL = ''
        self.dynamoDBRawDataTable = ''
        self.dynamoDBAggregateTable = ''
        self.datetimeFormat = ''

        #reading config file and initialzing variables to be used
        self.__initialize_config()

        print('Checking if required tables exists...')
        self.__determine_requiredtables()
    
    #the function __determine_requiredtables is used to determine if all the required tables exists before proceeding
    def __determine_requiredtables(self):
        if not DBHelper.istableexist(self.dynamoDBRawDataTable):
            print('Table ' + self.dynamoDBRawDataTable + ' does not exist. Please run the dbsetup program to create it.')
            exit(-1)
        
        if not DBHelper.istableexist(self.dynamoDBAggregateTable):
            print('Table ' + self.dynamoDBAggregateTable + ' does not exist. Please run the dbsetup program to create it.')
            exit(-1)
 
    # the private method __initialize_config is used to initialize instance variables 
    def __initialize_config(self):
        try:
            jconfig = self.__get_jconfig()
            self.dynamoDBEndPointURL = jconfig["dynamodb_endpoint_url"]
            self.dynamoDBRawDataTable = jconfig["dynamodb_raw_data_table"]
            self.dynamoDBAggregateTable = jconfig["dynamodb_aggregate_data_table"]
            self.datetimeFormat = jconfig["datetimeFormat"]
        except Exception as e:
            self.__latest_error = str(e)

    # the method get_latesterror returns any latest error persisted in the instance variable
    def get_latesterror(self):
        return self.__latest_error

    # the method get_allitems takes timestamp to return all matching items
    def get_allitems(self, starttime, endtime):
        try:
            items = DBHelper.get_allitems(starttime, endtime, self.dynamoDBEndPointURL, self.dynamoDBRawDataTable, "timestamp")
            return items
        except Exception as e:
            raise Exception(str(e))

    # the method get_devices is used to populate list of unique devices
    def get_devices(self, items):
        try:
            devices = Utility.get_devices(items)
            return devices
        except Exception as e:
            self.__latest_error = str(e)

    # the method get_sensors is used to populate list of sensors
    def get_sensors(self, items):
        try:
            sensors = {item["datatype"] for item in items}
            return sensors
        except Exception as e:
            self.__latest_error = str(e)
    
    # the private method __get_jocnfig returns all configurations keys from json configuration file
    def __get_jconfig(self):
        try:
            jconfig = ConfigFileHelper.get_config(self.configFile)
            return jconfig
        except Exception as e:
            self.__latest_error = str(e)

    #the method printitem is used to print the aggregate item in specific format
    @staticmethod
    def printitem(item):
        print("deviceid: {0}, datatype: {1}, average: {2}, maximum: {3}, minimum: {4}, devicetime: {5}, timestamp: {6}"
        .format(item["deviceid"], item["datatype"], item["average"], item["maximum"], item["minimum"], item["devicetime"], item["timestamp"]))

    # the method insert items takes the aggregated item and inserts into dynamodb table
    def insert_item(self, item):
        try:
            DBHelper.insertitem(item, self.dynamoDBAggregateTable)
        except Exception as e:
            raise Exception(str(e))

    #the start method is used to compute aggregates using other methods and persist the aggregated item in the database
    #call this method to start computing
    def start(self, starttime, endtime):
        print('Computing aggregates...')
        try:
            #format datetime
            startTime = datetime.strptime(starttime, self.datetimeFormat)
            endTime = datetime.strptime(endtime, self.datetimeFormat)

            #get all the items for the given period
            items = self.get_allitems(startTime.isoformat(), endTime.isoformat())

            # Sort the response by timestamp before grouping
            sorted_items = sorted(
                items, 
                key=lambda x: (x["timestamp"][:16], x["deviceid"], x["datatype"])
            )

            # Group the response by items per minute
            items_by_minute = itertools.groupby(
                sorted_items, 
                key=lambda x: (x["timestamp"][:16], x["deviceid"], x["datatype"])
            )

            # Calculate the statistics for each minute
            for minute, sorted_items in items_by_minute:
                values_per_minute = [item["value"] for item in sorted_items]
                
                avg = statistics.mean(values_per_minute)
                min_value = min(values_per_minute)
                max_value = max(values_per_minute)
                
                agg_item = {}
                agg_item["deviceid"] = minute[1]
                agg_item['datatype'] = minute[2]
                agg_item['average'] = round(avg,2)
                agg_item['minimum'] = round(min_value,2)
                agg_item['maximum'] = round(max_value,2)
                agg_item['devicetime'] = minute[0] + ":00"
                agg_item['timestamp'] = datetime.now().isoformat() # isoformatted string does not require conversion from string to date for comparison

                Aggregator.printitem(agg_item)
                # persist aggregated item into DynamoDB table
                self.insert_item(agg_item)
                print('Computed and persisted aggregates')
        except Exception as e:
            self.__latest_error = str(e)

'''
#testing
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-s", "--starttime", action="store", required=True, dest="starttime", help="start time is a required parameter")
parser.add_argument("-e", "--endtime", action="store", required=True, dest="endtime", help="end time is a required parameter")
args = parser.parse_args()

aggregator = Aggregator()
aggregator.start(args.starttime,args.endtime)
print(aggregator.get_latesterror())
'''