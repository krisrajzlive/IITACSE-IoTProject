# this programs should be called only after running BedSideMonitor.py simulator program
# this program is used to invoke aggregator and alert programs, also we can separately run them
#
# usage: python main.py -s "startdate" -e "endstate" -p "aggregates"
# usage: python main.py -s "startdate" -e "endstate" -p "rules"
#
# parameter definition: -s start date in YY/MM/DD HH:MM:SS 21/06/12 18:26:00 format, 
#                       -e end date in YY/MM/DD HH:MM:SS 21/06/12 18:26:00 format, 
#                       -p name of the program to run, should be either "aggregates" or "rules"
#
# exxample usage: python main.py -s "21/06/12 18:26:00" -e "21/06/12 18:30:00" -p "aggregates"
# Note: if any new devices/sensors are added, those should be added to the config file 

import argparse
from os import error
# importing the user defined factory class DataProcessor that creates instances of Aggregator or RuleAlert class based on program parameter
from DataProcessor import DataProcessor
errorMessage = ''
try:
#processing command line parameters

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--starttime", action="store", required=True, dest="starttime", help="start time is a required parameter")
    parser.add_argument("-e", "--endtime", action="store", required=True, dest="endtime", help="end time is a required parameter")
    parser.add_argument("-p", "--program", action="store", required=True, dest="program", help="program is a required parameter")
    args = parser.parse_args()

    dataprocessor = DataProcessor.create(args.program)
    dataprocessor.start(args.starttime, args.endtime)
    errorMessage = dataprocessor.get_latesterror()
except Exception as e:
    print(str(e))
finally:
    print(errorMessage)