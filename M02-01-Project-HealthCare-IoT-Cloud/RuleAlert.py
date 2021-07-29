# this program is used to check rules breaches for each sensor in every device
# sensor rules are specified in rules.json file under configs folder
# To run this program: python RuleAlert.py -s "21/06/12 18:26:00" -e "21/06/12 18:30:00"
# date parameters should be in YY/MM/DD HH:MM:SS format 

import datetime
from datetime import datetime
from datetime import timedelta
from decimal import *
#user defined module that has DBHelper, ConfigFileHelper and Utility classes
from Utility import DBHelper, ConfigFileHelper, Utility

class RuleAlert:

    def __init__(self):
        self.__latest_error = ''
        self.configFile = 'configs/config.json'
        self.rulesFile = 'configs/rules.json'
        self.dynamoDBEndPointURL = ''
        self.dynamoDBRuleAlertTable = ''
        self.dynamoDBAggregateTable = ''
        self.datetimeFormat = ''

        self.__initialize_config()

        print('Checking if required tables exists...')
        self.__determine_requiredtables()
            
        self.deviceid = ''
        self.sensorType = ''
        self.avgMin = 0
        self.avgMax = 0
        self.triggerCount = 0
    
    #the function __determine_requiredtables is used to determine if all the required tables exists before proceeding
    def __determine_requiredtables(self):
        if not DBHelper.istableexist(self.dynamoDBAggregateTable):
            print('Table ' + self.dynamoDBAggregateTable + ' does not exist. Please run the dbsetup program to create it.')
            exit(-1)
        
        if not DBHelper.istableexist(self.dynamoDBRuleAlertTable):
            print('Table ' + self.dynamoDBRuleAlertTable + ' does not exist. Please run the dbsetup program to create it.')
            exit(-1)

    # the private method __initialize_config is used to initialize instance variables
    def __initialize_config(self):
        try:
            jconfig = self.__get_jconfig()
            self.dynamoDBEndPointURL = jconfig["dynamodb_endpoint_url"]
            self.dynamoDBAggregateTable = jconfig["dynamodb_aggregate_data_table"]
            self.dynamoDBRuleAlertTable = jconfig["dynamodb_rulalert_table"]
            self.datetimeFormat = jconfig["datetimeFormat"]
        except Exception as e:
            self.__latest_error = str(e)

    # the method get_latesterror returns any latest error persisted in the instance variable
    def get_latesterror(self):
        return self.__latest_error

    # the method get_alert_message returns the message for rule breach 
    def get_alert_message(self, agg_item, timestamp, breachCount):
        try:
            msg = "Warning! Maximum breach " + str(breachCount) + " has occured on Device: " + self.deviceid + " with "+ self.sensorType + " sensor"
            msg = msg + " at " + str(timestamp)
            msg = msg + " min allowed " + str(self.avgMin) + " detected " + str(agg_item["average"])
            msg = msg + " max allowed " + str(self.avgMax) + " detected " + str(agg_item["average"])
            return msg
        except Exception as e:
            self.__latest_error = str(e)
 
    #the method set_rule sets the rule properties
    def set_rule(self, rule, deviceid):
        try:
            if rule:
                self.deviceid = deviceid
                self.sensorType = rule["type"]
                self.avgMin = Decimal(rule["avg_min"])
                self.avgMax = Decimal(rule["avg_max"])
                self.triggerCount = int(rule["trigger_count"])
            else:
                self.__latest_error = 'No rule found'
        except Exception as e:
            self.__latest_error = str(e)

    # the method is_rule_breached it to detect any rule is breached 
    def is_rule_breached(self, agg_item):
        try:
            if(self.sensorType == agg_item["datatype"] and ( Decimal(agg_item["average"]) < self.avgMin or Decimal(agg_item["average"]) > self.avgMax)):
                return True
            else: return False 
        except Exception as e:
            self.__latest_error = str(e)

    # the method is_rule_breachedMax is to detect maximum rule count is detected
    def is_rule_breachedMax(self, breachCount):
        try:
            if(breachCount >= self.triggerCount):
                return True
            else: return False
        except Exception as e:
            self.__latest_error = str(e)

    # the private method __get_jconfig returns all configurations keys from json configuration file
    def __get_jconfig(self):
        try:
            jconfig = ConfigFileHelper.get_config(self.configFile)
            return jconfig
        except Exception as e:
            self.__latest_error = str(e)

    # the method get_jrules returns rules stored in rules.json file
    def get_jrules(self, filename):
        try:
            jrules = ConfigFileHelper.get_config(filename)
            return jrules
        except Exception as e:
            self.__latest_error = str(e)

    # the method get_rule return only those items belongs to the given sensor
    def get_rule(self, jrules, sensortype):
        try:
            rules = [jrule for jrule in jrules["rules"] if jrule['type'] == sensortype]

            if rules:
                return rules[0]
        except Exception as e:
            self.__latest_error = str(e)

    # the method get_allitems takes timestamp to return all matching items from the aggregate table
    def get_allitems(self, starttime, endtime):
        try:
            items = DBHelper.get_allitems(starttime, endtime, self.dynamoDBEndPointURL, self.dynamoDBAggregateTable, "devicetime")
            return items
        except Exception as e:
            self.__latest_error = str(e)

    # the method get_devices is used to populate list of unique devices
    def get_devices(self, items):
        try:
            devices = Utility.get_devices(items)
            return devices
        except Exception as e:
            self.__latest_error = str(e)

    # the method get_items takes deviceid, time and sensor type and returns matching items
    def get_items(self, items, deviceid, sensortype, starttime, endtime):
        try:
            tempitems = [item for item in items 
            if (item['deviceid'] == deviceid 
            and item["datatype"] == sensortype 
            and item['devicetime'] >= starttime 
            and item["devicetime"] < endtime )]
            return tempitems
        except Exception as e:
            self.__latest_error = str(e)

    # the method insert items takes rule breached with message, device, sensor etc., and persists them in the dynamnodb rules table
    def insert_item(self, item):
        try:
            DBHelper.insertitem(item,self.dynamoDBRuleAlertTable)
        except Exception as e:
            raise Exception(str(e))

    # the method start is used to perform checking the rules, generates alert message and persist it in the database table
    # call this method
    def start(self, starttime, endtime):

        try:
            startTime = datetime.strptime(starttime, self.datetimeFormat)
            endTime = datetime.strptime(endtime, self.datetimeFormat)

            #retreive all matching items and set these items, devices, sensors
            items = self.get_allitems(startTime.isoformat(), endTime.isoformat())
            
            #there are no items to process
            if len(items) == 0:
                print('Sorry, can not find any records to process. Please try with a different time range.')
                exit(-1)

            devices = self.get_devices(items)

            # read rules.josn containing rules to detect any anomolies
            jrules = self.get_jrules(self.rulesFile);
            sensors = jrules["rules"]

            #iterate for every minute and for the sensors (sensors are hardcoded in config file)
            print('Detecting rule breach begins ...')

            minute = 0
            alert_startTime = startTime + timedelta(minutes=minute)

            for device in devices:

                triggerCount = 0
                minute = 0
                alert_startTime = startTime + timedelta(minutes=minute)

                #getting rule for the sensor from config and serialize it for easy usage
                for sensor in sensors:  

                    triggerCount = 0
                    minute = 0
                    alert_startTime = startTime + timedelta(minutes=minute)

                    while alert_startTime < endTime:
                        
                        self.set_rule(self.get_rule(jrules, sensor["type"]), device)

                        minute = minute + 1

                        alert_endTime = startTime + timedelta(minutes=minute)

                        print('Detecting rule breach on ' + device + ' for ' + sensor["type"], 'from ', str(alert_startTime) + ' to ' + str(alert_endTime) )
                        # get item for the sepcfic time period and for the specific sensor
                        # isoformatted string does not require conversion from string to date for comparison
                        agg_item = self.get_items(items, device, sensor["type"], alert_startTime.isoformat(), alert_endTime.isoformat())
                        
                        if (agg_item):
                            # determine if a breach is occurred
                            if (self.is_rule_breached(agg_item[0]) == True):
                                triggerCount = triggerCount + 1
                                print(str(triggerCount) + ' occurrence(s) of rule breache detected')

                            # determine if a maximum breach is occurred
                            if (self.is_rule_breachedMax(triggerCount)):
                                alertmessage = ""
                                alertmessage = self.get_alert_message(agg_item[0], agg_item[0]["devicetime"], triggerCount)
                                print(alertmessage)
                                triggerCount = 0

                                # preparing data for persistent storing into DynamoDB table
                                alert_item = {}
                                
                                alert_item['alertmessage'] = alertmessage
                                alert_item['deviceid'] = device
                                alert_item['datatype'] = sensor["type"]
                                alert_item['breachtime'] = agg_item[0]['devicetime']
                                alert_item['timestamp'] = datetime.now().isoformat() # isoformatted string does not require conversion from string to date for comparison

                                # persist(insert) item into DynamoDB table
                                self.insert_item(alert_item)
                                print('Persisted message in DynamoDB table.')
                        else:
                            print('No record(s) found to detect breach')
                        alert_startTime = alert_endTime

            print('Detecting rule breach ends.')
        except Exception as e:
            self.__latest_error = str(e)

'''
#testing
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-s", "--starttime", action="store", required=True, dest="starttime", help="start time is a required parameter")
parser.add_argument("-e", "--endtime", action="store", required=True, dest="endtime", help="end time is a required parameter")
args = parser.parse_args()

rulealert = RuleAlert()
rulealert.start(args.starttime,args.endtime)
print(rulealert.get_latesterror())
'''