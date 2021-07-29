import boto3
import json
from boto3.dynamodb.conditions import Attr

import botocore
#python retry cloud pattern
#https://github.com/jd/tenacity
from tenacity import *

#the class DBHelper has methods to perform repetive database operations
class DBHelper:

    #the method retry_attempt gets called whenever there is a retry attempt
    def __retry_attempt(retry_state):
        print('Trying to connect AWS one more time, meanwhile please check your session is active and not expired...')

    #the method return_last_value gets called after all the attempts have failed
    def __return_last_value(retry_state):
        print('Connecting to AWS failed, please see the error message.')
        return retry_state.outcome.result()

    #the method istableexist is used to determine if a dynamodb table exists
    @staticmethod
    #since using student account which will expire often, retry 3 times for every 15 seconds
    @retry(reraise=True, stop=(stop_after_delay(15) | stop_after_attempt(3)), before_sleep=__retry_attempt, retry_error_callback=__return_last_value)

    def istableexist(tablename):
        dynamodb_client = boto3.client('dynamodb')

        try:
            response = dynamodb_client.describe_table(TableName=tablename)
            if response:
                return True
            else:
                return False
        except dynamodb_client.exceptions.ResourceNotFoundException:
            return False
    
    # the method get_allitems takes timestamp to return all matching items
    @staticmethod
    #since using student account which will expire often, retry 3 times for every 15 seconds
    @retry(reraise=True, stop=(stop_after_delay(15) | stop_after_attempt(3)), before_sleep=__retry_attempt, retry_error_callback=__return_last_value)
    def get_allitems(starttime, endtime, endpointurl, tablename, comparecolumn):

        try:
            dynamodb = boto3.resource('dynamodb', endpoint_url=endpointurl)

            table = dynamodb.Table(tablename)

            response = table.scan(FilterExpression=Attr(comparecolumn).between(starttime, endtime))

            items = response['Items']

            # Since scan returns only chunks of data in batch, get everything it returns until there is no more
            while 'LastEvaluatedKey' in response:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items.extend(response['Items'])

            return items
        except Exception as e:
            raise Exception(str(e))

    #the method insertitem inserts an item into the given dynamodb table
    @staticmethod
    #since using student account which will expire often, retry 3 times for every 15 seconds
    @retry(reraise=True, stop=(stop_after_delay(15) | stop_after_attempt(3)), before_sleep=__retry_attempt, retry_error_callback=__return_last_value)
    def insertitem(item, tablename):
        try:
            dynamodb = boto3.resource("dynamodb")
            table = dynamodb.Table(tablename)
            #table.batch_write_item(RequestItems=item)
            table.put_item(Item = item)
        except Exception as e:
            raise Exception(str(e))

#the class ConfigHelper contains methods to read and return data items from the config file
class ConfigFileHelper:
    #the method getconfig is used to read and return the config file
    @staticmethod
    def get_config(filename):
        try:
            f=open(filename)
            config = json.loads(f.read())
            f.close()
            return config
        except Exception as e:
            raise Exception(str(e))

#the Utility class contains some generic methods repeatedly used over other classes
class Utility:
    #the method get_devices returns distinct devices from the given list
    @staticmethod
    def get_devices(items):
        try:
            devices = list({item["deviceid"] for item in items})
            devices.sort()
            return devices
        except Exception as e:
            raise Exception(str(e))

    #the method get_devices returns distinct devices from the given list
    @staticmethod
    def get_iotdeviceids():
        try:
            iot_client = boto3.client('iot',region_name ='us-east-1')
            # retrive unique device ids from the IOT Core 
            response = iot_client.list_things(maxResults=100, thingTypeName='BedSideMonitorType')
            devices = response["things"]
            device_ids = []
            for y in devices:
                device_id = y["thingName"]
                device_ids.append(device_id)
            return device_ids
        except Exception as e:
            raise Exception(str(e))


'''
#test
#checking existing table
#print(DBHelper.istableexist("bsm_data"))
#checking non-existent table
#print(DBHelper.istableexist("idontexist8234hxb8hdf"))

#config = ConfigFileHelper.get_config("config.json")
#print(config)
#config = ConfigFileHelper.get_config("rules.json")
#print(config)
#print(Utility.get_iotdeviceids())
'''