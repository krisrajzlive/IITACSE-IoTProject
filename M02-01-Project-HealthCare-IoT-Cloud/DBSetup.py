import argparse
import boto3
import json
from Utility import DBHelper, ConfigFileHelper
#python retry cloud pattern
#https://github.com/jd/tenacity
from tenacity import *

# this class is responsible for creating the tables required for this project.
# Has methods to create individual table as well as all the tables in one go

# usage #1 to create all the tables: python DBSetup.py -t 'all'

# usage #2 to create a specific table: python dbsetup.py -t 'bsm_alerts'

# usage #3 to create all tables without specifying any parameter: python dbsetup.py

# by default the program creates all the tables if no parameter is provided

class DatabaseSetup:
    def __init__(self):
        self.__latest_error = ''
        self.configFile = 'configs/config.json'
        self.dynamoDBEndPointURL = ''
        self.dynamoDBRawDataTable = ''
        self.dynamoDBAggregateTable = ''
        self.dynamoDBRuleAlertTable = ''
              
        self.__initialize_config()
        self.dynamoDB = boto3.resource('dynamodb', endpoint_url=self.dynamoDBEndPointURL)

    #the method retry_attempt gets called whenever there is a retry attempt
    def __retry_attempt(retry_state):
        print('Trying to connect AWS one more time, meanwhile please check your session is active and not expired...')

    #the method return_last_value gets called after all the attempts have failed
    def __return_last_value(retry_state):
        print('Connecting to AWS failed, please see the error message.')
        return retry_state.outcome.result()
 
    # the private method __initialize_config is used to initialize instance variables
    def __initialize_config(self):
        try:
            jconfig = self.__get_jconfig()
            self.dynamoDBEndPointURL = jconfig["dynamodb_endpoint_url"]
            self.dynamoDBRawDataTable = jconfig["dynamodb_raw_data_table"]
            self.dynamoDBAggregateTable = jconfig["dynamodb_aggregate_data_table"]
            self.dynamoDBRuleAlertTable = jconfig["dynamodb_rulalert_table"]
        except Exception as e:
            self.__latest_error = str(e)
    
    # the private method __get_jocnfig returns all configurations keys from json configuration file
    def __get_jconfig(self):
        try:
            jconfig = ConfigFileHelper.get_config(self.configFile)
            return jconfig
        except Exception as e:
            self.__latest_error = str(e)

    # the method get_latesterror returns any latest error persisted in the instance variable
    def get_latesterror(self):
        return self.__latest_error

    # the method create_tables creates all the required tables for this project
    def create_tables(self):
        dbsetup = DatabaseSetup()
        
        print('Creating Tables ...')
        
        tbl = dbsetup.create_bsm_data_table()
        print("Table status (bsm_data): ", tbl.table_status if not tbl == None else 'not created')

        tbl = dbsetup.create_bsm_agg_data_table()
        print("Table status (bsm_agg_data): ", tbl.table_status if not tbl == None else 'not created')

        tbl = dbsetup.create_bsm_alerts_table()
        print("Table status (bsm_alerts): ", tbl.table_status if not tbl == None else 'not created')

    # the method create_bsm_data_table creates bsm_data table, if there is any change in the keys/name update the method
    # since using student account which will expire often, retry 3 times for every 15 seconds
    @retry(reraise=True, stop=(stop_after_delay(15) | stop_after_attempt(3)), before_sleep=__retry_attempt, retry_error_callback=__return_last_value)
    def create_bsm_data_table(self):
        tablename = self.dynamoDBRawDataTable
        try:

            if not DBHelper.istableexist(tablename):
                table = self.dynamoDB.create_table(
                    TableName=tablename,
                    KeySchema=[
                        {
                            'AttributeName': 'deviceid',
                            'KeyType': 'HASH'  # Partition key
                        },
                        {
                            'AttributeName': 'timestamp',
                            'KeyType': 'RANGE'  # Sort key
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'deviceid',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'timestamp',
                            'AttributeType': 'S'
                        },

                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                )
                #wait until the table has been created 
                table.meta.client.get_waiter('table_exists').wait(TableName=tablename)
                return table
            else: print("Table "  + tablename + " already exists")
        except Exception as e:
            self.__latest_error = str(e)

    # the method create_bsm_alerts_table creates bsm_alerts table, if there is any change in the keys/name update the method
    # since using student account which will expire often, retry 3 times for every 15 seconds
    @retry(reraise=True, stop=(stop_after_delay(15) | stop_after_attempt(3)), before_sleep=__retry_attempt, retry_error_callback=__return_last_value)
    def create_bsm_alerts_table(self):
        tablename = self.dynamoDBRuleAlertTable

        try:
            if not DBHelper.istableexist(tablename):
                table = self.dynamoDB.create_table(
                    TableName=tablename,
                    KeySchema=[
                        {
                            'AttributeName': 'deviceid',
                            'KeyType': 'HASH'  # Partition key
                        },
                        {
                            'AttributeName': 'timestamp',
                            'KeyType': 'RANGE'  # Sort key
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'deviceid',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'timestamp',
                            'AttributeType': 'S'
                        },

                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                )
                #wait until the table has been created 
                table.meta.client.get_waiter('table_exists').wait(TableName=tablename)
                return table
            else: print("Table "  + tablename + " already exists")
        except Exception as e:
            self.__latest_error = str(e)

    # the method create_bsm_agg_data_table creates bsm_agg_data table, if there is any change in the keys/name update the method
    # since using student account which will expire often, retry 3 times for every 15 seconds
    @retry(reraise=True, stop=(stop_after_delay(15) | stop_after_attempt(3)), before_sleep=__retry_attempt, retry_error_callback=__return_last_value)
    def create_bsm_agg_data_table(self):
        tablename = self.dynamoDBAggregateTable
        try:
            if not DBHelper.istableexist(tablename):
                table = self.dynamoDB.create_table(
                    TableName=tablename,
                    KeySchema=[
                        {
                            'AttributeName': 'deviceid',
                            'KeyType': 'HASH'  # Partition key
                        },
                        {
                            'AttributeName': 'timestamp',
                            'KeyType': 'RANGE'  # Sort key
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'deviceid',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'timestamp',
                            'AttributeType': 'S'
                        },

                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                )
                #wait until the table has been created 
                table.meta.client.get_waiter('table_exists').wait(TableName=tablename)
                return table
            else: print("Table "  + tablename + " already exists")
        except Exception as e:
            self.__latest_error = str(e)

#Only to run the program independantly
if __name__ == '__main__':
    parser = argparse.ArgumentParser()  
    parser.add_argument("-t", "--tables", action="store", required=False, dest="tables", default="all", help="name(s) of the table to be created. All for all tables.")
    args = parser.parse_args()
    
    dbsetup = DatabaseSetup()

    if args.tables == 'all':
        dbsetup.create_tables()
    elif args.tables == 'bsm_data':
        tbl = dbsetup.create_bsm_data_table()
        print("Table status (bsm_data): ", tbl.table_status if not tbl == None else 'not created')
    elif args.tables == 'bsm_agg_data':
        tbl = dbsetup.create_bsm_agg_data_table()
        print("Table status (bsm_agg_data): ",tbl.table_status if not tbl == None else 'not created')
    elif args.tables == 'bsm_alerts':
        tbl = dbsetup.create_bsm_alerts_table()
        print("Table status (bsm_alerts): ",tbl.table_status if not tbl == None else 'not created')
    else:
        print('Invalid parameter, value can be all or bsm_data or bsm_agg_data or bsm_alerts!')
    
    print(dbsetup.get_latesterror())