This file is not required to run any of the programs
This file contains only information on project setup and steps to run the programs
----------------------------------------------------------------------------------
Step 1: Login to AWS to Create ThingsGroups, Types, Things, Rules and Roles etc.,

Step 2: Download all the certificates, keys and store it in the credentials file

Step 3: Two config files (config.json and rules.json) are avaialable under configs directory
        config.json contains DynamoDB end point URL, table names, sensor frequencies
        rules.json contains rules for the sensors

Step 3: Create DynamoDB Tables 
        
        run python DBSetup.py to create all the tables

        open dbsetup.py and see more details
        Update Action for Rule created with reference to the newly created table bsm_data table in AWS IoT Core
        
Step 4: Run BSM_BedSiteMonitor in multiple terminals to populate some data for us for some time. Here are the commands:

python BedSideMonitor.py -e "a1soq35jhyucr0-ats.iot.us-east-1.amazonaws.com" -r root-CA.crt -c BSM_DEV01_Certs/4493530037-certificate.pem.crt -k BSM_DEV01_Certs/4493530037-private.pem.key -id "BSM01" -t "bsm/BSM_DEV01" -di "BSM_DEV01"
python BedSideMonitor.py -e "a1soq35jhyucr0-ats.iot.us-east-1.amazonaws.com" -r root-CA.crt -c BSM_DEV02_Certs/cc90914574-certificate.pem.crt -k BSM_DEV02_Certs/cc90914574-private.pem.key -id "BSM02" -t "bsm/BSM_DEV02" -di "BSM_DEV02"

Stop the programs runnning after sometime by pressing Ctrl+c so as to have some data to compute aggregates

Step 5: Step 2 :: Run python main.py -s "21/06/12 18:26:00" -e "21/06/12 18:30:00" -p "aggregates", to run aggregation program
-p parameter signifies the program we want to run, -p "aggregates" computes aggregates and persists in the database
Note: Date should be in YY/MM/DD HH:MM:SS

Step 6: Step 3 :: Run python main.py -s "21/06/12 18:26:00" -e "21/06/12 18:30:00" -p "rules", to run rule breach detection program
-p parameter signifies the program we want to run, -p "rules" detects anomaly and persist details in the database
Note: Date should be in YY/MM/DD HH:MM:SS

Step 7: Or alternatively run Aggregator.py -s "21/06/12 18:26:00" -e "21/06/12 18:30:00"
        -s for start time, -e for end time
Note: Might need to uncomment the commented lines if this file is executed independantly     

Step 8: After running aggregates, run python RuleAlert.py -s "21/06/12 18:26:00" -e "21/06/12 18:30:00"
        -s for start time, -e for end time
Note: Might need to uncomment the commented lines if this file is executed independantly 

Step 9: Step 4:: Login AWS Educate https://www.awseducate.com/signin/SiteLogin and check the DynamoDB tables: bsm_data, bsm_agg_data, bsm_alerts. 

Step 10: Delete the objects created in AWS once it is no longer used
Step 11: Logout from AWS Educate

List of files/folders used (alphabetically) for this project: (excluding certificates, screenshots, accesskeys and readme.txt)
        01. Aggregator.py
        02. BedSideMonitor.py
        03. config.json -> configs folder
        04. DataProcessor.py
        05. DBSetup.py
        06. main.py
        07. RuleAlert.py
        08. root-CA.crt
        09. rules.json -> configs folder
        10. Utility.py
        11. configs folder contains rules and config json files
        12. BSM_DEV01_Certs contains certs sepcific to the device
        13. BSM_DEV02_Certs contains certs specific to the device

Note: Utility.py and DataProcessor.py are used in other program files