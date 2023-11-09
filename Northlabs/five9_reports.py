from five9 import Five9
from io import StringIO
import pandas as pd
import datetime
from datetime import datetime, timedelta
import time
from pytz import timezone
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import write_pandas

#Connect in FIVE9
def authenticate(username, pwd):
    auth = Five9('tlgjustin', 'WyzwZMa41s42CXuBu2a8bFGbDKzzJGAi')
    config = auth.configuration
    return (auth, config)

f9, config = authenticate("username", "password")


def exec(script,folder,report,target_table):

	#create connection
	engine2 = snowflake.connector.connect(
			account = 'irsopih-uyb98241',
			user = 'KEBOOLA',
			password = 'J24VtkSandWhich#45',
			database = 'PRD_PYTHON_SOURCE',
			schema = 'FIVE9',
			warehouse = 'COMPUTE_WH',
			role='PRD_SOURCES_RW',) 

	cs = engine2.cursor()

	cs.execute(script)
	
	#Run the report 
	identifier = config.runReport(folderName = folder,
								  reportName = report,
								  criteria = criteria)

	#Wait if the report is running 
	while(config.isReportRunning(identifier, 5)):
	  time.sleep(3)
  
	#Retrieve data from execution 
	result = config.getReportResultCsv(identifier)

	#Convert string to DataFrame 
	data = StringIO(result)
	df = pd.read_csv(data, sep=",")

	#Count Number of records pulled
	num_of_rows = len(df)
	print(f"The number of rows is {num_of_rows}")

	#Set dates used for extraction
	df['START_EXTRACTION_DATE'] = start
	df['END_EXTRACTION_DATE'] = end


	write_pandas(engine2, df, target_table, auto_create_table=True, overwrite = False)

	engine2.close()

#Set timezone
now_utc = datetime.now(timezone('UTC'))
now_eastern = now_utc.astimezone(timezone('US/Eastern'))

#Change days from current time
count = 1  ## number of days to go back
startreportime = now_eastern - timedelta(days=count)
endreportime = now_eastern - timedelta(days=count)

#Set start and end time for report criteria
starttime = f"{(startreportime):%Y-%m-%d}" + 'T00:00:00.000'
endtime = f"{(endreportime):%Y-%m-%d}" + 'T23:59:59.999'

#Set variables as start and end
start = starttime
end = endtime
print(start)
print(end)

#set criteria using variables
criteria = {'time':{'end':end, 'start':start}}

#set list of folders, reports and target tables
list_folder = ['Agent Reports','Agent Reports','Call Log Reports','Shared Reports','Shared Reports','Individual vs Whole - JB','Individual vs Whole - JB','Individual vs Whole - JB']
list_report = ['Agent Login-Logout','Agent State Details','Call Log','Call Log Talk Time','Call Log with Notes','Individual vs Whole - Reason Code Breakdown','Individual vs Whole / Rolling Daily - Login / Ready% / On Call% / Not Ready% - Agent First Name','Individual vs Whole / Rolling Daily - Login / Ready% / On Call% / Not Ready% - None']
list_target_table = ['AGENT_LOGIN_LOGOUT','AGENT_STATE_DETAILS','CALL_LOG','CALL_LOG_TALK_TIME','CALL_LOG_WITH_NOTES','INDIVIDUAL_VS_WHOLE_REASON_CODE_BREAKDOWN','INDIVIDUAL_VS_WHOLE_ROLLING_DAILY_LOGIN_READY_ON_CALL_NOT_READY_AGENT_FIRST_NAME','INDIVIDUAL_VS_WHOLE_ROLLING_DAILY_LOGIN_READY_ON_CALL_NOT_READY_NONE']


ind   = 0 
total_reports = len(list_folder)

while (ind < total_reports):

	#set folder, report and target table
	folder = list_folder[ind]
	report = list_report[ind]
	target_table = list_target_table[ind]

	script = 'DELETE FROM ' + 'PRD_PYTHON_SOURCE.FIVE9.' + target_table + ' WHERE START_EXTRACTION_DATE =  \'' + start +  '\' '
	print(f"script {script}")
	
	execution = exec(script,folder,report,target_table)

	print(f"indice {ind}")
	print(f"folder {folder}")
	print(f"report {report}")
	print(f"target_table {target_table}")
	print('--------------------------------------------------------------------------------')
	ind = ind + 1

