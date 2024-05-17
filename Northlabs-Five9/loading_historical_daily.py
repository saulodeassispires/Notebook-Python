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

#Set timezone
now_utc = datetime.now(timezone('UTC'))
now_eastern = now_utc.astimezone(timezone('US/Eastern'))

#set folder, report and target table
folder = "Shared Reports"
report = "Call Log with Notes"
target_table = 'CALL_LOG_WITH_NOTES'

def executa(cont):
	#Change days from current time
	startreportime = now_eastern - timedelta(days=cont)
	endreportime = now_eastern - timedelta(days=cont)

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


	#Run the report 
	identifier = config.runReport(folderName = folder,
								  reportName = report,
								  criteria = criteria)

	#Wait if the report is running 
	while(config.isReportRunning(identifier, 5)):
	  time.sleep(15)
  
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

	#Connect Snowflake & Insert data into it. 
	engine2 = snowflake.connector.connect(
			account = 'irsopih-uyb98241',
			user = 'SPIRES',
			password = 'Olivetti@5!',
			database = 'PRD_PYTHON_SOURCE',
			schema = 'FIVE9',
			warehouse = 'COMPUTE_WH',
			role='SYSADMIN',) 

	cs = engine2.cursor()
     
	 

	write_pandas(engine2, df, target_table, auto_create_table=True, overwrite = False)

	engine2.close()
	
count = 20
while (count < 90):
    count = count + 1
    print(f'contador {count}')
    exec = executa(count)
    time.sleep(5)
