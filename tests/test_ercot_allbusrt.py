"""
This script just does a flat pull on pyiso using ALLBUSRT.
The ALLBUSRT market option will, for get_lmp, pull a separate report that 
grabs the LMPs for all ~12K electrical buses, and not just the Settlment Points
that are accessed via the RT5M market option.
"""
from datetime import datetime, timedelta
import pandas as pd
from pyiso import client_factory
ercot = client_factory('ERCOT', timeout_seconds=60)

#Use start and end time from now
starts = datetime.now()-timedelta(minutes=6) #Note that one 5m interval will have LMPs for ~12K electrical buses
ends = datetime.now()
#make strings for end and start times
startstr = starts.strftime("%Y-%m-%d %H:%M:%S")
endstr = ends.strftime("%Y-%m-%d %H:%M:%S")


#Get New Data via pyiso into dataframe
print('I am getting the new data. This can take a while...')
data = ercot.get_lmp(start_at=startstr, end_at=endstr, market="ALLBUSRT", node_id=None)
new_data = pd.DataFrame(data)

#correct time from UTC back to US Central (ERCOT time)
new_data['timestamp'] = new_data['timestamp'].dt.tz_convert('US/Central')


#now display the results
print('I just got the new data.')
print(new_data)
