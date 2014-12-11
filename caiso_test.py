# Run this from the /github/pyiso directory, not /github/pyiso/pyiso

from pyiso import caiso
import pandas as pd

startDate = '2013-05-25T00:00'  # General format: '2013-05-25' or '2013-05-25T00:00'
endDate='2013-05-25T02:00'
market='RTHR'             # Construct_oasis_payload expects market option to be one of # {'RT5M': 'RTM', 'DAHR': 'DAM', 'RTHR': 'HASP'}
grp_type='ALL'            # ALL, ALL_APNODES, or SELECT_NODE  
node='ALL'                # ALL or specific node code

# Positional arguments: latest, start_at, end_at, market, grp_type, node)
mycaiso = caiso.CAISOClient()
myData = mycaiso.get_lmp(False,startDate,endDate,market,grp_type,node)
mydf = pd.DataFrame(myData)
print mydf
exit()


# What does 'sliceable' option mean?
# What does the control of the market option do?
# How to handle multiple 5-minute market names (RTPD, RTD, RTM)?
# I don't quite understand how the market identities are being handled... 'hourly' vs 'HASP', 'fivemin' vs 'RTM' vs 'RT5m'
# what does 'frequency' option do?
# It feels like very different parsing approaches are used for different calls. Can this be standardized?
# Do you need to worry about cleaning up files after unzipping them?  We will potentially be generating many GB of files...
# Data management: this looks like it may get really heavy:
# ~2247 nodes for each time step each with 4 LMP components, with 96 timesteps/day (HASP) or 288 times/day (RTM) = 35040 times/yr (HASP) or 105120 times/yr (RTM)
# My inclination is to have a dict for organizing the time series, each item in which will contain a dict of nodes, which will in turn contain a dict of LMP values.  Does this make sense?
# It would be helpful to have a progress report as requests are issued and downloaded


# This also appears on my get_lmp in the fetch_caiso method after unzipping the data, but before BeautifulSoup has successfully processed this (the segmentation error is likely somewhere within BS)
# running caiso.get_generation(forecast=True) gives the fatal error 'Segmentation fault: 11' and kills python.  The error occurs at the same point on get_generation as it does on my get_lmp process