import requests
import copy
from datetime import datetime
from dateutil.parser import parse as dateutil_parse
import pytz
from apps.griddata.models import DataPoint


class SPPClient:
    def __init__(self):
        self.ba_name = 'SPP'
        
        self.base_url = 'http://www.spp.org/GenerationMix/'
        
        self.fuels = {
            'COAL': 'coal',
            'HYDRO': 'hydro',
            'GAS': 'natgas',
            'NUCLEAR': 'nuclear',
            'DFO': 'oil',
            'WIND': 'wind',
        }
        
    def _utcify(self, naive_local_timestamp):
        aware_local_timestamp = pytz.timezone('America/Chicago').localize(naive_local_timestamp)
        aware_utc_timestamp = aware_local_timestamp.astimezone(pytz.utc)
        return aware_utc_timestamp
        
    def _preprocess(self, row):
        vals = row.split(',')
        vals[0] = self._utcify(dateutil_parse(vals[0]))
        return vals

    def get_generation(self, latest=False, market='RTHR',
                       start_at=None, end_at=None,**kwargs):
        # process args
        if market == 'RTHR':
            file_freq = 'Hourly'
        elif market == 'RT5M':
            file_freq = '5Minute'
        else:
            raise ValueError('market must be RTHR or RT5M.')
        request_urls = []            
        if latest:
            request_urls.append('%d_%s_GenMix.csv' % (datetime.today().year, file_freq))

        elif start_at and end_at:
            this_year = start_at.year
            while this_year <= end_at.year:
                request_urls.append('%d_%s_GenMix.csv' % (this_year, file_freq))
                this_year += 1
        else:
            raise ValueError('Either latest must be True, or start_at and end_at must both be provided.')
            
        # set up storage
        raw_data = []
        parsed_data = []

        # collect raw data
        for request_url in request_urls:
            # set up request
            url = copy.deepcopy(self.base_url)
            url += request_url
            
            # carry out request
            response = requests.get(url).content
            
            # preliminary parsing
            rows = response.split('\n')
            header = rows[0].split(',')
            if latest:
                raw_data.append(dict(zip(header, self._preprocess(rows[-2]))))
            else:
                for row in rows[1:]:
                    vals = self._preprocess(row)
                    if vals[0] >= start_at and vals[0] <= end_at:
                        raw_data.append(dict(zip(header, vals)))
            
        # parse data
        for raw_dp in raw_data:            
            for raw_fuel_name, parsed_fuel_name in self.fuels.iteritems():
                # set up storage
                parsed_dp = {}   
    
                # add values
                parsed_dp['timestamp'] = raw_dp['']
                parsed_dp['gen_MW'] = float(raw_dp[raw_fuel_name])
                parsed_dp['fuel_name'] = parsed_fuel_name
                parsed_dp['ba_name'] = self.ba_name
                if market == 'RTHR':
                    parsed_dp['freq'] = DataPoint.HOURLY
                    parsed_dp['market'] = DataPoint.RTHR
                elif market == 'RT5M':
                    parsed_dp['freq'] = DataPoint.FIVEMIN
                    parsed_dp['market'] = DataPoint.RT5M                    
                
                # add to full storage
                parsed_data.append(parsed_dp)
            
        return parsed_data
