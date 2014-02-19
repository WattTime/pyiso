import requests
import copy
from datetime import datetime
from dateutil.parser import parse as dateutil_parse
import pytz


class SPPClient:
    def __init__(self):
        self.ba_name = 'SPS'
        
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

    def get_generation(self, latest=False, market='RTHR', **kwargs):
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

        else:
            raise ValueError('Latest must be True.')
            
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
            raw_data.append(dict(zip(header, rows[-2].split(','))))
            
        # parse data
        for raw_dp in raw_data:            
            # parse times
            timestamp = self._utcify(dateutil_parse(raw_dp['']))
            
            for raw_fuel_name, parsed_fuel_name in self.fuels.iteritems():
                # set up storage
                parsed_dp = {}   
    
                # add values
                parsed_dp['timestamp'] = timestamp
                parsed_dp['gen_MW'] = raw_dp[raw_fuel_name]
                parsed_dp['fuel_name'] = parsed_fuel_name
                parsed_dp['ba_name'] = self.ba_name
                
                # add to full storage
                parsed_data.append(parsed_dp)
            
        return parsed_data
