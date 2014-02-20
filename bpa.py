import requests
import copy
from datetime import datetime, timedelta
from dateutil.parser import parse as dateutil_parse
import pytz
from apps.griddata.models import DataPoint


class BPAClient:
    def __init__(self):
        self.ba_name = 'BPA'
        
        self.base_url = 'http://transmission.bpa.gov/business/operations/'
        
        self.fuels = {
            'Hydro': 'hydro',
            'Wind': 'wind',
            'Thermal': 'thermal',
        }
        

    def _utcify(self, naive_local_timestamp):
        aware_local_timestamp = pytz.timezone('America/Los_Angeles').localize(naive_local_timestamp)
        aware_utc_timestamp = aware_local_timestamp.astimezone(pytz.utc)
        return aware_utc_timestamp
        
    def _preprocess(self, row):
        vals = row.split('\t')
        vals[0] = self._utcify(dateutil_parse(vals[0]))
        return vals

    def get_generation(self, latest=False, start_at=False, end_at=False, **kwargs):
        # process args
        request_urls = []
        if latest:
            request_urls.append('wind/baltwg.txt')

        elif start_at and end_at:
            assert start_at < end_at
            if start_at >= pytz.utc.localize(datetime.today() - timedelta(days=7)):
                request_urls.append('wind/baltwg.txt')
            else:
                raise ValueError('start_at can be no more than 7 days in the past')
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
            response = requests.get(url).text
                        
            # preliminary parsing
            rows = response.split('\n')
            header = [x.strip() for x in rows[6].split('\t')]
            for row in rows[7:]:
                vals = self._preprocess(row)
                # save valid values
                if len(vals) > 1:
                    if vals[1]:
                        if latest: # overwrite list every time
                            raw_data = [dict(zip(header, vals))]
                        else: # save if date in range
                            if vals[0] >= start_at and vals[0] <= end_at:
                                raw_data.append(dict(zip(header, vals)))
                    
        # parse data
        for raw_dp in raw_data:
            for raw_fuel_name, parsed_fuel_name in self.fuels.iteritems():
                # set up storage
                parsed_dp = {}
    
                # add values
                parsed_dp['timestamp'] = raw_dp['Date/Time']
                parsed_dp['gen_MW'] = float(raw_dp[raw_fuel_name])
                parsed_dp['fuel_name'] = parsed_fuel_name
                parsed_dp['ba_name'] = self.ba_name
                parsed_dp['market'] = DataPoint.RT5M
                parsed_dp['freq'] = DataPoint.FIVEMIN
                
                # add to full storage
                parsed_data.append(parsed_dp)
            
        return parsed_data
