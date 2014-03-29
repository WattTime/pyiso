import requests
import copy
from datetime import datetime, timedelta
from dateutil.parser import parse as dateutil_parse
import pytz
from pyiso.base import BaseClient


class SPPClient(BaseClient):
    def __init__(self):
        self.ba_name = 'SPP'
        
        self.base_url = 'http://www.spp.org/GenerationMix/'
                
    def get_fuels(self, year=2014):
        if year == 2014:
            return {
                'COAL': 'coal',
                'FUEL_OIL': 'oil',
                'GAS': 'natgas',
                'HYDRO': 'hydro',
                'NUCLEAR': 'nuclear',
                'OTHER': 'other',
                'PUMP_HYDRO': 'smhydro',
                'SOLAR': 'solar',
                'WASTE': 'refuse',
                'WIND': 'wind',
            }
            
        else:
            return {
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
                       start_at=None, end_at=None, yesterday=False, **kwargs):
        # process args
        if market == 'RTHR':
            file_freq = 'Hourly'
        elif market == 'RT5M':
            file_freq = '5Minute'
        else:
            raise ValueError('market must be RTHR or RT5M.')
        request_urls = []
        if yesterday:
            midnight = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
            end_at = self._utcify(midnight) - timedelta(hours=1)
            start_at = end_at - timedelta(days=1) + timedelta(hours=1)
        if latest:
            request_urls.append('%d_%s_GenMix.csv' % (datetime.today().year, file_freq))            
        elif start_at and end_at:
            this_year = start_at.year
            while this_year <= end_at.year:
                request_urls.append('%d_%s_GenMix.csv' % (this_year, file_freq))
                this_year += 1
        else:
            raise ValueError('Either latest or yesterday must be True, or start_at and end_at must both be provided.')
            
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
            # get timestamp and expected fuels
            ts = raw_dp['']
            fuels = self.get_fuels(ts.year)
            
            for raw_fuel_name, parsed_fuel_name in fuels.iteritems():
                # set up storage
                parsed_dp = {}   
    
                # add values
                parsed_dp['timestamp'] = ts
                try:
                    parsed_dp['gen_MW'] = float(raw_dp[raw_fuel_name])
                except KeyError:
                    self.logger.error('No data for %s found in %s; skipping this time.' % (raw_fuel_name, raw_dp))
                    break
                except ValueError:
                    self.logger.error('Found %s instead of float for %s; skipping time %s.' % (raw_dp[raw_fuel_name], raw_fuel_name, ts))
                    break                    
                parsed_dp['fuel_name'] = parsed_fuel_name
                parsed_dp['ba_name'] = self.ba_name
                if market == 'RTHR':
                    parsed_dp['freq'] = self.FREQUENCY_CHOICES.hourly
                    parsed_dp['market'] = self.MARKET_CHOICES.hourly
                elif market == 'RT5M':
                    parsed_dp['freq'] = self.FREQUENCY_CHOICES.fivemin
                    parsed_dp['market'] = self.MARKET_CHOICES.fivemin
                
                # add to full storage
                parsed_data.append(parsed_dp)
            
        return parsed_data
