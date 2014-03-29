import requests
from dateutil.parser import parse as dateutil_parse
import pytz
import copy
from bs4 import BeautifulSoup
from grid_clients.base import BaseClient


class PJMClient(BaseClient):
    def __init__(self):
        self.ba_name = 'PJM'
        self.base_url = 'http://edatamobile.pjm.com/eDataWireless/SessionManager'

    def _get_edata(self, data_type, key):
        # get request
        r = requests.get(self.base_url, params={'a': data_type})
        soup = BeautifulSoup(r.content)
        
        # get time
        ts_str = soup.find(class_='ts').string
        ts = self._utcify(ts_str)
        
        # get value
        val = None
        for elt in soup.find_all('td'):
            try:
                if elt.find('a').string == key:
                    # numbers may have commas in the thousands
                    val_str = elt.next_sibling.string.replace(',', '')
                    val = float(val_str)
            except AttributeError: # no 'a' child
                continue
            
        if val is None:
            self.logger.error('Data not found in PJM for query %s at %s' % (data_type, ts))
            
        # return
        return ts, val
        
    def _utcify(self, ts_str):
        naive_local_time = dateutil_parse(ts_str)
        is_dst = 'EDT' in ts_str
        aware_local_time = pytz.timezone('America/New_York').localize(naive_local_time, is_dst=is_dst)
        aware_utc_time = aware_local_time.astimezone(pytz.utc)
        return aware_utc_time
        
    def get_generation(self, latest=False, **kwargs):
        # get data
        load_ts, load_val = self._get_edata('instLoad', 'PJM RTO Total')
        imports_ts, imports_val = self._get_edata('tieFlow', 'PJM RTO')
        wind_ts, wind_gen = self._get_edata('wind', 'RTO Wind Power')
        
        # compute nonwind gen
        total_gen = load_val - imports_val
        nonwind_gen = total_gen - wind_gen
        
        # set up storage
        parsed_data = []
        base_dp = {'timestamp': load_ts,
                   'freq': self.FREQUENCY_CHOICES.fivemin, 'market': self.MARKET_CHOICES.fivemin,
                   'gen_MW': 0, 'ba_name': self.ba_name}

        # collect data
        for gen_MW, fuel_name in [(wind_gen, 'wind'), (nonwind_gen, 'nonwind')]:
            parsed_dp = copy.deepcopy(base_dp)
            parsed_dp['fuel_name'] = fuel_name
            parsed_dp['gen_MW'] = gen_MW
            parsed_data.append(parsed_dp)

        # return
        return parsed_data
