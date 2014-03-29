import requests
import copy
from datetime import datetime, timedelta
from dateutil.parser import parse as dateutil_parse
import pytz
import pandas as pd
import urllib2
from pyiso.base import BaseClient


class BPAClient(BaseClient):
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
        
    def _request_tsv(self, url, latest, start_at, end_at):
        # set up
        raw_data = []
        response = requests.get(url)
        rows = response.text.split('\n')
        header = [x.strip() for x in rows[6].split('\t')]
        
        # process rows
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
        return raw_data

    def _request_xls(self, url, latest, start_at, end_at):
        # set up
        raw_data = []
        socket = urllib2.urlopen(url)
        xd = pd.ExcelFile(socket)
        
        for sheet in xd.sheet_names:
            df = xd.parse(sheet, skiprows=18, parse_cols=[0, 2, 4, 5])
            df.columns = ['Date/Time', 'Wind', 'Hydro', 'Thermal']
            
            for vals in df.values:
                try:
                    vals[0] = self._utcify(dateutil_parse(vals[0]))
                except TypeError: # last row can be missing data
                    continue

                # save valid values
                if not pd.isnull(vals).any():
                    if latest: # overwrite list every time
                        raw_data = [dict(zip(df.columns, vals))]
                    else: # save if date in range
                        if vals[0] >= start_at and vals[0] <= end_at:
                            raw_data.append(dict(zip(df.columns, vals)))
                
        return raw_data

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
                this_year = start_at.year
                while this_year <= end_at.year:
                    if this_year >= 2011:
                        request_urls.append('wind/WindGenTotalLoadYTD_%d.xls' % (this_year))
                    else:
                        raise ValueError('Cannot get BPA generation data before 2011.')
                    this_year += 1
        else:
            raise ValueError('Either latest must be True, or start_at and end_at must both be provided.')
        market = kwargs.get('market', self.MARKET_CHOICES.fivemin)
        if market != self.MARKET_CHOICES.fivemin:
            raise ValueError('Market must be %s' % self.MARKET_CHOICES.fivemin)
            
        # set up storage
        raw_data = []
        parsed_data = []

        # collect raw data
        for request_url in request_urls:
            # set up request
            url = copy.deepcopy(self.base_url)
            url += request_url
            
            # carry out request with preliminary parsing
            if 'xls' in url:
                raw_data += self._request_xls(url, latest, start_at, end_at)
            else:
                raw_data += self._request_tsv(url, latest, start_at, end_at)
                        
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
                parsed_dp['market'] = self.MARKET_CHOICES.fivemin
                parsed_dp['freq'] = self.FREQUENCY_CHOICES.fivemin
                
                # add to full storage
                parsed_data.append(parsed_dp)
            
        return parsed_data
