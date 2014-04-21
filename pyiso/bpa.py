import StringIO
import copy
from datetime import datetime, timedelta
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

        self.TZ_NAME = 'America/Los_Angeles'
       
    def get_generation_historical(self, start_at, end_at, **kwargs):
        # set up requests
        request_urls = []
        this_year = start_at.year
        while this_year <= end_at.year:
            if this_year >= 2011:
                request_urls.append(self.base_url + 'wind/WindGenTotalLoadYTD_%d.xls' % (this_year))
            else:
                raise ValueError('Cannot get BPA generation data before 2011.')
            this_year += 1

        # set up storage
        parsed_data = []

        # get each year of data
        for url in request_urls:

            # set up
            raw_data = []
            socket = urllib2.urlopen(url)
            xd = pd.ExcelFile(socket)
            
            for sheet in xd.sheet_names:
                df = xd.parse(sheet, skiprows=18, parse_cols=[0, 2, 4, 5])
                df.columns = ['Date/Time', 'Wind', 'Hydro', 'Thermal']
                
                for vals in df.values:
                    try:
                        vals[0] = self.utcify(vals[0])
                    except TypeError: # last row can be missing data
                        continue

                    # save valid values
                    if not pd.isnull(vals).any():
                        if vals[0] >= start_at and vals[0] <= end_at:
                                raw_data.append(dict(zip(df.columns, vals)))
                    
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


    def get_generation_recent(self, latest=False, start_at=False, end_at=False, **kwargs):
        """Get BPA generation data from the past week"""
        # request text file
        response = self.request(self.base_url + 'wind/baltwg.txt')

        # parse like tsv
        filelike = StringIO.StringIO(response.text)
        df = self.parse_to_df(filelike, skiprows=6, header=0, delimiter='\t',
                            index_col=0, parse_dates=True, usecols=[0, 2, 3, 4])

        # original header is fuel names
        df.rename(columns=self.fuels, inplace=True)
        pivoted = self.unpivot(df)
        pivoted.rename(columns={'level_1': 'fuel_name', 0: 'gen_MW'}, inplace=True)

        # process times
        pivoted.index = self.utcify_index(pivoted.index)
        sliced = self.slice_times(pivoted, latest, start_at, end_at)

        # serialize
        data = self.serialize(sliced,
                              header=['timestamp', 'fuel_name', 'gen_MW'],
                              extras={'ba_name': self.ba_name,
                                      'market': self.MARKET_CHOICES.fivemin,
                                      'freq': self.FREQUENCY_CHOICES.fivemin})

        # return
        return data

    def get_generation(self, latest=False, start_at=False, end_at=False, **kwargs):
        # check kwargs
        market = kwargs.get('market', self.MARKET_CHOICES.fivemin)
        if market != self.MARKET_CHOICES.fivemin:
            raise ValueError('Market must be %s' % self.MARKET_CHOICES.fivemin)

        if start_at and end_at:
            # check start_at and end_at args
            assert start_at < end_at
            start_at = self.utcify(start_at)
            end_at = self.utcify(end_at)

            if start_at < pytz.utc.localize(datetime.today() - timedelta(days=7)):
                # far past
                return self.get_generation_historical(start_at, end_at, **kwargs)

        # latest or recent
        return self.get_generation_recent(latest=latest, start_at=start_at, end_at=end_at, **kwargs)
