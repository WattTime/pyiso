from datetime import datetime, timedelta
import pytz
from dateutil.parser import parse as dateutil_parse
import pandas as pd
from pyiso.base import BaseClient
from pyiso import LOGGER


class BPAClient(BaseClient):
    NAME = 'BPA'

    base_url = 'https://transmission.bpa.gov/business/operations/'

    fuels = {
        'Hydro': 'hydro',
        'Wind': 'wind',
        'Thermal': 'thermal',
    }

    TZ_NAME = 'America/Los_Angeles'

    def fetch_historical(self):
        """Get BPA generation or load data from the far past"""
        # set up requests
        request_urls = []
        this_year = self.options['start_at'].year
        while this_year <= self.options['start_at'].year:
            if this_year >= 2011:
                request_urls.append(self.base_url + 'wind/WindGenTotalLoadYTD_%d.xls' % (this_year))
            else:
                raise ValueError('Cannot get BPA generation data before 2011.')
            this_year += 1

        # set up columns to get
        mode = self.options['data']
        if mode == 'gen':
            cols = [0, 2, 4, 5]
            header_names = ['Wind', 'Hydro', 'Thermal']
        elif mode == 'load':
            cols = [0, 3]
            header_names = ['Load']
        else:
            raise ValueError('Cannot fetch data without a data mode')

        # get each year of data
        pieces = []
        for url in request_urls:
            xd = self.fetch_xls(url)
            piece = self.parse_to_df(xd, mode='xls', sheet_names=xd.sheet_names,
                                     skiprows=18, parse_cols=cols,
                                     index_col=0, parse_dates=True,
                                     header_names=header_names)
            pieces.append(piece)

        # return
        df = pd.concat(pieces)
        return df

    def fetch_recent(self):
        """Get BPA generation or load data from the past week"""
        # request text file
        response = self.request(self.base_url + 'wind/baltwg.txt')

        # set up columns to get
        mode = self.options['data']
        if mode == 'gen':
            cols = [0, 2, 3, 4]
        elif mode == 'load':
            cols = [0, 1]
        else:
            raise ValueError('Cannot fetch data without a data mode')

        # parse like tsv
        if response:
            df = self.parse_to_df(response.text, skiprows=6, header=0, delimiter='\t',
                                  index_col=0, usecols=cols,
                                  date_parser=self.date_parser)
        else:
            LOGGER.warn('No recent data found for BPA %s' % self.options)
            df = pd.DataFrame()

        return df

    def date_parser(self, ts_str):
        ts = dateutil_parse(ts_str)
        return ts

    def fetcher(self):
        """Choose the correct fetcher method for this request"""
        # get mode from options
        mode = self.options.get('data')

        if mode in ['gen', 'load']:
            # default: latest or recent
            fetcher = self.fetch_recent
            if self.options.get('sliceable', None):
                if self.options['start_at'] < pytz.utc.localize(datetime.today() - timedelta(days=7)):
                    # far past
                    fetcher = self.fetch_historical

        else:
            raise ValueError('Cannot choose a fetcher without a data mode')

        return fetcher

    def parse_generation(self, df):
        # process times
        df.index = self.utcify_index(df.index)
        sliced = self.slice_times(df)

        # original header is fuel names
        sliced.rename(columns=self.fuels, inplace=True)
        pivoted = self.unpivot(sliced)
        pivoted.rename(columns={'level_1': 'fuel_name', 0: 'gen_MW'}, inplace=True)

        # return
        return pivoted

    def handle_options(self, **kwargs):
        # default handler
        super(BPAClient, self).handle_options(**kwargs)

        # check kwargs
        market = self.options.get('market', self.MARKET_CHOICES.fivemin)
        if market != self.MARKET_CHOICES.fivemin:
            raise ValueError('Market must be %s' % self.MARKET_CHOICES.fivemin)

    def get_generation(self, latest=False, start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='gen', latest=latest, start_at=start_at, end_at=end_at, **kwargs)

        # fetch dataframe of data
        df = self.fetcher()()

        # return empty list if null
        if len(df) == 0:
            return []

        # parse and clean
        cleaned_df = self.parse_generation(df)

        # serialize and return
        return self.serialize(cleaned_df,
                              header=['timestamp', 'fuel_name', 'gen_MW'],
                              extras={'ba_name': self.NAME,
                                      'market': self.MARKET_CHOICES.fivemin,
                                      'freq': self.FREQUENCY_CHOICES.fivemin})

    def get_load(self, latest=False, start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='load', latest=latest, start_at=start_at, end_at=end_at, **kwargs)

        # fetch dataframe of data
        df = self.fetcher()()

        # return empty list if null
        if len(df) == 0:
            return []

        # parse and clean
        df.index = self.utcify_index(df.index)
        cleaned_df = self.slice_times(df)

        # serialize and return
        return self.serialize(cleaned_df,
                              header=['timestamp', 'load_MW'],
                              extras={'ba_name': self.NAME,
                                      'market': self.MARKET_CHOICES.fivemin,
                                      'freq': self.FREQUENCY_CHOICES.fivemin})
