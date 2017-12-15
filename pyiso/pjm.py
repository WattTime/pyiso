from bs4 import BeautifulSoup
from pyiso.base import BaseClient
from pyiso import LOGGER
import pandas as pd
from dateutil.parser import parse
import pytz
from datetime import datetime, timedelta
import re
import json


class PJMClient(BaseClient):
    NAME = 'PJM'
    TZ_NAME = 'America/New_York'
    base_url = 'https://datasnapshot.pjm.com/content/'
    base_dataminer_url = 'https://dataminer.pjm.com/dataminer/rest/public/api'
    oasis_url = 'http://oasis.pjm.com/system.htm'
    markets_operations_url = 'http://www.pjm.com/markets-and-operations.aspx'

    zonal_aggregate_nodes = {
        'AECO': 51291,
        'AEP': 8445784,
        'APS': 8394954,
        'ATSI': 116013753,
        'BGE': 51292,
        'COMED': 33092371,
        'DAY': 34508503,
        'DEOK': 124076095,
        'DOM': 34964545,
        'DPL': 51293,
    }

    fuels = {
        'Coal': 'coal',
        'Gas': 'natgas',
        'Nuclear': 'nuclear',
        'Other': 'other',
        'Wind': 'wind',
        'Solar': 'solar',
        'Other Renewables': 'renewable',
        'Oil': 'oil',
        'Other': 'other',
        'Multiple Fuels': 'thermal',
        'Hydro': 'hydro',
        'Black Liquor': 'other', # Is this the right mapping? What about 'thermal'? 'other'?
        'Storage': 'other', # Seems to be new
    }

    def time_as_of(self, content):
        """
        Returns a UTC timestamp if one is found in the html content,
        or None if an error was encountered.
        """
        # soup it up
        soup = BeautifulSoup(content, 'lxml')

        # like 12.11.2015 17:15
        ts_elt = soup.find(id='ctl00_ContentPlaceHolder1_DateAndTime')
        if not ts_elt:
            LOGGER.error('PJM: Timestamp not found in soup:\n%s' % soup)
            return None
        ts_str = ts_elt.string

        # EDT or EST
        tz_elt = ts_elt.next_sibling
        tz_str = tz_elt.string.strip()
        is_dst = tz_str == 'EDT'

        # utcify and return
        return self.utcify(ts_str, is_dst=is_dst)

    def fetch_edata_point(self, data_type, key, header):
        # get request
        url = self.base_url + data_type + '.aspx'
        response = self.request(url)
        if not response:
            return None, None

        # get time as of
        ts = self.time_as_of(response.content)

        # round down to 5min
        extra_min = ts.minute % 5
        ts -= timedelta(minutes=extra_min)

        # parse html to df
        dfs = pd.read_html(response.content, header=0, index_col=0)
        df = dfs[0]
        if key and header:
            val = df.loc[key][header]
        else:
            val = df

        # return
        return ts, val

    def fetch_edata_series(self, data_type, params=None):
        # get request
        url = self.base_url + data_type + '.aspx'
        response = self.request(url, params=params)
        if not response:
            return pd.Series()

        # parse html to df
        dfs = pd.read_html(response.content, header=0, index_col=0)
        df = dfs[0]
        df.index = pd.to_datetime(df.index, utc=True)
        df.index.set_names(['timestamp'], inplace=True)

        # return df
        return df

    def request(self, *args, **kwargs):
        response = super(PJMClient, self).request(*args, **kwargs)
        if response and response.status_code == 400:
            LOGGER.warn('PJM request returned Bad Request %s' % response)
            return None

        return response

    def fetch_historical_load(self, year, region_name='RTO'):
        # get RTO data
        url = 'http://www.pjm.com/pub/operations/hist-meter-load/%s-hourly-loads.xls' % year
        df = pd.read_excel(url, sheetname=region_name)

        # drop unneeded cols
        drop_cols = ['Unnamed: %d' % i for i in range(35)]
        drop_cols += ['MAX', 'HOUR', 'DATE.1', 'MIN', 'HOUR.1', 'DATE.2']
        df.drop(drop_cols, axis=1, inplace=True, errors='ignore')

        # reshape from wide to tall
        df = pd.melt(df, id_vars=['DATE', 'COMP'])

        # HE01, HE02, ... HE24; hour ending in local time
        # convert to hour beginning as integer
        df['hour'] = df['variable'].str.strip('HE').astype(int) - 1

        # set naive local datetime column
        df['datetime_str'] = (pd.to_datetime(df['DATE']).astype(str) + ':' +
                              df['hour'].astype(str).str.zfill(2))
        df['timestamp'] = pd.to_datetime(df['datetime_str'], format='%Y-%m-%d:%H')

        # utcify
        # TODO handle DST transitions properly, this just returns Not a Time
        # and utcify_index fails with AmbiguousTimeError, even with ambiguous='infer'
        f = lambda x: pytz.timezone(self.TZ_NAME).localize(x['timestamp'])
        df['timestamp'] = df.apply(f, axis=1)
        df.set_index('timestamp', inplace=True)
        df = self.utcify_index(df)

        # drop unneeded cols
        drop_col = ['datetime_str', 'DATE', 'hour', 'variable', 'COMP']
        df.drop(drop_col, axis=1, inplace=True)

        # add formatting
        df.rename(columns={'value': 'load_MW'}, inplace=True)

        # Drop the couple of times around DST transition that we don't handle correctly
        df.dropna(subset=['load_MW'], inplace=True)
        return df

    def get_load(self, latest=False, start_at=None, end_at=None, forecast=False, **kwargs):
        # set args
        self.handle_options(data='load', latest=latest,
                            start_at=start_at, end_at=end_at, forecast=forecast,
                            **kwargs)

        if self.options['forecast']:
            # fetch from eData
            df = self.fetch_edata_series('ForecastedLoadHistory', {'name': 'PJM RTO Total'})
            sliced = self.slice_times(df)
            sliced.columns = ['load_MW']

            # format
            extras = {
                'freq': self.FREQUENCY_CHOICES.hourly,
                'market': self.MARKET_CHOICES.dam,
                'ba_name': self.NAME,
            }
            data = self.serialize_faster(sliced, extras=extras)

            # return
            return data

        elif self.options['end_at'] and self.options['end_at'] < datetime.now(pytz.utc) - timedelta(hours=1):
            df = self.fetch_historical_load(self.options['start_at'].year)
            sliced = self.slice_times(df)

            # format
            extras = {
                'freq': self.FREQUENCY_CHOICES.hourly,
                'market': self.MARKET_CHOICES.dam,
                'ba_name': self.NAME,
            }
            data = self.serialize_faster(sliced, extras=extras)

            # return
            return data

        else:
            # handle real-time
            load_ts, load_val = self.fetch_edata_point('InstantaneousLoad', 'PJM RTO Total', 'MW')

            # fall back to OASIS
            if not (load_ts and load_val):
                load_ts, load_val = self.fetch_oasis_data()
            if not (load_ts and load_val):
                LOGGER.warn('No PJM latest load data')
                return []

            # format and return
            return [{
                'timestamp': load_ts,
                'freq': self.FREQUENCY_CHOICES.fivemin,
                'market': self.MARKET_CHOICES.fivemin,
                'load_MW': load_val,
                'ba_name': self.NAME,
            }]

    def get_trade(self, latest=False, **kwargs):
        # set args
        self.handle_options(data='trade', latest=latest, **kwargs)

        if not self.options['latest']:
            raise ValueError('Only latest trade values available in PJM')

        # handle real-time imports
        ts, val = self.fetch_edata_point('TieFlows', 'PJM RTO', 'Actual (MW)')

        # format and return
        if ts and val:
            return [{
                    'timestamp': ts,
                    'freq': self.FREQUENCY_CHOICES.fivemin,
                    'market': self.MARKET_CHOICES.fivemin,
                    'net_exp_MW': -val,
                    'ba_name': self.NAME,
                    }]
        else:
            return []

    def handle_options(self, **kwargs):
        super(PJMClient, self).handle_options(**kwargs)

        # load specific options
        if self.options['data'] == 'load':
            if not self.options['latest']:
                # for historical, only DAHR load allowed
                if self.options.get('market'):
                    if self.options['market'] != self.MARKET_CHOICES.dam:
                        raise ValueError('PJM historical load data only available for %s' % self.MARKET_CHOICES.dam)
                else:
                    self.options['market'] = self.MARKET_CHOICES.dam

        # gen specific options
        if self.options['data'] == 'gen':
            if not self.options['latest']:
                raise ValueError('PJM generation mix only available with latest=True')

    def parse_date_from_oasis(self, content):
        # find timestamp
        soup = BeautifulSoup(content, 'lxml')

        # the datetime is the only bold text on the page, this could break easily
        ts_elt = soup.find('b')

        # do not pass tzinfos argument to dateutil.parser.parse, it fails arithmetic
        ts = parse(ts_elt.string, ignoretz=True)
        ts = pytz.timezone('US/Eastern').localize(ts)
        ts = ts.astimezone(pytz.utc)

        # return
        return ts

    def fetch_oasis_data(self):
        response = self.request(self.oasis_url)
        if not response:
            return None, None

        # get timestamp
        ts = self.parse_date_from_oasis(response.content)

        # parse to dataframes
        dfs = pd.read_html(response.content, header=0, index_col=0, parse_dates=False)

        if self.options['data'] == 'load':
            # parse real-time load
            df = dfs[4]
            load_val = df.loc['PJM RTO'][0]
            return ts, load_val
        else:
            raise ValueError('Cannot parse OASIS load data for %s' % self.options['data'])

    def fetch_markets_operations_soup(self):
        response = self.request(self.markets_operations_url)

        if not response:
            return None

        soup = BeautifulSoup(response.content, 'lxml')
        return soup

    def parse_date_from_markets_operations(self, soup):
        # get text of element with timestamp
        elt = soup.find(id='genFuelMix')
        time_str = elt.find(id='asOfDate').contents[0]

        # string like ' As of 6:00 p.m. EPT'
        time_str = time_str.replace(' As of ', '')

        # error at 10pm?
        try:
            naive_local_ts = parse(time_str)
        except ValueError:
            raise ValueError('Error parsing %s from %s' % (time_str, elt))

        # return
        return self.utcify(naive_local_ts)

    def parse_realtime_genmix(self, soup):
        # get text of element with data
        elt = soup.find(id='genFuelMix')
        data_str = elt.find(id='rtschartallfuelspjmGenFuel_container').next_sibling.contents[0]

        # set up regex to match data json
        match = re.search(r'data: \[.*?\]', data_str)
        match_str = match.group(0)

        # transform from json
        json_str = '{' + match_str + '}'
        json_str = json_str.replace('data:', '"data":')
        json_str = json_str.replace('color:', '"color":')
        json_str = json_str.replace('name:', '"name":')
        json_str = json_str.replace('y:', '"y":')
        json_str = json_str.replace('\'', '"')
        raw_data = json.loads(json_str)

        # get date
        try:
            ts = self.parse_date_from_markets_operations(soup)
        except ValueError:
            # error handling date, assume no data
            return []

        # parse data
        data = []

        for raw_dp in raw_data['data']:
            dp = {
                'timestamp': ts,
                'gen_MW': raw_dp['y'],
                'fuel_name': self.fuels[raw_dp['name']],
                'freq': self.FREQUENCY_CHOICES.hourly,
                'market': self.MARKET_CHOICES.hourly,
                'ba_name': self.NAME,
            }
            data.append(dp)

        # return
        return data

    def get_generation(self, latest=False, **kwargs):
        # handle options
        self.handle_options(data='gen', latest=latest, **kwargs)

        # fetch and parse
        soup = self.fetch_markets_operations_soup()

        if soup:
            data = self.parse_realtime_genmix(soup)
        else:
            return []

        # return
        return data
