import copy
from bs4 import BeautifulSoup
from pyiso.base import BaseClient
from pyiso import LOGGER
import pandas as pd


class PJMClient(BaseClient):
    NAME = 'PJM'
    TZ_NAME = 'America/New_York'
    base_url = 'https://datasnapshot.pjm.com/content/'

    def time_as_of(self, content):
        """
        Returns a UTC timestamp if one is found in the html content,
        or None if an error was encountered.
        """
        # soup it up
        soup = BeautifulSoup(content)

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

        # parse html to df
        dfs = pd.read_html(response.content, header=0, index_col=0)
        df = dfs[0]
        val = df.loc[key][header]

        # return
        return ts, val

    def fetch_edata_series(self, data_type, params=None):
        # get request
        url = self.base_url + data_type + '.aspx'
        response = self.request(url, params=params)
        if not response:
            return pd.Series()

        # parse html to df
        dfs = pd.read_html(response.content, header=0, index_col=0, parse_dates=True)
        df = self.utcify_index(dfs[0])

        # return df
        return df

    def get_load(self, latest=False, start_at=None, end_at=None, forecast=False, **kwargs):
        # set args
        self.handle_options(data='load', latest=latest,
                            start_at=start_at, end_at=end_at, forecast=forecast,
                            **kwargs)

        if self.options['forecast']:
            # handle forecast
            df = self.fetch_edata_series('ForecastedLoadHistory', {'name': 'PJM RTO Total'})
            sliced = self.slice_times(df)
            sliced.columns = ['load_MW']
            sliced.index.set_names(['timestamp'], inplace=True)

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

            # format and return
            if load_ts and load_val:
                return [{
                        'timestamp': load_ts,
                        'freq': self.FREQUENCY_CHOICES.fivemin,
                        'market': self.MARKET_CHOICES.fivemin,
                        'load_MW': load_val,
                        'ba_name': self.NAME,
                        }]
            else:
                return []

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
