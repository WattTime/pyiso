import copy
from bs4 import BeautifulSoup
from pyiso.base import BaseClient
from pyiso import LOGGER
import pandas as pd


class PJMClient(BaseClient):
    NAME = 'PJM'
    TZ_NAME = 'America/New_York'
    base_url = 'https://datasnapshot.pjm.com/content/'
    base_dataminer_url = 'https://dataminer.pjm.com/dataminer/rest/public/api'

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
        dfs = pd.read_html( response.content, header=0, index_col=0, parse_dates=True)
        df = self.utcify_index(dfs[0], tz_name='utc')

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

    def parse_datasnapshot_df(self, ts, df):
        df['timestamp'] = ts

        rename_d = {'LMP': 'lmp',}
        df.rename(columns=rename_d, inplace=True)
        # convert $12.44 to float
        df['lmp'] = df['lmp'].apply(lambda x: float(x.replace('$', '')))

        df['node_id'] = df.index
        df['freq'] = self.options['freq']
        df['market'] = self.options['market']
        df['ba_name'] = 'PJM'
        df['lmp_type'] = 'TotalLMP'
        return df

    def parse_dataminer_df(self, json):
        df = pd.DataFrame(json)

        # drop CongLMP and LossLMP
        df = df[df.priceType == 'TotalLMP']

        # turn nested prices into DataFrame
        df['lmp'] = df['prices'].apply(lambda x: pd.DataFrame.from_dict(x))

        # reindex and drop extra columns
        df = df.reset_index()
        df.drop(['index', 'prices'], axis=1)

        # list of DataFrames to concatenate
        dfs = []
        # group by day
        grouped = df.groupby('publishDate')
        for name, gr in grouped:
            # unpack nested lmp dataframe
            lmps = pd.concat([d.set_index('utchour') for d in gr['lmp']])

            # set high level columns
            for col in ['pnodeId', 'priceType', 'publishDate', 'versionNum']:
                lmps[col] = gr[col].iloc[0]

            dfs.append(lmps)

        retdf = pd.concat(dfs)

        # Convert datetime string to datetime object with timezone
        retdf['timestamp'] = pd.to_datetime(retdf.index, utc=True)

        # rename, drop and add standard columns
        rename_d = {'price':'lmp',
                    'pnodeId': 'node_id',
                    'priceType': 'lmp_type',
                   }
        retdf.rename(columns=rename_d, inplace=True)
        retdf.drop(['publishDate', 'versionNum'], axis=1, inplace=True)
        retdf['freq'] = self.options['freq']
        retdf['market'] = self.options['market']
        retdf['ba_name'] = 'PJM'

        return retdf

    def fetch_dataminer_df(self, endpoint, params):
        url = self.base_dataminer_url + endpoint
        response = self.request(url, mode='post', json=params)
        df = self.parse_dataminer_df(response.json())

        return df

    def handle_options(self, **kwargs):
        super(PJMClient, self).handle_options(**kwargs)

        if 'market' not in self.options:
            self.options['market'] = self.MARKET_CHOICES.dam

        if 'freq' not in self.options:
            self.options['freq'] = self.FREQUENCY_CHOICES.hourly

        if self.options['market'] in (self.MARKET_CHOICES.dam, self.MARKET_CHOICES.hourly):
            # set correct endpoint for lmp data
            endpoints = {
                self.MARKET_CHOICES.dam: '/markets/dayahead/lmp/daily',
                self.MARKET_CHOICES.hourly: '/markets/realtime/lmp/daily',}
            self.options['endpoint'] = endpoints[self.options['market']]
            self.options['method'] = 'dataminer'

        # special handling for five minute lmps
        elif self.options['market'] == self.MARKET_CHOICES.fivemin:
            self.options['method'] = 'datasnapshot'
            self.options['freq'] = self.FREQUENCY_CHOICES.fivemin
            # no historical data for 5min lmp
            self.options['latest'] = True


    def get_lmp(self, node_id=None, **kwargs):
        self.handle_options(data='lmp', **kwargs)

        if self.options['method'] == 'datasnapshot':
            (ts, df) = self.fetch_edata_point('ZonalAggregateLmp', None, None)
            df = self.parse_datasnapshot_df(ts, df)
        else:
            # if getting from dataminer method, setup parameters
            format_str = '%Y-%m-%dT%H:%M:%SZ'  # "1998-04-01T05:00:00Z"
            params = {'startDate': self.options['start_at'].strftime(format_str),
                  'endDate': self.options['end_at'].strftime(format_str),
                  'pnodeList': [node_id]}
            df = self.fetch_dataminer_df(self.options['endpoint'], params=params)



        return df.to_dict(orient='records')


