from pyiso.base import BaseClient
from pyiso import LOGGER
import pandas as pd
try:
    from urllib2 import HTTPError
except ImportError:
    from urllib.error import HTTPError
from io import StringIO
from datetime import datetime
import pytz
from dateutil.parser import parse


class MISOClient(BaseClient):
    NAME = 'MISO'

    base_url = 'https://www.misoenergy.org'

    fuels = {
        'Coal': 'coal',
        'Natural Gas': 'natgas',
        'Nuclear': 'nuclear',
        'Other': 'other',
        'Wind': 'wind',
    }

    TZ_NAME = 'America/New_York'

    def utcify(self, local_ts, **kwargs):
        # MISO is always on Eastern Standard Time, even during DST
        # ie UTC offset = -5 always
        utc_ts = super(MISOClient, self).utcify(local_ts, is_dst=False)
        utc_ts += utc_ts.astimezone(pytz.timezone(self.TZ_NAME)).dst()  # adjust for EST
        return utc_ts

    def get_generation(self, latest=False, **kwargs):
        # set args
        self.handle_options(data='gen', latest=latest, **kwargs)

        # get data
        if self.options['latest']:
            data = self.latest_fuel_mix()
            extras = {
                'ba_name': self.NAME,
                'market': self.MARKET_CHOICES.fivemin,
                'freq': self.FREQUENCY_CHOICES.fivemin,
            }
        elif self.options['forecast']:
            data = self.handle_forecast()
            extras = {
                'ba_name': self.NAME,
                'market': self.MARKET_CHOICES.dam,
                'freq': self.FREQUENCY_CHOICES.hourly,
            }
        else:
            raise ValueError('Either latest or forecast must be True')

        # return
        return self.serialize_faster(data, extras=extras)

    def get_load(self, latest=False, **kwargs):
        # set args
        self.handle_options(data='load', latest=latest, **kwargs)

        # get data
        if self.options['forecast']:
            data = self.handle_forecast()
            extras = {
                'ba_name': self.NAME,
                'market': self.MARKET_CHOICES.dam,
                'freq': self.FREQUENCY_CHOICES.hourly,
            }
        else:
            raise ValueError('forecast must be True')

        # return
        return self.serialize_faster(data, extras=extras)

    def get_trade(self, latest=False, **kwargs):
        # set args
        self.handle_options(data='trade', latest=latest, **kwargs)

        # get data
        if self.options['forecast']:
            data = self.handle_forecast()
            extras = {
                'ba_name': self.NAME,
                'market': self.MARKET_CHOICES.dam,
                'freq': self.FREQUENCY_CHOICES.hourly,
            }
        else:
            raise ValueError('forecast must be True')

        # return
        return self.serialize_faster(data, extras=extras)

    def latest_fuel_mix(self):
        # set up request
        url = self.base_url + '/ria/FuelMix.aspx?CSV=True'

        # carry out request
        response = self.request(url)
        if not response:
            return pd.DataFrame()

        # test for valid content
        if 'The page cannot be displayed' in response.text:
            LOGGER.error('MISO: Error in source data for generation')
            return pd.DataFrame()

        # preliminary parsing
        df = pd.read_csv(StringIO(response.text), header=0, index_col=0, parse_dates=True)

        # set index
        df.index = self.utcify_index(df.index)
        df.index.set_names(['timestamp'], inplace=True)

        # set names and labels
        df['fuel_name'] = df.apply(lambda x: self.fuels[x['CATEGORY']], axis=1)
        df['gen_MW'] = df['ACT']

        # return
        return df[['fuel_name', 'gen_MW']]

    def handle_forecast(self):
        dates_list = self.dates()
        if min(dates_list) > self.local_now().date():
            dates_list = [self.local_now().date()]
        pieces = [self.fetch_forecast(date) for date in dates_list]
        df = pd.concat(pieces)
        return self.parse_forecast(df)

    def fetch_forecast(self, date):
        # construct url
        datestr = date.strftime('%Y%m%d')
        url = self.base_url + '/Library/Repository/Market%20Reports/' + datestr + '_da_ex.xls'

        # make request
        try:
            xls = pd.read_excel(url)
        except HTTPError:
            LOGGER.debug('No MISO forecast data available at %s' % datestr)
            return pd.DataFrame()

        # clean header
        header_df = xls.iloc[:5]
        df = xls.iloc[5:]
        df.columns = ['hour_str'] + list(header_df.iloc[-1][1:])

        # set index
        idx = []
        for hour_str in df['hour_str']:
            # format like 'Hour 01' to 'Hour 24'
            ihour = int(hour_str[5:]) - 1
            local_ts = datetime(date.year, date.month, date.day, ihour)
            idx.append(self.utcify(local_ts))
        df.index = idx
        df.index.set_names(['timestamp'], inplace=True)

        # return
        return df

    def parse_forecast(self, df):
        sliced = self.slice_times(df)

        if self.options['data'] == 'gen':
            sliced['gen_MW'] = 1000.0 * sliced['Supply Cleared (GWh) - Physical']
            sliced['fuel_name'] = 'other'
            return sliced[['gen_MW', 'fuel_name']]

        elif self.options['data'] == 'load':
            sliced['load_MW'] = 1000.0 * (sliced['Demand Cleared (GWh) - Physical - Fixed'] + sliced['Demand Cleared (GWh) - Physical - Price Sen.'])
            return sliced['load_MW']

        elif self.options['data'] == 'trade':
            sliced['net_exp_MW'] = -1000.0 * sliced['Net Scheduled Imports (GWh)']
            return sliced['net_exp_MW']

        else:
            raise ValueError('Can only parse MISO forecast gen, load, or trade data, not %s' % self.options['data'])


    def get_realtime_lmp(self, node_id, **kwargs):
        # get csv with latest 5 minute data
        url = self.base_url + '/ria/Consolidated.aspx?format=csv'
        response = self.request(url)

        # parse data into DataFrame
        data = StringIO(response.text)
        df = pd.read_csv(data, skiprows=[1,3], header=None)

        # parse timestamp from column name, add timezone
        ts = df.iloc[0,13]
        ts = ts.replace('RefId=', '')
        ts = parse(ts, ignoretz=True)
        ts = self.utcify(ts)

        #MEC = Marginal Energy Component (unconstrained LMP)
        #MCC = Marginal Congestion Component (GSF X Marginal Value)
        # drop MEC and MCC prices
        drop_col = [2,3,5,6,8,9,11,12,13]

        # drop Ex/Post Ante prices
        import pytest; pytest.set_trace()

        df.drop(drop_col, axis=1, inplace=True)

        # drop headers
        df.drop([0,1], axis=0, inplace=True)

        # add columns
        df.rename(columns={0: 'node_id', 1: 'lmp'}, inplace=True)
        df['timestamp'] = ts
        df['ba_name'] = 'MISO'
        df['lmp_type'] = 'TotalLMP'
        df['freq'] = self.FREQUENCY_CHOICES.fivemin
        df['market'] = self.MARKET_CHOICES.fivemin

        # parse lmp as int
        df['lmp'] = df['lmp'].astype(float)

        return df

    def get_historical_lmp(self):
         url = self.base_url + '/Library/Repository/Market%20Reports/' + datestr + '_da_ex.xls'

    def get_lmp(self, node_id, latest=True, **kwargs):
        self.handle_options(latest=latest, **kwargs)

        if self.options['latest']:
            df = self.get_realtime_lmp(node_id, **kwargs)

        else:
            #TODO waiting on MISO market portal access
            pass

        return df.to_dict(orient='records')






