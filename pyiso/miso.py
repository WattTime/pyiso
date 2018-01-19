from collections import namedtuple
from pyiso.base import BaseClient
from pyiso import LOGGER
import pandas as pd
from io import BytesIO
from datetime import datetime

IntervalChoices = namedtuple('IntervalChoices',
                             ['hourly', 'hourly_prelim', 'fivemin', 'tenmin',
                              'na', 'dam', 'dam_exante'])


class MISOClient(BaseClient):
    NAME = 'MISO'

    base_url = 'https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx'
    docs_url = 'https://docs.misoenergy.org/marketreports/'

    fuels = {
        'Coal': 'coal',
        'Natural Gas': 'natgas',
        'Nuclear': 'nuclear',
        'Other': 'other',
        'Wind': 'wind',
    }

    # MISO is always on utc offset is -5
    # Due to a legacy problem, pytz time zones names are sign reversed
    TZ_NAME = 'Etc/GMT+5'

    MARKET_CHOICES = IntervalChoices(hourly='RTHR', fivemin='RT5M', tenmin='RT5M', na='RT5M',
                                     dam='DAHR', hourly_prelim='RTHR_prelim',
                                     dam_exante='DAHR_exante')

    def get_generation(self, latest=False, **kwargs):
        # set args
        self.handle_options(data='gen', latest=latest, **kwargs)

        # get data
        if self.options['latest']:
            content = self.get_latest_fuel_mix()
            data = self.parse_latest_fuel_mix(content)
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

    def get_latest_fuel_mix(self):
        # set up request
        url = self.base_url + '?messageType=getfuelmix&returnType=csv'

        # carry out request
        response = self.request(url)
        if not response:
            return None

        # test for valid content
        if 'The page cannot be displayed' in response.text:
            LOGGER.error('MISO: Error in source data for generation')
            return None

        # return good
        return response.content

    def parse_latest_fuel_mix(self, content):
        # handle bad input
        if not content:
            return pd.DataFrame()

        # preliminary parsing
        df = pd.read_csv(BytesIO(content), header=0, index_col=0, skiprows=2, parse_dates=True)

        # set index
        try:
            df.index = self.utcify_index(df.index)
        except AttributeError:
            LOGGER.error('MISO: Error in source data for generation %s' % content)
            return pd.DataFrame()
        df.index.set_names(['timestamp'], inplace=True)

        # set names and labels
        df['fuel_name'] = df.apply(lambda x: self.fuels[x['CATEGORY']], axis=1)
        df['gen_MW'] = df['ACT']

        # return
        return df[['fuel_name', 'gen_MW']]

    def handle_forecast(self):
        dates_list = self.dates()
        if min(dates_list) > self.local_now().date():
            dates_list = [self.local_now().date()] + dates_list
        pieces = [self.fetch_forecast(date) for date in dates_list]
        df = pd.concat(pieces)
        return self.parse_forecast(df)

    def fetch_forecast(self, date):
        # construct url
        datestr = date.strftime('%Y%m%d')
        url = self.docs_url +  datestr + '_da_ex.xls'

        # make request with self.request for easier debugging, mocking
        response = self.request(url)
        if not response:
            return pd.DataFrame()
        if response.status_code == 404:
            LOGGER.debug('No MISO forecast data available at %s' % datestr)
            return pd.DataFrame()

        xls = pd.read_excel(BytesIO(response.content))

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
            try:
                sliced['gen_MW'] = 1000.0 * sliced['Supply Cleared (GWh) - Physical']
                sliced['fuel_name'] = 'other'
                return sliced[['gen_MW', 'fuel_name']]
            except KeyError:
                LOGGER.warn('MISO genmix error: missing key %s in %s' % ('Supply Cleared (GWh) - Physical', sliced.columns))
                return pd.DataFrame()

        elif self.options['data'] == 'load':
            try:
                sliced['load_MW'] = 1000.0 * (sliced['Demand Cleared (GWh) - Physical - Fixed'] +
                                              sliced['Demand Cleared (GWh) - Physical - Price Sen.'])
                return sliced['load_MW']
            except KeyError:
                LOGGER.warn('MISO load error: missing key %s in %s' % ('Demand Cleared (GWh) - Physical - Fixed', sliced.columns))
                return pd.DataFrame()

        elif self.options['data'] == 'trade':
            try:
                sliced['net_exp_MW'] = -1000.0 * sliced['Net Scheduled Imports (GWh)']
                return sliced['net_exp_MW']
            except KeyError:
                LOGGER.warn('MISO trade error: missing key %s in %s' % ('Net Scheduled Imports (GWh)', sliced.columns))
                return pd.DataFrame()

        else:
            raise ValueError('Can only parse MISO forecast gen, load, or trade data, not %s'
                             % self.options['data'])

    def handle_options(self, **kwargs):
        super(MISOClient, self).handle_options(**kwargs)
        if 'market' not in self.options:
            self.options['market'] = self.MARKET_CHOICES.dam
            self.options['freq'] = self.FREQUENCY_CHOICES.hourly

        if 'freq' not in self.options:
            self.options['freq'] = self.FREQUENCY_CHOICES.hourly
