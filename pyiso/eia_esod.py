from pyiso.base import BaseClient
import json
from os import environ
from dateutil.parser import parse as dateutil_parse
from datetime import datetime, timedelta
import pytz


class EIACLIENT(BaseClient):
    """
    Interface to EIA API.

    The EIA API provides this information for the US lower 48 and beyond:
     -Hourly load (actual and forecast),
     -Generation
     -Imports/exports

    Full listing of BAs with time zones here:
    https://www.eia.gov/beta/realtime_grid/docs/UserGuideAndKnownIssues.pdf

    """

    NAME = 'EIA'

    base_url = 'http://api.eia.gov/'

    fuels = {
        'Other': 'other',
    }

    def __init__(self, *args, **kwargs):
        super(EIACLIENT, self).__init__(*args, **kwargs)
        try:
            self.auth = environ['EIA_KEY']
        except KeyError:
            msg = 'You must define EIA_KEY environment variable to use the \
                   EIA client.'
            raise RuntimeError(msg)

        self.category_url = '{url}category/?api_key={key}&category_id='.format(
            url=self.base_url, key=self.auth)

        self.series_url = '{url}series/?api_key={key}&series_id=EBA.'.format(
            url=self.base_url, key=self.auth)

    def get_generation(self, latest=False, yesterday=False,
                       start_at=False, end_at=False, **kwargs):
        """
        Scrape and parse generation fuel mix data.
        Note: Generation may be quite low for HST and NSB BAs.
        """

        self.handle_options(data='gen', latest=latest, yesterday=yesterday,
                            start_at=start_at, end_at=end_at, **kwargs)
        self.handle_ba_limitations()
        self.format_url()
        result = json.loads(self.request(self.url).text)
        result_formatted = self.format_result(result)

        return result_formatted

    def get_load(self, latest=False, yesterday=False, start_at=False,
                 end_at=False, forecast=False, **kwargs):
        """
        Scrape and parse load data.
        """

        self.handle_options(data='load', latest=latest, yesterday=yesterday,
                            start_at=start_at, end_at=end_at,
                            forecast=forecast, **kwargs)
        self.handle_ba_limitations()
        self.format_url()
        result = json.loads(self.request(self.url).text)
        result_formatted = self.format_result(result)
        return result_formatted

    def get_trade(self, latest=False, yesterday=False, start_at=False,
                  end_at=False, **kwargs):
        """
        Scrape and parse import/export data.
        """

        self.handle_options(data='trade', latest=latest, yesterday=yesterday,
                            start_at=start_at, end_at=end_at, **kwargs)
        self.handle_ba_limitations()
        self.format_url()
        result = json.loads(self.request(self.url).text)
        result_formatted = self.format_result(result)

        return result_formatted

    def handle_options(self, **kwargs):
        """
        Process and store keyword argument options.
        """
        super(EIACLIENT, self).handle_options(**kwargs)

        self.options = kwargs

        """Validate options"""
        if 'latest' not in self.options:
            self.options['latest'] = False
        if 'forecast' not in self.options:
            self.options['forecast'] = False
        if 'market' not in self.options:
            if self.options['forecast']:
                self.options['market'] = self.MARKET_CHOICES.dam
            else:
                self.options['market'] = self.MARKET_CHOICES.hourly
        if 'freq' not in self.options:
            self.options['freq'] = self.FREQUENCY_CHOICES.hourly
        if not self.options['start_at'] and self.options['end_at']:
            raise ValueError('You must specify a start_at date.')
        elif self.options['start_at'] and not self.options['end_at']:
            raise ValueError('You must specify an end_at date.')

        """Clean up time values (same as base.py)"""
        if self.options.get('start_at', None) and self.options.get('end_at', None):
            assert self.options['start_at'] < self.options['end_at']
            self.options['start_at'] = self.utcify(self.options['start_at'])
            self.options['end_at'] = self.utcify(self.options['end_at'])
            self.options['sliceable'] = True
            self.options['latest'] = False

            # force forecast to be True if end_at is in the future
            if self.options['end_at'] > pytz.utc.localize(datetime.utcnow()):
                self.options['forecast'] = True
            else:
                self.options['forecast'] = False

        # set start_at and end_at for yesterday in local time
        elif self.options.get('yesterday', None):
            local_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone(self.TZ_NAME))
            self.options['end_at'] = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
            self.options['start_at'] = self.options['end_at'] - timedelta(days=1)
            self.options['sliceable'] = True
            self.options['latest'] = False
            self.options['forecast'] = False

        # set start_at and end_at for today + tomorrow in local time
        elif self.options.get('forecast', None):
            local_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone(self.TZ_NAME))
            self.options['start_at'] = local_now.replace(microsecond=0)
            self.options['end_at'] = self.options['start_at'] + timedelta(days=2)
            self.options['sliceable'] = True
            self.options['latest'] = False
            self.options['forecast'] = True

        else:
            self.options['sliceable'] = False
            self.options['forecast'] = False

    def handle_ba_limitations(self):
        """Handle BA limitations"""
        today = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone(self.TZ_NAME))
        two_days_ago = today - timedelta(days=2)
        load_not_supported_bas = ['DEAA', 'EEI', 'GRIF', 'GRMA', 'GWA',
                                  'HGMA', 'SEPA', 'WWA', 'YAD']
        delay_bas = ['AEC', 'DOPD', 'GVL', 'HST', 'NSB', 'PGE', 'SCL',
                     'TAL', 'TIDC', 'TPWR']
        if self.options['end_at'] and self.options['bal_auth'] in delay_bas:
            if self.options['end_at'] > two_days_ago:
                raise ValueError('No data: 2 day delay for this BA.')

        if self.options['bal_auth'] in load_not_supported_bas:
            if self.options['data'] == 'load':
                raise ValueError('Load data not supported for this BA.')

    def set_url(self, type, text):
        if type == 'category':
            self.url = '{url}{num}'.format(url=self.category_url,
                                           num=text)
        elif type == 'series':
            self.url = '{url}{ba}{abbrev}'.format(url=self.series_url,
                                                  ba=self.options['bal_auth'],
                                                  abbrev=text)

    def format_url(self):
        """Set EIA API URL based on options"""
        if 'bal_auth' not in self.options:
            if self.data == 'gen':
                self.set_url('category', '2122629')
            elif self.data == 'load':
                if self.options['forecast']:
                    self.set_url('category', '2122627')
                else:
                    self.set_url('category', '2122628')
            elif self.data == 'trade':
                self.set_url('category', '2122632')
        else:
            if self.options['data'] == 'gen':
                self.set_url('series', '-ALL.NG.H')
            elif self.options['data'] == 'load':
                if self.options['forecast']:
                        self.set_url('series', '-ALL.DF.H')
                else:
                    self.set_url('series', '-ALL.D.H')
            elif self.options['data'] == 'trade':
                self.set_url('series', '-ALL.TI.H')

    def format_result(self, data):
        """Output EIA API results in pyiso format"""
        if self.options['forecast']:
            market = 'DAHR'
        else:
            market = 'RTHR'

        if self.options['data'] == 'trade':
            data_type = 'net_exp_MW'
        elif self.options['data'] == 'gen':
            data_type = 'gen_MW'
        elif self.options['data'] == 'load':
            data_type = 'load_MW'

        data_formatted = []

        if self.options['latest']:
            last_datapoint = data['series'][0]['data'][0]
            data_formatted.append(
                                    {
                                        'ba_name': self.options['bal_auth'],
                                        'timestamp': last_datapoint[0],
                                        'freq': self.options['freq'],
                                        data_type: last_datapoint[1],
                                        'market': market
                                    }
                        )
            if self.options['data'] == 'gen':
                for i in data_formatted:
                    i['fuel_name'] = 'other'
        elif self.options['yesterday']:
            yesterday = self.local_now() - timedelta(days=1)

            for i in data['series']:
                for j in i['data']:
                    timestamp = dateutil_parse(j[0])
                    if timestamp.year == yesterday.year and \
                       timestamp.month == yesterday.month and \
                       timestamp.day == yesterday.day:
                        data_formatted.append(
                                            {
                                                'ba_name': self.options['bal_auth'],
                                                'timestamp': j[0],
                                                'freq': self.options['freq'],
                                                data_type: j[1],
                                                'market': market
                                            }
                                        )
            if self.options['data'] == 'gen':
                for i in data_formatted:
                    i['fuel_name'] = 'other'
        else:
            for i in data['series']:
                for j in i['data']:
                    data_formatted.append(
                                        {
                                            'ba_name': self.options['bal_auth'],
                                            'timestamp': j[0],
                                            'freq': self.options['freq'],
                                            data_type: j[1],
                                            'market': market
                                        }
                                    )
            if self.options['data'] == 'gen':
                for i in data_formatted:
                    i['fuel_name'] = 'other'
        return data_formatted
