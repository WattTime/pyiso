from pyiso.base import BaseClient
import json
from os import environ
from dateutil.parser import parse as dateutil_parse
from datetime import datetime, timedelta
import pytz
from pyiso import LOGGER


class EIAClient(BaseClient):
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

    base_url = 'http://api.eia.gov'

    fuels = {
        'Other': 'other',
    }

    FUEL_CHOICES = ['other']

    EIA_BAs = ['AEC', 'AECI', 'AESO', 'AVA', 'AZPS', 'BANC', 'BCTC',
               'BPAT', 'CISO', 'CFE', 'CHPD', 'CISO', 'CPLE', 'CPLW',
               'DEAA', 'DOPD', 'DUK', 'EEI', 'EPE', 'ERCO', 'FMPP',
               'FPC', 'FPL', 'GCPD', 'GRID', 'GRIF', 'GRMA', 'GVL',
               'GWA', 'HGMA', 'HQT', 'HST', 'IESO', 'IID', 'IPCO',
               'ISNE', 'JEA', 'LDWP', 'LGEE', 'MHEB', 'MISO', 'NBSO',
               'NEVP', 'NSB', 'NWMT', 'NYIS', 'OVEC', 'PACE', 'PACW',
               'PGE', 'PJM', 'PNM', 'PSCO', 'PSEI', 'SC', 'SCEG',
               'SCL', 'SEC', 'SEPA', 'SOCO', 'SPA', 'SPC', 'SRP',
               'SWPP', 'TAL', 'TEC', 'TEPC', 'TIDC', 'TPWR', 'TVA',
               'WACM', 'WALC', 'WAUW', 'WWA', 'YAD']

    def __init__(self, *args, **kwargs):
        # start here- add method to set BA
        # would need to add ba as a parameter

        super(EIAClient, self).__init__(*args, **kwargs)
        try:
            self.auth = environ['EIA_KEY']
        except KeyError:
            msg = 'You must define EIA_KEY environment variable to use the \
                   EIA client.'
            raise RuntimeError(msg)

        self.TZ_NAME = 'UTC'

    def set_ba(self, bal_auth):
        if bal_auth in self.EIA_BAs:
            self.BA = bal_auth
        else:
            LOGGER.error('Unknown BA: %s' % bal_auth)
            raise ValueError('Unknown BA: %s' % bal_auth)

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
        result = self.request(self.url)
        if result is not None:
            result_json = json.loads(result.text)
            result_formatted = self.format_result(result_json)
            return result_formatted
        else:
            LOGGER.error('No results for %s' % self.BA)
            return []

    def get_load(self, latest=False, yesterday=False, start_at=False,
                 end_at=False, forecast=False, **kwargs):
        """
        Scrape and parse load data.
        """

        self.handle_options(data='load', latest=latest, start_at=start_at,
                            end_at=end_at, **kwargs)
        self.handle_ba_limitations()
        self.format_url()
        result = self.request(self.url)
        if result is not None:
            result_json = json.loads(result.text)
            result_formatted = self.format_result(result_json)
            return result_formatted
        else:
            LOGGER.error('No results for %s' % self.BA)
            return []

    def get_trade(self, latest=False, yesterday=False, start_at=False,
                  end_at=False, **kwargs):
        """
        Scrape and parse import/export data.
        """

        self.handle_options(data='trade', latest=latest, yesterday=yesterday,
                            start_at=start_at, end_at=end_at, **kwargs)
        self.handle_ba_limitations()
        self.format_url()
        result = self.request(self.url)
        if result is not None:
            result_json = json.loads(result.text)
            result_formatted = self.format_result(result_json)
            return result_formatted
        else:
            LOGGER.error('No results for %s' % self.BA)
            return []

    def handle_options(self, **kwargs):
        """
        Process and store keyword argument options.
        """
        super(EIAClient, self).handle_options(**kwargs)

        if not hasattr(self, 'BA'):
            LOGGER.error('Balancing authority not set.')
            raise ValueError('Balancing authority not set.')

        if 'market' not in self.options:
            if self.options['forecast']:
                self.options['market'] = self.MARKET_CHOICES.dam
            elif self.options['sliceable'] and self.options['data'] == 'gen':
                self.options['market'] = self.MARKET_CHOICES.dam
            else:
                self.options['market'] = self.MARKET_CHOICES.hourly
        if 'freq' not in self.options:
            if self.options['forecast']:
                self.options['freq'] = self.FREQUENCY_CHOICES.hourly
            elif self.options['sliceable'] and self.options['data'] == 'gen':
                self.options['freq'] = self.FREQUENCY_CHOICES.hourly
            else:
                self.options['freq'] = self.FREQUENCY_CHOICES.hourly
        if 'yesterday' not in self.options:
            self.options['yesterday'] = False

    def handle_ba_limitations(self):
        """Handle BA limitations"""
        today = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone(self.TZ_NAME))
        two_days_ago = today - timedelta(days=2)
        load_not_supported_bas = ['DEAA', 'EEI', 'GRIF', 'GRMA', 'GWA',
                                  'HGMA', 'SEPA', 'WWA', 'YAD']
        delay_bas = ['AEC', 'DOPD', 'GVL', 'HST', 'NSB', 'PGE', 'SCL',
                     'TAL', 'TIDC', 'TPWR']
        canada_mexico = ['IESO', 'BCTC', 'MHEB', 'AESO', 'HQT', 'NBSO',
                         'CFE', 'SPC']
        if self.BA in delay_bas:
            if self.options['end_at'] and self.options['end_at'] > two_days_ago:
                LOGGER.error('No data for %s due to 2 day delay' % self.BA)
                raise ValueError('No data: 2 day delay for this BA.')
            elif self.options['yesterday']:
                LOGGER.error('No data for %s due to 2 day delay' % self.BA)
                raise ValueError('No data: 2 day delay for this BA.')
            elif self.options['forecast']:
                raise ValueError('No data: 2 day delay for this BA.')

        if self.BA in load_not_supported_bas:
            if self.options['data'] == 'load':
                LOGGER.error('Load data not supported for %s' % self.BA)
                raise ValueError('Load data not supported for this BA.')
        if self.BA in canada_mexico:
            LOGGER.error('Data not supported for %s' % self.BA)
            raise ValueError('Data not currently supported for Canada and Mexico')

    def set_url(self, type, series_id_suffix):
        url_format = '{base_url}/series/?api_key={api_key}&series_id=EBA.{ba}{suffix}'
        self.url = url_format.format(base_url=self.base_url, api_key=self.auth, ba=self.BA, suffix=series_id_suffix)

    def format_url(self):
        """Set EIA API URL based on options"""
        if self.options['data'] == 'gen':
            if self.options['forecast']:
                LOGGER.error('Forecast not supported for generation.')
                raise ValueError('Forecast not supported for generation.')
            else:
                self.set_url('series', '-ALL.NG.H')
        elif self.options['data'] == 'load':
            if self.options['forecast']:
                    self.set_url('series', '-ALL.DF.H')
            else:
                self.set_url('series', '-ALL.D.H')
        elif self.options['data'] == 'trade':
            if self.options['forecast']:
                LOGGER.error('Forecast not supported for generation.')
                raise ValueError('Forecast not supported for trade.')
            elif self.options['end_at']:
                if self.options['end_at'] > pytz.utc.localize(datetime.utcnow()):
                    LOGGER.error('Forecast not supported for generation.')
                    raise ValueError('Forecast not supported for trade.')
                else:
                    self.set_url('series', '-ALL.TI.H')
            else:
                self.set_url('series', '-ALL.TI.H')

    def format_data(self, data):
        """Convert load data to int, handle None"""
        if data is None:
            return 0
        else:
            return int(data)

    def add_gen_data(self, data_list):
        for i in data_list:
            i['fuel_name'] = 'other'
        return data_list

    def _set_market(self):
        if self.options['forecast']:
            mkt = 'DAHR'
        else:
            mkt = 'RTHR'
        return mkt

    def _set_data_type(self):
        if self.options['data'] == 'trade':
            data_type = 'net_exp_MW'
        elif self.options['data'] == 'gen':
            data_type = 'gen_MW'
        elif self.options['data'] == 'load':
            data_type = 'load_MW'
        return data_type

    def _format_list(self, data, timestamp, d_type, mkt):
        pyiso_format = {
                        'ba_name': self.BA,
                        'timestamp': timestamp,
                        'freq': self.options['freq'],
                        d_type: data,
                        'market': mkt
                    }
        return pyiso_format

    def _format_latest(self, data, d_type, mkt):
        formatted_list = []
        last_datapoint = data['series'][0]['data'][0]
        timestamp = self.utcify(dateutil_parse(last_datapoint[0]))
        data = self.format_data(last_datapoint[1])
        formatted = self._format_list(data, timestamp, d_type, mkt)
        formatted_list.append(formatted)  # will be just one
        return formatted_list

    def _format_yesterday(self, data, d_type, mkt):
        formatted_list = []
        yesterday = self.local_now() - timedelta(days=1)
        for i in data['series']:
            for j in i['data']:
                timestamp = self.utcify(dateutil_parse(j[0]))
                data = self.format_data(j[1])
                if timestamp.year == yesterday.year and \
                   timestamp.month == yesterday.month and \
                   timestamp.day == yesterday.day:
                    formatted = self._format_list(data, timestamp, d_type, mkt)
                    formatted_list.append(formatted)
        return formatted_list

    def _format_general(self, data, d_type, mkt):
        formatted_list = []
        for i in data['series']:
            for j in i['data']:
                timestamp = self.utcify(dateutil_parse(j[0]))
                data = self.format_data(j[1])
                formatted = self._format_list(data, timestamp, d_type, mkt)
                formatted_list.append(formatted)
        return formatted_list

    def _format_start_end(self, data):
        formatted_sliced = []
        if 'gen' not in self.options['data']:
            formatted_sliced = [i for i in data if i['timestamp'] >= self.options['start_at'] and i['timestamp'] <= self.options['end_at']]
        else:
            try:
                yesterday = (self.local_now() - timedelta(days=2)).replace(hour=0, minute=0,
                                                                           second=0, microsecond=0)
                tomorrow = (self.local_now() + timedelta(days=1)).replace(hour=23, minute=0,
                                                                          second=0, microsecond=0)
                assert ((self.options['start_at'] >= yesterday) and (self.options['end_at'] <= tomorrow))
                formatted_sliced = [i for i in data if i['timestamp'] >= self.options['start_at'] and i['timestamp'] <= self.options['end_at']]
            except:
                LOGGER.error('Generation data error for %s' % self.BA)
                raise ValueError('Generation data is available for the \
                                 previous and current day.', self.options)
        return formatted_sliced

    def format_result(self, data):
        """Output EIA API results in pyiso format"""
        try:
            assert('series' in data)
        except:
            LOGGER.error('Unable to format result for %s' % data['request'])
            raise ValueError('Query error for %s:' % data['request'])
        market = self._set_market()
        data_type = self._set_data_type()
        data_formatted = []
        if self.options['latest']:
            data_formatted = self._format_latest(data, data_type, market)
        elif self.options['yesterday']:
            data_formatted = self._format_yesterday(data, data_type, market)
        else:
            data_formatted = self._format_general(data, data_type, market)

        if self.options['start_at'] and self.options['end_at']:
            data_formatted = self._format_start_end(data_formatted)
        if self.options['data'] == 'gen':
            data_formatted = self.add_gen_data(data_formatted)
        return data_formatted
