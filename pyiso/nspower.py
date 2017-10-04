import pandas
import pytz
from datetime import timedelta, datetime
from pyiso import LOGGER
from pyiso.base import BaseClient


class NSPowerClient(BaseClient):
    NAME = 'NSP'
    TZ_NAME = 'Canada/Atlantic'

    # LM6000s are combined cycle gas turbines. I don't know if the value being listed seperately represents just the
    # condensing steam generator (i.e. Tuft Cove 6) or the entire combined cycle system of two natural gas generators
    # (Tuft Cove 4 & 5) plus the condensing steam generator (Tuft Cove 6).
    # See http://www.novascotia.ca/nse/ea/tuftscove6/NSPI-TuftsCove6-Registration.pdf
    #
    # CTs are diesel (oil) combustion turbines (I think).
    # See http://www.nspower.ca/en/home/about-us/how-we-make-electricity/thermal-electricity/oil-facilities.aspx
    #
    # "HFO/Natural Gas" is Heavy Fuel Oil/Natural Gas
    fuels = {
        'Solid Fuel': 'coal',
        'HFO/Natural Gas': 'natgas',
        'CT\'s': 'oil',
        'LM 6000\'s': 'thermal',
        'Biomass': 'biomass',
        'Hydro': 'hydro',
        'Wind': 'wind',
        'Imports': 'other'
    }

    def __init__(self):
        super(NSPowerClient, self).__init__()
        self.atlantic_tz = pytz.timezone(self.TZ_NAME)
        self.base_url = 'http://www.nspower.ca/system_report/today/'
        self.ns_now = self.local_now()

    def handle_options(self, **kwargs):
        super(NSPowerClient, self).handle_options(**kwargs)

        start_of_hour = self.ns_now.replace(minute=0, second=0, microsecond=0)
        self.options['earliest_data_at'] = start_of_hour - timedelta(hours=24)
        if self.options.get('data', None) == 'load':
            self.options['latest_data_at'] = start_of_hour + timedelta(hours=24)
        else:
            self.options['latest_data_at'] = start_of_hour

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, data='gen', **kwargs)

        genmix = []
        if not self._is_valid_date_range():
            if self.options.get('forecast', False):
                LOGGER.warn(self.NAME + ': Generation mix forecasts are not supported.')
            else:
                msg = '%s: Requested date range %s to %s is outside range of available data from %s to %s.' % \
                      (self.NAME, self.options.get('start_at', None), self.options.get('end_at', None),
                       self.options.get('earliest_data_at', None), self.options.get('latest_data_at', None))
                LOGGER.warn(msg)
        else:
            self._parse_currentmix(genmix)
        return genmix

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, data='load', **kwargs)
        load_0_24_url = self.base_url + 'currentload.json'
        load_24_48_url = self.base_url + 'forecast.json'

        if not self._is_valid_date_range():
            msg = '%s: Requested date range %s to %s is outside range of available data from %s to %s.' % \
                  (self.NAME, self.options.get('start_at', None), self.options.get('end_at', None),
                   self.options.get('earliest_data_at', None), self.options.get('latest_data_at', None))
            LOGGER.warn(msg)
            return []
        elif latest:
            pass
        else:
            pass

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        # The data from currentmix.json contains 'Imports' but exports are not reported anywhere.
        # There's not enough information to derive trade as specified by the pyiso BaseClient.
        pass

    def get_lmp(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def _is_valid_date_range(self):
        """
        Checks whether the start_at and end_at options provided are within the date boundaries for the given
        pyiso request type. Assumes that self.handle_options(...) has already been called.
        :return: True/False indicating whether to requested date range is valid.
        :rtype: bool
        """
        if self.options['start_at'] > self.options['latest_data_at'] or \
           self.options['end_at'] < self.options['earliest_data_at']:
            return False
        else:
            return True

    def _parse_currentmix(self, genmix):
        """
        :param list genmix:
        """
        generation_url = self.base_url + 'currentmix.json'
        response = self.request(url=generation_url)
        currentmix_df = pandas.read_json(response.content.decode('utf-8'))
        currentmix_df['datetime'] = currentmix_df['datetime'].str.replace(r'\D+', '').astype('int')
        currentmix_df['datetime'] = currentmix_df['datetime'].apply(lambda d: datetime.fromtimestamp(d / 1000,
                                                                                                     tz=pytz.utc))
        currentmix_df.set_index('datetime', inplace=True, drop=True)
        stacked = currentmix_df.stack()
        for index, row in list(stacked.items()):
            row_dt = index[0].to_pydatetime()
            if self.options['start_at'] <= row_dt <= self.options['end_at']:
                self.append_generation(genmix=genmix, fuel_name=self.fuels[index[1]], tzaware_dt=row_dt, gen_mw=row)

    def append_generation(self, genmix, fuel_name, tzaware_dt, gen_mw):
        genmix.append({
            'ba_name': self.NAME,
            'timestamp': tzaware_dt.astimezone(pytz.utc),
            'freq': self.FREQUENCY_CHOICES.hourly,
            'market': self.MARKET_CHOICES.hourly,
            'fuel_name': fuel_name,
            'gen_MW': gen_mw
        })
