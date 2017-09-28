import pytz
from datetime import timedelta

from pyiso import LOGGER
from pyiso.base import BaseClient


class NSPowerClient(BaseClient):
    NAME = 'NSP'
    TZ_NAME = 'Canada/Atlantic'

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
        # LM6000s are gas turbines. CTs are diesel (oil) combustion turbines.
        # See http://www.nspower.ca/en/home/about-us/how-we-make-electricity/thermal-electricity/oil-facilities.aspx
        generation_url = self.base_url + 'currentmix.json'

        if not self._is_valid_date_range():
            if self.options.get('forecast', False):
                LOGGER.warn(self.NAME + ': Generation mix forecasts are not supported.')
            else:
                msg = '%s: Requested date range %s to %s is outside range of available data from %s to %s.' % \
                      (self.NAME, self.options.get('start_at', None), self.options.get('end_at', None),
                       self.options.get('earliest_data_at', None), self.options.get('latest_data_at', None))
                LOGGER.warn(msg)
            return []
        elif latest:
            pass
        else:
            pass

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
