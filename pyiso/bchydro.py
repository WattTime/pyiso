from datetime import timedelta

import pytz

from pyiso import LOGGER
from pyiso.base import BaseClient


class BCHydroClient(BaseClient):
    NAME = 'BCH'
    TZ_NAME = 'Canada/Pacific'

    def __init__(self):
        super(BCHydroClient, self).__init__()
        self.bc_tz = pytz.timezone(self.TZ_NAME)
        self.bc_now = self.local_now()

    def handle_options(self, **kwargs):
        super(BCHydroClient, self).handle_options(**kwargs)
        self.options['earliest_data_at'] = self.bc_now - timedelta(days=9)
        self.options['latest_data_at'] = self.bc_now

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, data='trade')
        trades = []
        if latest:
            self._trade_latest()
        elif self._is_valid_date_range():
            self._trade_historical()
        else:
            if self.options.get('forecast', False):
                LOGGER.warn(self.NAME + ': Trade forecasts are not supported.')
            else:
                msg = '%s: Requested date range %s to %s is outside range of available data from %s to %s.' % \
                      (self.NAME, self.options.get('start_at', None), self.options.get('end_at', None),
                       self.options.get('earliest_data_at', None), self.options.get('latest_data_at', None))
                LOGGER.warn(msg)
        return trades

    def get_lmp(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def _is_valid_date_range(self):
        """
        Checks whether the start_at and end_at options provided lie within the supported date range. Assumes that
        self.handle_options(...) has already been called.
        :return: True/False indicating whether to requested date range is valid.
        :rtype: bool
        """
        if self.options['start_at'] > self.options['latest_data_at'] or \
           self.options['end_at'] < self.options['earliest_data_at']:
            return False
        else:
            return True

    def _trade_historical(self):
        pass

    def _trade_latest(self):
        pass
