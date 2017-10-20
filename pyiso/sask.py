import json
import warnings
from datetime import datetime

import pytz

from pyiso.base import BaseClient


class SaskPowerClient(BaseClient):
    NAME = 'SASK'
    TZ_NAME = 'Canada/Saskatchewan'
    SYSLOAD_URL = 'http://www.saskpower.com/spfeeds.nsf/feeds/sysloadJSON'

    def __init__(self):
        super(SaskPowerClient, self).__init__()
        self.sask_tz = pytz.timezone(self.TZ_NAME)

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        if latest:
            return self.get_latest_load()
        else:
            warnings.warn(message='SaskPowerClient only supports the latest=True argument for retrieving load data.',
                          category=UserWarning)

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_latest_load(self):
        """
        Requests the "Current System Load" JSON from Sask Power's public data feeds and returns it in pyiso load format.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, load_MW]``.
           Timestamps are in UTC.
        :rtype: list
        """
        response = self.request(self.SYSLOAD_URL)
        sysload_json = json.loads(response.content.decode('utf-8'))
        updated_str = sysload_json.get('updatedTS', None)
        last_updated = self.sask_tz.localize(datetime.strptime(updated_str.upper(), '%Y-%m-%d %I:%M %p'))
        current_sysload = float(sysload_json.get('currentSysLoad', None))
        return [{
            'ba_name': self.NAME,
            'timestamp': last_updated.astimezone(pytz.utc),
            'freq': self.FREQUENCY_CHOICES.fivemin,
            'market': self.MARKET_CHOICES.fivemin,
            'load_MW': current_sysload
        }]

