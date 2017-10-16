import pytz

from pyiso.base import BaseClient


class BCHydroClient(BaseClient):
    NAME = 'BCH'
    TZ_NAME = 'Canada/Pacific'

    def __init__(self):
        super(BCHydroClient, self).__init__()
        self.bc_tz = pytz.timezone(self.TZ_NAME)
        self.bc_now = self.local_now()

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_lmp(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass