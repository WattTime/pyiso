import pytz
from pyiso.base import BaseClient


class NSPowerClient(BaseClient):
    NAME = 'NSP'
    TZ_NAME = 'Canada/Atlantic'

    def __init__(self):
        super(NSPowerClient, self).__init__()
        self.ns_tz = pytz.timezone(self.TZ_NAME)

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_lmp(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass
