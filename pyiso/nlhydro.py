import pytz

from pyiso.base import BaseClient


class NLHydroClient(BaseClient):
    NAME = 'NLH'
    TZ_NAME = 'Canada/Newfoundland'
    SYSTEM_INFO_URL = 'https://www.nlhydro.com/system-information/system-information-center'

    def __init__(self):
        super(NLHydroClient, self).__init__()
        self.atlantic_tz = pytz.timezone(self.TZ_NAME)
        self.nl_now = self.local_now()

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_lmp(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass
