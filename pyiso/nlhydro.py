import re
import warnings
import pytz
from datetime import datetime
from bs4 import BeautifulSoup
from pyiso.base import BaseClient


class NLHydroClient(BaseClient):
    """
    Client for Newfoundland and Labrador Hydro (Canada) which parses latest system load data from
    https://www.nlhydro.com/system-information/system-information-center/
    """

    NAME = 'NLH'
    TZ_NAME = 'Canada/Newfoundland'
    SYSTEM_INFO_URL = 'https://www.nlhydro.com/system-information/system-information-center'

    def __init__(self):
        super(NLHydroClient, self).__init__()
        self.nl_tz = pytz.timezone(self.TZ_NAME)
        self.nl_now = self.local_now()

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at)
        if latest:
            return self.get_latest_load()
        else:
            warnings.warn(message='%s only supports the latest=True argument for retrieving load data.' % self.NAME,
                          category=UserWarning)
            return []

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_latest_load(self):
        """
        Requests the "Current Island System Generation" HTML and returns it in pyiso load format.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, load_MW]``.
           Timestamps are in UTC.
        :rtype: list
        """
        response = self.request(self.SYSTEM_INFO_URL)
        if response and response.content:
            soup = BeautifulSoup(response.content, 'html.parser')
            sysgen_div = soup.find(name='div', attrs={'id': 'sysgen'})
            sysgen_children = sysgen_div.find_all(name='p')
            load_p = sysgen_children[0]
            current_island_gen = float(re.search('\d+', load_p.string).group())
            datetime_ele = sysgen_children[1].find('span').string
            last_updated = self.nl_tz.localize(datetime.strptime(datetime_ele, '%m/%d/%Y %I:%M %p'))

            return [{
                'ba_name': self.NAME,
                'timestamp': last_updated.astimezone(pytz.utc),
                'freq': self.FREQUENCY_CHOICES.hourly,
                'market': self.MARKET_CHOICES.hourly,
                'load_MW': current_island_gen
            }]
        else:
            return []
