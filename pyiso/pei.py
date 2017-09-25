import json
import warnings
import pytz
from datetime import datetime
from pyiso.base import BaseClient


class PEIClient(BaseClient):
    """
    The government of Prince Edward Island, Canada publishes a "Wind Energy" summary page for the island at
    http://www.gov.pe.ca/windenergy/chart.php The latest load and a basic generation mix can be derived from this data.
    """

    NAME = 'PEI'
    TZ_NAME = 'Etc/GMT+4'  # Times are always given in Atlantic Standard Time

    def __init__(self):
        super(PEIClient, self).__init__()
        self.pei_tz = pytz.timezone(self.TZ_NAME)
        self.chart_values_url = 'http://www.gov.pe.ca/windenergy/chart-values.php'

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, **kwargs)
        if latest:
            return self.get_latest_generation()
        else:
            warnings.warn(message='PEIClient only supports the latest=True argument for retrieving generation mix.',
                          category=UserWarning)

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, **kwargs)
        if latest:
            return self.get_latest_load()
        else:
            warnings.warn(message='PEIClient only supports the latest=True argument for retrieving load data.',
                          category=UserWarning)

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_lmp(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_latest_load(self):
        """
        Requests the JSON backing PEI's public "Wind Energy" page (http://www.gov.pe.ca/windenergy/chart.php)
        and returns it in pyiso load format.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, load_MW]``.
            Timestamps are in UTC.
        :rtype: list
        """
        loads = []
        response = self.request(self.chart_values_url)
        if response:
            sysload_list = json.loads(response.content.decode('utf-8'))
            sysload_json = sysload_list[0]
            seconds_epoch = int(sysload_json.get('updateDate', None))
            last_updated = self.pei_tz.localize(datetime.fromtimestamp(seconds_epoch))
            total_on_island_load = float(sysload_json.get('data1', None))
            loads.append({
                'ba_name': self.NAME,
                'timestamp': last_updated.astimezone(pytz.utc),
                'freq': self.FREQUENCY_CHOICES.tenmin,
                'market': self.MARKET_CHOICES.tenmin,
                'load_MW': total_on_island_load
            })
        return loads

    def get_latest_generation(self):
        """
        Requests the JSON backing PEI's public "Wind Energy" page (http://www.gov.pe.ca/windenergy/chart.php)
        and returns it in pyiso generation mix format.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, fuel_name, gen_MW]``.
            Timestamps are in UTC.
        :rtype: list
        """
        loads = []
        response = self.request(self.chart_values_url)
        if response:
            sysload_list = json.loads(response.content.decode('utf-8'))
            sysload_json = sysload_list[0]
            seconds_epoch = int(sysload_json.get('updateDate', None))
            last_updated = self.pei_tz.localize(datetime.fromtimestamp(seconds_epoch))
            total_on_island_load = float(sysload_json.get('data1', None))
            wind_mw = float(sysload_json.get('data2', None))
            other_mw = total_on_island_load - wind_mw
            loads.append({
                'ba_name': self.NAME,
                'timestamp': last_updated.astimezone(pytz.utc),
                'freq': self.FREQUENCY_CHOICES.tenmin,
                'market': self.MARKET_CHOICES.tenmin,
                'fuel_name': 'wind',
                'gen_MW': wind_mw
            })
            loads.append({
                'ba_name': self.NAME,
                'timestamp': last_updated.astimezone(pytz.utc),
                'freq': self.FREQUENCY_CHOICES.tenmin,
                'market': self.MARKET_CHOICES.tenmin,
                'fuel_name': 'other',
                'gen_MW': other_mw
            })
        return loads
