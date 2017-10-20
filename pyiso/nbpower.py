import warnings
from copy import copy
from datetime import datetime, timedelta
from io import BytesIO

import pytz
from bs4 import BeautifulSoup
from pandas import read_csv

from pyiso import LOGGER
from pyiso.base import BaseClient

try:
    from urllib import quote  # Python 2.X
except ImportError:
    from urllib.parse import quote  # Python 3+


class NBPowerClient(BaseClient):
    NAME = 'NBP'
    TZ_NAME = 'Canada/Atlantic'
    LATEST_REPORT_URL = 'http://tso.nbpower.com/Public/en/SystemInformation_realtime.asp'

    def __init__(self):
        super(NBPowerClient, self).__init__()
        self.atlantic_tz = pytz.timezone(self.TZ_NAME)
        self._local_now = self.local_now()  # So that all functions compare against the same "now".

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, **kwargs)

        if latest:
            return self._get_latest_report(parser_format=ParserFormat.load)
        elif self.local_start_at and self.local_end_at:
            load_ts = self._get_latest_report(parser_format=ParserFormat.load)
            if self.options.get('forecast', False):
                forecast_ts = self._get_load_forecast_report()
                load_ts = load_ts + forecast_ts
            return load_ts
        else:
            warnings.warn(message='NBPower only supports latest=True or valid start_at and end_at parameters for '
                                  'retrieving load data.', category=UserWarning)
            return None

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, **kwargs)
        if latest:
            return self._get_latest_report(parser_format=ParserFormat.trade)
        else:
            warnings.warn(message='NBPower only supports latest=True for retrieving net export data.',
                          category=UserWarning)
            return None

    def handle_options(self, **kwargs):
        super(NBPowerClient, self).handle_options(**kwargs)
        self.local_start_at = self.options['start_at'].astimezone(self.atlantic_tz) \
            if self.options.get('start_at', None) else None
        self.local_end_at = self.options['end_at'].astimezone(self.atlantic_tz) \
            if self.options.get('end_at', None) else None

    def _get_latest_report(self, parser_format):
        response = self.request(url=self.LATEST_REPORT_URL)
        report_soup = BeautifulSoup(response.content, 'html.parser')
        report_dt = self._parse_date_from_latest_report(report_soup=report_soup)

        if self.options.get('latest', False) or self.local_start_at <= report_dt <= self.local_end_at:
            if parser_format == ParserFormat.load:
                return self._parse_latest_load(report_soup=report_soup, report_dt=report_dt)
            elif parser_format == ParserFormat.trade:
                return self._parse_latest_trade(report_soup=report_soup, report_dt=report_dt)
        else:
            return list([])

    def _parse_date_from_latest_report(self, report_soup):
        """
        :param BeautifulSoup report_soup: The Sysytem Information report's HTML content.
        :return: A timezone-aware local datetime indicating when the report was generated.
        :rtype: datetime
        """
        italic_element = report_soup.find(name='i')
        timestamp = italic_element.string.split(' Atlantic Time.')[0]
        local_report_dt = self.atlantic_tz.localize(datetime.strptime(timestamp, '%b %d, %Y %H:%M:%S'))
        return local_report_dt

    def _parse_latest_load(self, report_soup, report_dt):
        """
        :param BeautifulSoup report_soup: The System Information report's HTML content.
        :param datetime report_dt: A timezone-aware datetime indicating when the report was generated.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, load_MW]``.
           Timestamps are in UTC.
        :rtype: list
        """
        # Only td elements aligned center with no bgcolor contain electricity data
        nbload_td = report_soup.find(name='td', attrs={'align': 'center', 'bgcolor': ''})
        nbload = float(nbload_td.string)
        load_ts = [{
            'ba_name': self.NAME,
            'timestamp': report_dt.astimezone(pytz.utc),
            'freq': self.FREQUENCY_CHOICES.fivemin,
            'market': self.MARKET_CHOICES.fivemin,
            'load_MW': nbload
        }]
        return load_ts

    def _parse_latest_trade(self, report_soup, report_dt):
        """
        :param BeautifulSoup report_soup: The System Information report's HTML content.
        :param datetime report_dt: A timezone-aware datetime indicating when the report was generated.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, net_exp_MW]``.
           Timestamps are in UTC.
        :rtype: list
        """
        # Only td elements aligned center with no bgcolor contain electricity data
        table_datas = report_soup.find_all(name='td', attrs={'align': 'center', 'bgcolor': ''})

        # Net Scheduled Interchange nodes are elements 2-7
        nb_trade = 0
        i = 0
        for td in table_datas:
            if 2 <= i <= 7:
                nb_trade += float(td.string)
            i += 1

        trade_ts = [{
            'ba_name': self.NAME,
            'timestamp': report_dt.astimezone(pytz.utc),
            'freq': self.FREQUENCY_CHOICES.fivemin,
            'market': self.MARKET_CHOICES.fivemin,
            'net_exp_MW': nb_trade
        }]
        return trade_ts

    def _get_load_forecast_report(self):
        """
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, load_MW]``.
           Timestamps are in UTC.
        :rtype: list
        """
        load_ts = list([])
        forecast_url_base = 'http://tso.nbpower.com/reports%20%26%20assessments/load%20forecast/hourly/'
        forecast_filename_fmt = '%Y-%m-%d %H.csv'
        earliest_forecast = copy(self._local_now).replace(minute=0, second=0, microsecond=0)
        latest_forecast = earliest_forecast + timedelta(hours=3)

        if self.local_start_at <= latest_forecast:
            forecast_filename = earliest_forecast.strftime(forecast_filename_fmt)
            load_forecast_url = forecast_url_base + quote(forecast_filename)
            response = self.request(load_forecast_url)
            response_body = BytesIO(response.content)
            response_df = read_csv(response_body, names=['timestamp', 'load'], usecols=[0, 1],
                                   dtype={'load': float}, parse_dates=[0], date_parser=self.parse_forecast_timestamps)
            for idx, row in response_df.iterrows():
                if self._local_now <= row.timestamp and self.local_start_at <= row.timestamp <= self.local_end_at:
                    load_ts.append({
                        'ba_name': self.NAME,
                        'timestamp': row.timestamp.to_pydatetime().astimezone(pytz.utc),
                        'freq': self.FREQUENCY_CHOICES.hourly,
                        'market': self.MARKET_CHOICES.dam,
                        'load_MW': row.load
                    })
        else:
            LOGGER.warn('The latest load forecast available is ' + str(latest_forecast) +
                        '. The requested start_at must be before this time.')
        return load_ts

    def parse_forecast_timestamps(self, column_value):
        return self.atlantic_tz.localize(datetime.strptime(column_value, '%Y%m%d%H%M%SAD'))


class ParserFormat(object):
    generation = 'generation'
    load = 'load'
    trade = 'trade'
    lmp = 'lmp'
