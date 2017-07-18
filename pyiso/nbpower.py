import warnings

from bs4 import BeautifulSoup
from datetime import datetime
from pyiso.base import BaseClient


class NBPowerClient(BaseClient):
    NAME = 'NBP'
    TZ_NAME = 'Canada/Atlantic'
    LATEST_REPORT_URL = 'http://tso.nbpower.com/Public/en/SystemInformation_realtime.asp'

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        if latest:
            return self._get_latest_report(parser_format=ParserFormat.load)
        else:
            warnings.warn(message='The NBPower client only supports latest=True for retrieving net export data.',
                          category = UserWarning)
            return None

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        if latest:
            return self._get_latest_report(parser_format=ParserFormat.trade)
        else:
            warnings.warn(message='The NBPower client only supports latest=True for retrieving net export data.',
                          category = UserWarning)
            return None

    def get_lmp(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def _get_latest_report(self, parser_format):
        response = self.request(url=self.LATEST_REPORT_URL)
        report_soup = BeautifulSoup(response.content, 'html.parser')
        report_dt = self._parse_date_from_report(report_soup=report_soup)

        if parser_format == ParserFormat.load:
            return self._parse_latest_load(report_soup=report_soup, report_dt=report_dt)
        elif parser_format == ParserFormat.trade:
            return self._parse_latest_trade(report_soup=report_soup, report_dt=report_dt)

    @staticmethod
    def _parse_date_from_report(report_soup):
        """
        :param BeautifulSoup report_soup:
        :return: Local datetime parsed from the report.
        :rtype: datetime
        """
        i_element = report_soup.find(name='i')
        timestamp = i_element.string.split(' Atlantic Time.')[0]
        local_dt = datetime.strptime(timestamp, '%b %d, %Y %H:%M:%S')
        return local_dt

    def _parse_latest_load(self, report_soup, report_dt):
        """
        :param BeautifulSoup report_soup:
        :param datetime report_dt:
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, load_MW]``.
           Timestamps are in UTC.
        :rtype: list
        """
        # Only td elements aligned center with no bgcolor contain electricity data
        nbload_td = report_soup.find(name='td', attrs={'align': 'center', 'bgcolor': ''})
        nbload = float(nbload_td.string)
        load_ts = [{
            'ba_name': self.NAME,
            'timestamp': self.utcify(report_dt),
            'freq': self.FREQUENCY_CHOICES.fivemin,
            'market': self.MARKET_CHOICES.fivemin,
            'load_MW': nbload
        }]
        return load_ts

    def _parse_latest_trade(self, report_soup, report_dt):
        """
        :param BeautifulSoup report_soup:
        :param datetime report_dt:
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, net_exp_MW]``.
           Timestamps are in UTC.
        :rtype: list
        """
        # Only td elements aligned center with no bgcolor contain electricity data
        nbtrade_tds = report_soup.find_all(name='td', attrs={'align': 'center', 'bgcolor': ''})

        # Size intertie nodes are elements 2-7
        nbtrade = 0
        i = 0
        for td in nbtrade_tds:
            if 2 <= i <= 7:
                nbtrade += float(td.string)
            i += 1

        load_ts = [{
            'ba_name': self.NAME,
            'timestamp': self.utcify(report_dt),
            'freq': self.FREQUENCY_CHOICES.fivemin,
            'market': self.MARKET_CHOICES.fivemin,
            'net_exp_MW': nbtrade
        }]
        return load_ts


class ParserFormat(object):
    generation = 'generation'
    load = 'load'
    trade = 'trade'
    lmp = 'lmp'
