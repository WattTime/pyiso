from datetime import timedelta, datetime

import pytz
from bs4 import BeautifulSoup

from pyiso import LOGGER
from pyiso.base import BaseClient


class YukonEnergyClient(BaseClient):
    NAME = 'YUKON'
    TZ_NAME = 'Canada/Yukon'

    # Their 'thermal' fuel represents both diesel and natural gas:
    # http://yukonenergy.ca/energy-in-yukon/electricity-101/quick-facts
    fuels = {
        'hydro': 'hydro',
        'thermal': 'thermal'
    }

    def __init__(self):
        super(YukonEnergyClient, self).__init__()
        self.yukon_tz = pytz.timezone(self.TZ_NAME)
        self.yukon_now = self.local_now()
        self.base_url = 'http://www.yukonenergy.ca/consumption/'
        self.current_url = self.base_url + 'chart_current.php?chart=current'
        self.hourly_url = self.base_url + 'chart.php?chart=hourly'

    def handle_options(self, **kwargs):
        super(YukonEnergyClient, self).handle_options(**kwargs)

        start_of_hour = self.yukon_now.replace(minute=0, second=0, microsecond=0)
        self.options['earliest_data_at'] = start_of_hour - timedelta(hours=24)
        self.options['latest_data_at'] = start_of_hour

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, data='gen', **kwargs)
        genmix = []
        if latest:
            self._generation_latest(genmix)
        elif self._is_valid_date_range():
            self._generation_range(genmix)
        else:
            if self.options.get('forecast', False):
                LOGGER.warn(self.NAME + ': Generation mix forecasts are not supported.')
            else:
                msg = '%s: Requested date range %s to %s is outside range of available data from %s to %s.' % \
                      (self.NAME, self.options.get('start_at', None), self.options.get('end_at', None),
                       self.options.get('earliest_data_at', None), self.options.get('latest_data_at', None))
                LOGGER.warn(msg)
        return genmix

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, data='load', **kwargs)
        loads = []
        if latest:
            self._load_latest(loads)
        elif self._is_valid_date_range():
            self._load_range(loads)
        else:
            if self.options.get('forecast', False):
                LOGGER.warn(self.NAME + ': Load forecasts are not supported.')
            else:
                msg = '%s: Requested date range %s to %s is outside range of available data from %s to %s.' % \
                      (self.NAME, self.options.get('start_at', None), self.options.get('end_at', None),
                       self.options.get('earliest_data_at', None), self.options.get('latest_data_at', None))
                LOGGER.warn(msg)
        return loads

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_lmp(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def _append_generation(self, result_ts, fuel, tz_aware_dt, gen_mw):
        """
        Appends a dict to the results list, with the keys [ba_name, timestamp, freq, market, fuel_name, gen_MW].
        Timestamps are in UTC.
        :param list result_ts: The timeseries (a list of dicts) which results should be appended to.
        :param str fuel: WattTime fuel name.
        :param datetime tz_aware_dt: The datetime of the data being appended (timezone-aware).
        :param float gen_mw: Electricity generation in megawatts (MW)
        """
        result_ts.append({
            'ba_name': self.NAME,
            'timestamp': tz_aware_dt.astimezone(pytz.utc),
            'freq': self.FREQUENCY_CHOICES.hourly,
            'market': self.MARKET_CHOICES.hourly,
            'fuel_name': fuel,
            'gen_MW': gen_mw
        })

    def _append_load(self, result_ts, tz_aware_dt, load_mw):
        """
        Appends a dict to the results list, with the keys [ba_name, timestamp, freq, market, load_MW].
        Timestamps are in UTC.
        :param list result_ts: The timeseries (a list of dicts) which results should be appended to.
        :param datetime tz_aware_dt: The datetime of the data being appended (timezone-aware).
        :param float load_mw: Electricity load in megawatts (MW)
        """
        result_ts.append({
            'ba_name': self.NAME,
            'timestamp': tz_aware_dt.astimezone(pytz.utc),
            'freq': self.FREQUENCY_CHOICES.hourly,
            'market': self.MARKET_CHOICES.hourly,
            'load_MW': load_mw
        })

    def _datetime_from_current_chart(self, current_soup):
        """
        :param BeautifulSoup current_soup: The Current Energy Consumption chart's HTML content.
        :return: A timezone-aware local datetime indicating when the report was generated.
        :rtype: datetime
        """
        time_element = current_soup.find(name='div', attrs={'class': 'current_time'})
        date_element = current_soup.find(name='div', attrs={'class': 'current_date'})
        dt_format = '%A, %B %d, %Y %I:%M %p'
        concat_timestamp = date_element.string + ' ' + time_element.string
        local_report_dt = self.yukon_tz.localize(datetime.strptime(concat_timestamp, dt_format))
        return local_report_dt

    def _generation_latest(self, genmix):
        """
        Requests the latest electricity generation mix data and appends results in pyiso format to the provided list.
        :param list genmix: The pyiso results list to append results to.
        """
        response = self.request(url=self.current_url)
        if response and response.content:
            current_soup = BeautifulSoup(response.content, 'html.parser')
            latest_dt = self._datetime_from_current_chart(current_soup)

            # hydro generation
            hydro_legend_element = current_soup.find(name='div', attrs={'class': 'chart_legend load_hydro'})
            if hydro_legend_element and hydro_legend_element.div:
                hydro_str = hydro_legend_element.div.string.replace(' MW - hydro', '')
                self._append_generation(result_ts=genmix, fuel='hydro', tz_aware_dt=latest_dt,
                                        gen_mw=float(hydro_str))
            else:  # missing element means zero
                self._append_generation(result_ts=genmix, fuel='hydro', tz_aware_dt=latest_dt, gen_mw=0)

            # thermal generation
            thermal_legend_element = current_soup.find(name='div', attrs={'class': 'chart_legend load_thermal'})
            if thermal_legend_element and thermal_legend_element.div:
                thermal_str = thermal_legend_element.div.string.replace(' MW - thermal', '')
                self._append_generation(result_ts=genmix, fuel='thermal', tz_aware_dt=latest_dt,
                                        gen_mw=float(thermal_str))
            else:  # missing element means zero
                self._append_generation(result_ts=genmix, fuel='thermal', tz_aware_dt=latest_dt, gen_mw=0)

    def _generation_range(self, genmix):
        """
        Requests historical generation mix data and appends results in pyiso format to the provided list for those
        results which fall between start_at and end_at time range.
        :param list genmix: The pyiso results list to append results to.
        """
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

    def _load_range(self, loads):
        """
        Requests historical electricity loads and appends results in pyiso format to the provided list for those
        results which fall between start_at and end_at time range.
        :param list loads: The pyiso results list to append results to.
        """
        pass

    def _load_latest(self, loads):
        """
        Requests the latest electricity loads and appends results in pyiso format to the provided list.
        :param list loads: The pyiso results list to append results to.
        """
        response = self.request(url=self.current_url)
        if response and response.content:
            current_soup = BeautifulSoup(response.content, 'html.parser')
            latest_dt = self._datetime_from_current_chart(current_soup)
            total_load_element = current_soup.find(name='div', attrs={'class': 'total_load'})
            if total_load_element and total_load_element.span:
                load_str = total_load_element.span.string.replace(' MW (megawatt)', '')
                self._append_load(result_ts=loads, tz_aware_dt=latest_dt, load_mw=float(load_str))
