import re
import pytz
from datetime import timedelta, datetime
from bs4 import BeautifulSoup
from pyiso import LOGGER
from pyiso.base import BaseClient


class YukonEnergyClient(BaseClient):
    """
    Yukon Energy (Canada) publishes a "Current Energy Consumption" page at
    http://www.yukonenergy.ca/energy-in-yukon/electricity-101/current-energy-consumption which can be parsed for
    historical generation mix data up to 24 hours into the past.

    Furthermore, Yukon Energy operates an isolated grid. The imports/exports will always be zero and there is no
    additional external load.
    """

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
        self.hourly_regex_pattern = self._compile_hourly_regex_pattern()

    def handle_options(self, **kwargs):
        super(YukonEnergyClient, self).handle_options(**kwargs)

        start_of_hour = self.yukon_now.replace(minute=0, second=0, microsecond=0)
        self.options['earliest_data_at'] = start_of_hour - timedelta(hours=24)
        self.options['latest_data_at'] = start_of_hour

        if self.options.get('latest', False) and self.options['data'] == 'trade':
            self.options['start_at'] = start_of_hour
            self.options['end_at'] = start_of_hour

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, data='gen')
        genmix = []
        if latest:
            self._generation_latest(genmix)
        elif self._is_valid_date_range():
            self._hourly_range(genmix)
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
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, data='load')
        loads = []
        if latest:
            self._load_latest(loads)
        elif self._is_valid_date_range():
            self._hourly_range(loads)
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
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, data='trade')
        # http://yukonenergy.ca/energy-in-yukon/electricity-101/electricity-library/whats-an-isolated-grid-and-what-does-that-mean-for-me
        LOGGER.warn('Yukon Energy is an isolated grid. Trade will always be zero.')
        trades = []
        hourly_rounded_dt = self.options.get('start_at').replace(minute=0, second=0, microsecond=0)
        while hourly_rounded_dt <= self.options.get('end_at'):
            if self.options['start_at'] <= hourly_rounded_dt <= self.options['end_at']:
                trades.append({
                    'ba_name': self.NAME,
                    'timestamp': hourly_rounded_dt,
                    'freq': self.FREQUENCY_CHOICES.hourly,
                    'market': self.MARKET_CHOICES.hourly,
                    'net_exp_MW': 0
                })
            hourly_rounded_dt = hourly_rounded_dt + timedelta(hours=1)
        return trades

    @staticmethod
    def _compile_hourly_regex_pattern():
        """
        Hourly data is javascript content generated server-side. Javascript array elements are matched using regex.
        The pattern matches strings similar to: '12:00 AM',41.42,0,0
        :return: Compiled regex pattern matching each hourly javascript array element.
        :rtype: sre_parse.Pattern
        """
        pattern = '(\'\\d{1,2}:\\d{2} [AP]M\',\\d{1,3}\\.?\\d{0,2},\\d{1,3}\\.?\\d{0,2},\\d{1,3}\\.?\\d{0,2})+'
        return re.compile(pattern)

    def _append_generation(self, result_ts, fuel, tz_aware_dt, gen_mw):
        """
        Appends a dict to the results list, with the keys [ba_name, timestamp, freq, market, fuel_name, gen_MW].
        Timestamps are in UTC.
        :param list result_ts: The timeseries (a list of dicts) which results should be appended to.
        :param str fuel: WattTime fuel name.
        :param datetime tz_aware_dt: The datetime of the data being appended (timezone-aware).
        :param float gen_mw: Electricity generation in megawatts (MW)
        """
        freq = self.FREQUENCY_CHOICES.tenmin if self.options.get('latest', False) else self.FREQUENCY_CHOICES.hourly
        market = self.MARKET_CHOICES.tenmin if self.options.get('latest', False) else self.MARKET_CHOICES.hourly
        result_ts.append({
            'ba_name': self.NAME,
            'timestamp': tz_aware_dt.astimezone(pytz.utc),
            'freq': freq,
            'market': market,
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
        freq = self.FREQUENCY_CHOICES.tenmin if self.options.get('latest', False) else self.FREQUENCY_CHOICES.hourly
        market = self.MARKET_CHOICES.tenmin if self.options.get('latest', False) else self.MARKET_CHOICES.hourly
        result_ts.append({
            'ba_name': self.NAME,
            'timestamp': tz_aware_dt.astimezone(pytz.utc),
            'freq': freq,
            'market': market,
            'load_MW': load_mw
        })

    def _datetime_from_chart_soup(self, chart_soup):
        """
        :param BeautifulSoup chart_soup: HTML tag soup from charts in the in Current Energy pages of the website.
        :return: A timezone-aware local datetime indicating when the report was generated.
        :rtype: datetime
        """
        time_element = chart_soup.find(name='div', attrs={'class': 'current_time'})
        date_element = chart_soup.find(name='div', attrs={'class': 'current_date'})
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
            latest_dt = self._datetime_from_chart_soup(current_soup)

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

    def _hourly_range(self, results):
        """
        Requests historical generation mix data and appends results in pyiso format to the provided list for those
        results which fall between start_at and end_at time range.
        :param list results: A list to append pyiso-formatted results to.
        """
        response = self.request(url=self.hourly_url)
        if response and response.content:
            hourly_soup = BeautifulSoup(response.content, 'html.parser')
            javascript_elements = hourly_soup.find_all('script', attrs={'type': 'text/javascript'})
            hourly_js_content = javascript_elements[1]
            report_dt = self._datetime_from_chart_soup(hourly_soup)

            # First observation in hourly data always starts 25 hours before the report timestamp.
            row_dt = report_dt - timedelta(hours=25)
            js_hourly_items = self.hourly_regex_pattern.findall(hourly_js_content.string)
            for js_hourly_item in js_hourly_items:
                # js_hourly_item is in the format: '12:00 AM',41.42,0,0
                # The elements are: time, hydro, thermal, available hydro
                js_hr_elements = js_hourly_item.split(',')
                hr_time = js_hr_elements[0].replace('\'', '')
                hr_hydro = float(js_hr_elements[1])
                hr_thermal = float(js_hr_elements[2])
                hr_load = hr_hydro + hr_thermal

                # If row_dt time doesn't match hr_time, raise and exception so we don't misreport data.
                if hr_time.zfill(8) == row_dt.strftime('%I:%M %p'):
                    if self.options['start_at'] <= row_dt <= self.options['end_at']:
                        if self.options['data'] == 'gen':
                            self._append_generation(result_ts=results, fuel='hydro', tz_aware_dt=row_dt,
                                                    gen_mw=hr_hydro)
                            self._append_generation(result_ts=results, fuel='thermal', tz_aware_dt=row_dt,
                                                    gen_mw=hr_thermal)
                        elif self.options['data'] == 'load':
                            self._append_load(result_ts=results, tz_aware_dt=row_dt, load_mw=hr_load)
                    row_dt = row_dt + timedelta(hours=1)
                else:
                    raise RuntimeError('Yukon hourly report expected to return hour %s but was %s.' %
                                       (row_dt.strftime('%I:%M %p'), hr_time.zfill(8)))

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

    def _load_latest(self, loads):
        """
        Requests the latest electricity loads and appends results in pyiso format to the provided list.
        :param list loads: The pyiso results list to append results to.
        """
        response = self.request(url=self.current_url)
        if response and response.content:
            current_soup = BeautifulSoup(response.content, 'html.parser')
            latest_dt = self._datetime_from_chart_soup(current_soup)
            total_load_element = current_soup.find(name='div', attrs={'class': 'total_load'})
            if total_load_element and total_load_element.span:
                load_str = total_load_element.span.string.replace(' MW (megawatt)', '')
                self._append_load(result_ts=loads, tz_aware_dt=latest_dt, load_mw=float(load_str))
