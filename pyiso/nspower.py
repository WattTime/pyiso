import pandas
import pytz
from datetime import timedelta, datetime
from pyiso import LOGGER
from pyiso.base import BaseClient


class NSPowerClient(BaseClient):
    """
    Nova Scotia Power (Canada) has a public, informational page titled "Today's Power" which is backed by several
    useful JSON data sources.

    https://www.nspower.ca/en/home/about-us/todayspower.aspx

    This client provides:
        * historical loads up to 24 hours in the past and load forecasts up to 24 hours into the future.
        * historical generation mix up to 24 hours in the past through to the start of the current hour.
    """
    NAME = 'NSP'
    TZ_NAME = 'Canada/Atlantic'

    # LM6000s are combined cycle gas turbines. I don't know if the value being listed separately represents just the
    # condensing steam generator (i.e. Tuft Cove 6) or the entire combined cycle system of two natural gas generators
    # (Tuft Cove 4 & 5) plus the condensing steam generator (Tuft Cove 6).
    # See http://www.novascotia.ca/nse/ea/tuftscove6/NSPI-TuftsCove6-Registration.pdf
    fuels = {
        'Solid Fuel': 'coal',
        'HFO/Natural Gas': 'dual',  # Heavy Fuel Oil/Natural Gas, indicating that either fuel could be used.
        'CT\'s': 'oil',  # CTs are diesel (oil) combustion turbines (I think).
        'LM 6000\'s': 'ccgt',  # LM6000s are combined cycle gas turbines.
        'Biomass': 'biomass',
        'Hydro': 'hydro',
        'Wind': 'wind',
        'Imports': 'other'
    }

    def __init__(self):
        super(NSPowerClient, self).__init__()
        self.atlantic_tz = pytz.timezone(self.TZ_NAME)
        self.base_url = 'http://www.nspower.ca/system_report/today/'
        self.ns_now = self.local_now()

    def handle_options(self, **kwargs):
        super(NSPowerClient, self).handle_options(**kwargs)

        start_of_hour = self.ns_now.replace(minute=0, second=0, microsecond=0)
        self.options['earliest_data_at'] = start_of_hour - timedelta(hours=24)
        if self.options.get('data', None) == 'load':
            self.options['latest_data_at'] = start_of_hour + timedelta(hours=24)
        else:
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
            if self.options['start_at'] < self.ns_now:
                self._load_range(loads)
            if self.options.get('forecast', False):
                self._load_forecast(loads)
        else:
            msg = '%s: Requested date range %s to %s is outside range of available data from %s to %s.' % \
                  (self.NAME, self.options.get('start_at', None), self.options.get('end_at', None),
                   self.options.get('earliest_data_at', None), self.options.get('latest_data_at', None))
            LOGGER.warn(msg)
        return loads

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        # The data from currentmix.json contains 'Imports' but exports are not reported anywhere.
        # There's not enough information to derive trade as specified by the pyiso BaseClient.
        pass

    def _is_valid_date_range(self):
        """
        Checks whether the start_at and end_at options provided are within the date boundaries for the given
        pyiso request type. Assumes that self.handle_options(...) has already been called.
        :return: True/False indicating whether to requested date range is valid.
        :rtype: bool
        """
        if self.options['start_at'] > self.options['latest_data_at'] or \
           self.options['end_at'] < self.options['earliest_data_at']:
            return False
        else:
            return True

    def _current_mix_dataframe(self):
        """
        Requests the "current mix" data from Nova Scotia Power's public website and returns the data in a pandas
        DataFrame.
        :return: A pandas DataFrame indexed by datetime, fuel columns, and generation values in MW.
        :rtype: pandas.DataFrame
        """
        generation_url = self.base_url + 'currentmix.json'
        response = self.request(url=generation_url)
        if response and response.content:
            json = response.content.decode('utf-8')
            currentmix_df = pandas.read_json(json)
            currentmix_df['datetime'] = self._json_serialized_dates_to_timestamps(currentmix_df['datetime'])
            currentmix_df.set_index('datetime', inplace=True, drop=True)
            return currentmix_df
        else:
            return pandas.DataFrame()

    def _current_load_dataframe(self):
        """
        Requests the "current load" data from Nova Scotia Power's public website and returns the data in a pandas
        DataFrame.
        :return: A pandas DataFrame indexed by datetime with load values in MW.
        :rtype: pandas.DataFrame
        """
        currentload_url = self.base_url + 'currentload.json'
        response = self.request(url=currentload_url)
        if response and response.content:
            json = response.content.decode('utf-8')
            currentload_df = pandas.read_json(json)
            currentload_df.drop(currentload_df.head(1).index, inplace=True)  # First row is always 0; drop it.
            currentload_df['datetime'] = self._json_serialized_dates_to_timestamps(currentload_df['datetime'])
            currentload_df.set_index('datetime', inplace=True, drop=True)
            return currentload_df
        else:
            return pandas.DataFrame()

    def _generation_latest(self, genmix):
        """
        Requests the latest electricity generation mix data and appends results in pyiso format to the provided list.
        :param list genmix: The pyiso results list to append results to.
        """
        currentmix_df = self._current_mix_dataframe()
        if len(currentmix_df) > 0:
            latest_fuel_outputs = currentmix_df.iloc[len(currentmix_df) - 1]
            latest_dt = latest_fuel_outputs.name.to_pydatetime()
            for index, val in latest_fuel_outputs.iteritems():
                self._append_generation(result_ts=genmix, fuel=index, tz_aware_dt=latest_dt, gen_mw=val)

    def _generation_range(self, genmix):
        """
        Requests historical generation mix data and appends results in pyiso format to the provided list for those
        results which fall between start_at and end_at time range.
        :param list genmix: The pyiso results list to append results to.
        """
        currentmix_df = self._current_mix_dataframe()
        if len(currentmix_df) > 0:
            stacked = currentmix_df.stack()
            for index, row in stacked.iteritems():
                row_dt = index[0].to_pydatetime()
                if self.options['start_at'] <= row_dt <= self.options['end_at']:
                    self._append_generation(result_ts=genmix, fuel=index[1], tz_aware_dt=row_dt, gen_mw=row)

    def _append_generation(self, result_ts, fuel, tz_aware_dt, gen_mw):
        """
        Appends a dict to the results list, with the keys [ba_name, timestamp, freq, market, fuel_name, gen_MW].
        Timestamps are in UTC.
        :param list result_ts: The timeseries (a list of dicts) which results should be appended to.
        :param str fuel: NSP fuel name (will be converted to WattTime name).
        :param datetime tz_aware_dt: The datetime of the data being appended (timezone-aware).
        :param float gen_mw: Electricity generation in megawatts (MW)
        """
        result_ts.append({
            'ba_name': self.NAME,
            'timestamp': tz_aware_dt.astimezone(pytz.utc),
            'freq': self.FREQUENCY_CHOICES.hourly,
            'market': self.MARKET_CHOICES.hourly,
            'fuel_name': self.fuels[fuel],
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

    def _forecast_load_dataframe(self):
        """
        Requests the "forecast" load data from Nova Scotia Power's public website and returns the data in a pandas
        DataFrame.
        :return: A pandas DataFrame indexed by datetime with load values in MW.
        :rtype: pandas.DataFrame
        """
        load_forecast_url = self.base_url + 'forecast.json'
        response = self.request(url=load_forecast_url)
        if response and response.content:
            json = response.content.decode('utf-8')
            forecastload_df = pandas.read_json(json)
            forecastload_df['datetime'] = self._json_serialized_dates_to_timestamps(forecastload_df['datetime'])
            forecastload_df.set_index('datetime', inplace=True, drop=True)
            return forecastload_df
        else:
            return pandas.DataFrame()

    def _json_serialized_dates_to_timestamps(self, serialized_datetimes):
        """
        :param pandas.Series serialized_datetimes: Series of datetimes strings in NSPower's... special... serialization
            format.
        :return: pandas.Series Series of UTC Timestamps.
        """
        ticks = serialized_datetimes.str.replace(r'\D+', '').astype('int')
        return ticks.apply(lambda d: datetime.fromtimestamp(d / 1000, tz=pytz.utc))

    def _load_range(self, loads):
        """
        Requests historical electricity loads and appends results in pyiso format to the provided list for those
        results which fall between start_at and end_at time range.
        :param list loads: The pyiso results list to append results to.
        """
        currentload_df = self._current_load_dataframe()
        if len(currentload_df) > 0:
            for index, row in currentload_df.itertuples():
                row_dt = index.to_pydatetime()
                if self.options['start_at'] <= row_dt <= self.options['end_at']:
                    self._append_load(result_ts=loads, tz_aware_dt=row_dt, load_mw=row)

    def _load_latest(self, loads):
        """
        Requests the latest electricity loads and appends results in pyiso format to the provided list.
        :param list loads: The pyiso results list to append results to.
        """
        currentload_df = self._current_load_dataframe()
        if len(currentload_df) > 0:
            latest_load = currentload_df.tail(1)
            load_mw = latest_load.iloc[0]['Base Load']
            latest_dt = latest_load.iloc[0].name.to_pydatetime()
            self._append_load(result_ts=loads, tz_aware_dt=latest_dt, load_mw=load_mw)

    def _load_forecast(self, loads):
        """
        Requests the electricity demand forecast and appends results in pyiso format to the provided list for those
        results which fall between start_at and end_at time range.
        :param list loads: The pyiso results list to append results to.
        """
        forecastload_df = self._forecast_load_dataframe()
        if len(forecastload_df) > 0:
            for index, row in forecastload_df.itertuples():
                row_dt = index.to_pydatetime()
                # For some reason, historical forecast values are included in the response.
                # They must also be filtered out, in case the start_at/end_at range is a historical+forecast range.
                if self.options['start_at'] <= row_dt <= self.options['end_at'] and self.ns_now < row_dt:
                    self._append_load(result_ts=loads, tz_aware_dt=row_dt, load_mw=row)
