from datetime import timedelta

import pandas
import pytz

from pyiso import LOGGER
from pyiso.base import BaseClient


class BCHydroClient(BaseClient):
    """
    British Columbia Hydro (Canada) has a public transmission flow page available at
    https://www.bchydro.com/bctc/system_cms/actual_flow/data.aspx

    This client provides five-minute, historical trade data up to 9 days in the past through to the current time
    (i.e. most recent five minute interval).
    """
    NAME = 'BCH'
    TZ_NAME = 'Canada/Pacific'

    def __init__(self):
        super(BCHydroClient, self).__init__()
        self.bc_tz = pytz.timezone(self.TZ_NAME)
        self.bc_now = self.local_now()

    def handle_options(self, **kwargs):
        super(BCHydroClient, self).handle_options(**kwargs)
        self.options['earliest_data_at'] = self.bc_now - timedelta(days=9)
        self.options['latest_data_at'] = self.bc_now

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, data='trade')
        trades = []
        if latest:
            self._trade_latest(trades)
        elif self._is_valid_date_range():
            self._trade_historical(trades)
        else:
            if self.options.get('forecast', False):
                LOGGER.warn(self.NAME + ': Trade forecasts are not supported.')
            else:
                msg = '%s: Requested date range %s to %s is outside range of available data from %s to %s.' % \
                      (self.NAME, self.options.get('start_at', None), self.options.get('end_at', None),
                       self.options.get('earliest_data_at', None), self.options.get('latest_data_at', None))
                LOGGER.warn(msg)
        return trades

    def _actual_flow_data(self):
        """
        Requests the "Actual Flow" Excel and processes the response.
        :return: The "Actual Flow" Excel spreadsheet as a pandas timeseries.
        :rtype: pandas.DataFrame
        """
        data = self.fetch_xls('https://www.bchydro.com/bctc/system_cms/actual_flow/data1.xls')
        if data:
            actual_flow_df = data.parse('Sheet1')
            return actual_flow_df
        else:
            return pandas.DataFrame()

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

    def _trade_historical(self, trades):
        """
        Requests historical import/export trade data and appends results in pyiso format to the provided list for those
        results which fall between start_at and end_at time range.
        :param list trades: A list to append pyiso-formatted results to.
        """
        actual_flow_df = self._actual_flow_data()
        for idx, row in actual_flow_df.iterrows():
            local_flow_dt = row['Time'].to_pydatetime()
            local_flow_dt = self.bc_tz.localize(local_flow_dt)
            us_flow = row['BC-US Actual']
            ab_flow = row['BC-AB Actual']
            sum_flow = us_flow + ab_flow
            if self.options['start_at'] <= local_flow_dt <= self.options['end_at']:
                self._append_trade(result_ts=trades, tz_aware_dt=local_flow_dt, net_exp_mw=sum_flow)

    def _trade_latest(self, trades):
        """
        Requests the latest import/export trade data and appends results in pyiso format to the provided list.
        :param list trades: The pyiso results list to append results to.
        """
        actual_flow_df = self._actual_flow_data()
        if len(actual_flow_df) > 0:
            latest_flow = actual_flow_df.tail(1)
            local_flow_dt = latest_flow.iloc[0]['Time'].to_pydatetime()
            local_flow_dt = self.bc_tz.localize(local_flow_dt)
            us_flow = latest_flow.iloc[0]['BC-US Actual']
            ab_flow = latest_flow.iloc[0]['BC-AB Actual']
            sum_flow = us_flow + ab_flow
            self._append_trade(result_ts=trades, tz_aware_dt=local_flow_dt, net_exp_mw=sum_flow)

    def _append_trade(self, result_ts, tz_aware_dt, net_exp_mw):
        """
        Appends a dict to the results list, with the keys [ba_name, timestamp, freq, market, net_exp_MW]. Timestamps
        are in UTC.
        :param list result_ts: The timeseries (a list of dicts) which results should be appended to.
        :param datetime tz_aware_dt: The datetime of the data being appended (timezone-aware).
        :param float net_exp_mw: The net exported megawatts (MW) (i.e. export - import). Negative values indicate that
            more electricity was imported than exported.
        """
        result_ts.append({
            'ba_name': self.NAME,
            'timestamp': tz_aware_dt.astimezone(pytz.utc),
            'freq': self.FREQUENCY_CHOICES.fivemin,
            'market': self.MARKET_CHOICES.fivemin,
            'net_exp_MW': net_exp_mw
        })
