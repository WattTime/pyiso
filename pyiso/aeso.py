import warnings
from datetime import datetime, timedelta
from io import BytesIO

import pytz
from pandas import DataFrame
from pandas import read_csv

from pyiso import LOGGER
from pyiso.base import BaseClient


class AESOClient(BaseClient):
    """
    The Alberta Electricity System Operator (AESO) operates a single control area for Alberta, Canada.
    """
    NAME = 'AESO'
    REPORT_URL_BASE = 'http://ets.aeso.ca/ets_web/ip/Market/Reports'
    LATEST_REPORT_URL = REPORT_URL_BASE + '/CSDReportServlet?contentType=csv'

    fuels = {
        'COAL': 'coal',
        'GAS': 'natgas',
        'HYDRO': 'hydro',
        'OTHER': 'other',
        'WIND': 'wind'
    }

    TZ_NAME = 'Canada/Mountain'

    def __init__(self):
        super(AESOClient, self).__init__()
        self.mtn_tz = pytz.timezone(self.TZ_NAME)

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        if latest:
            return self._get_latest_report(request_type=ParserFormat.generation)
        else:
            warnings.warn(message='The AESO client only supports latest=True for retrieving generation fuel mix data.',
                          category=UserWarning)
            return None

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        if latest:
            return self._get_latest_report(request_type=ParserFormat.trade)
        else:
            warnings.warn(message='The AESO client only supports latest=True for retrieving net export data.',
                          category=UserWarning)
            return None

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        super(AESOClient, self).handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at,
                                               **kwargs)

        if latest:
            return self._get_latest_report(request_type=ParserFormat.load)
        elif self.options.get('start_at', None) and self.options.get('end_at', None):
            derp = pytz.timezone(self.TZ_NAME)
            earliest_load_dt = self.mtn_tz.localize(datetime(year=2000, month=1, day=1, hour=0, minute=0, second=0))

            latest_load_dt = self.local_now().replace(hour=23, minute=59, second=59, microsecond=999999)
            start_at = max(self.options['start_at'], earliest_load_dt).astimezone(self.mtn_tz)
            end_at = min(self.options['end_at'], latest_load_dt).astimezone(self.mtn_tz)
            return self._get_load_for_date_range(start_at=start_at, end_at=end_at)
        else:
            LOGGER.warn('No valid options were supplied.')

    def get_lmp(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def _append_load(self, result_ts, tz_aware_dt, load_mw):
        """
        Appends a dict to the results list, with the keys [ba_name, timestamp, freq, market, load_MW]. Timestamps are
        in UTC.
        :param list result_ts: The timeseries (a list of dicts) which results should be appended to.
        :param datetime tz_aware_dt: A timezone-aware datetime for the data being appended.
        :param float load_mw: Electricity load in megawatts (MW).
        """
        result_ts.append({
            'ba_name': self.NAME,
            'timestamp': tz_aware_dt.astimezone(pytz.utc),
            'freq': self.FREQUENCY_CHOICES.fivemin,
            'market': self.MARKET_CHOICES.fivemin,
            'load_MW': load_mw
        })

    def _get_latest_report(self, request_type):
        response = self.request(url=self.LATEST_REPORT_URL)
        response_body = BytesIO(response.content)
        response_df = read_csv(response_body, names=['label', 'col1', 'col2', 'col3'], skiprows=1)
        if request_type == ParserFormat.generation:
            return self._parse_generation(latest_df=response_df)
        elif request_type == ParserFormat.trade:
            return self._parse_trade(latest_df=response_df)
        elif request_type == ParserFormat.load:
            return self._parse_load(latest_df=response_df)
        else:
            raise RuntimeError('Unknown request type: ' + request_type)

    def _get_load_for_date_range(self, start_at, end_at):
        load_ts = list([])
        af_base_url = self.REPORT_URL_BASE + '/ActualForecastWMRQHReportServlet?contentType=csv'
        begin_param_fmt = '&beginDate=%m%d%Y'
        end_param_fmt = '&endDate=%m%d%Y'

        iter_date = start_at.date()
        while iter_date <= end_at.date():
            # Report lower bound must be at least one day in the past to request current day.
            lower_bound = iter_date if iter_date != self.local_now().date() else iter_date - timedelta(days=1)
            # Report request upper bound is not inclusive; add one day. Max report time range is 31 days. Pick one.
            upper_bound = min(end_at.date() + timedelta(days=1), lower_bound + timedelta(days=31))
            af_url = af_base_url + lower_bound.strftime(begin_param_fmt) + upper_bound.strftime(end_param_fmt)
            response = self.request(url=af_url)
            response_body = BytesIO(response.content)
            response_df = read_csv(response_body, skiprows=4)
            for idx, row in response_df.iterrows():
                hour_ending = int(row['Date'][-2:])
                hour_str = str(hour_ending - 1).zfill(2)
                day_str = row['Date'][:-3]
                row_local_dt = self.mtn_tz.localize(datetime.strptime(day_str + ' ' + hour_str, '%m/%d/%Y %H'))
                load_mw = None
                if row['Actual AIL'] != '-':
                    load_mw = float(row['Actual AIL'].replace(',', ''))
                elif row['Day-Ahead Forecasted AIL'] != '-':
                    load_mw = float(row['Day-Ahead Forecasted AIL'].replace(',', ''))

                if load_mw and start_at <= row_local_dt <= end_at and row_local_dt.date() >= iter_date:
                    self._append_load(result_ts=load_ts, tz_aware_dt=row_local_dt, load_mw=load_mw)
            iter_date = upper_bound
        return load_ts

    def _parse_generation(self, latest_df):
        """
        Parse fuel mix of electricity generation data from the latest AESO electricity market report.
        :param DataFrame latest_df: The latest electricity market report, parsed as a dataframe.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, fuel_name, gen_MW]``.
           Timestamps are in UTC.
        :rtype: list
        """
        local_dt = self._parse_local_time_from_latest_report(latest_df=latest_df)
        fuels_df = latest_df.loc[latest_df['label'].isin(self.fuels.keys())]
        fuels_df.rename(columns={'col1': 'mc', 'col2': 'tng', 'col3': 'dcr'}, inplace=True)

        generation_df = list([])
        for idx, row in fuels_df.iterrows():
            generation_df.append({
                'ba_name': self.NAME,
                'timestamp': self.utcify(local_dt),
                'freq': self.FREQUENCY_CHOICES.fivemin,
                'market': self.MARKET_CHOICES.fivemin,
                'fuel_name': row.label,
                'gen_MW': float(row.tng)
            })

        return generation_df

    def _parse_trade(self, latest_df):
        """
        Parse net export data from the latest AESO electricity market report.
        :param DataFrame latest_df: The latest electricity market report, parsed as a dataframe.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, net_exp_MW]``.
           Timestamps are in UTC.
        :rtype: list
        """
        local_dt = self._parse_local_time_from_latest_report(latest_df=latest_df)
        interchange_df = latest_df.loc[latest_df['label'] == 'Net Actual Interchange']
        net_actual_interchange = float(interchange_df.iloc[0, 1])

        load_df = [{
            'ba_name': self.NAME,
            'timestamp': self.utcify(local_dt),
            'freq': self.FREQUENCY_CHOICES.fivemin,
            'market': self.MARKET_CHOICES.fivemin,
            'net_exp_MW': net_actual_interchange
        }]
        return load_df

    def _parse_load(self, latest_df):
        """
        Parse load data from the latest AESO electricity market report.
        :param DataFrame latest_df: The latest electricity market report, parsed as a dataframe.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, load_MW]``.
           Timestamps are in UTC.
        :rtype: list
        """
        local_dt = self._parse_local_time_from_latest_report(latest_df=latest_df)
        ail_df = latest_df.loc[latest_df['label'] == 'Alberta Internal Load (AIL)']
        alberta_internal_load = float(ail_df.iloc[0, 1])

        load_df = list([])
        self._append_load(result_ts=load_df, tz_aware_dt=local_dt, load_mw=alberta_internal_load)
        return load_df

    def _parse_local_time_from_latest_report(self, latest_df):
        """
        :param DataFrame latest_df: The latest electricity market report, parsed as a dataframe.
        :return: The timezone-aware local datetime that the latest electricity market report was published.
        :rtype datetime
        """
        last_update_prefix = 'Last Update : '
        for lbl in latest_df.label:
            if lbl.startswith(last_update_prefix):
                local_date_str = lbl.lstrip(last_update_prefix)
                break
        local_dt = self.mtn_tz.localize(datetime.strptime(local_date_str, '%b %d, %Y %H:%M'))
        return local_dt


class ParserFormat:
    generation = 'generation'
    load = 'load'
    trade = 'trade'
    lmp = 'lmp'