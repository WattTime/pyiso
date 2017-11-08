import warnings
from datetime import datetime, timedelta
from io import BytesIO

import pytz
from pandas import DataFrame, read_csv

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
            earliest_load_dt = self.mtn_tz.localize(datetime(year=2000, month=1, day=1, hour=0, minute=0, second=0))
            latest_load_dt = self.local_now().replace(hour=23, minute=59, second=59, microsecond=999999)
            start_at = max(self.options['start_at'], earliest_load_dt).astimezone(self.mtn_tz)
            end_at = min(self.options['end_at'], latest_load_dt).astimezone(self.mtn_tz)
            return self._get_load_for_date_range(start_at=start_at, end_at=end_at)
        else:
            LOGGER.warn('No valid options were supplied.')

    def _get_latest_report(self, request_type):
        """
        Requests the latest AESO Market Report and parses it into one of several pyiso timeseries formats.
        :param str request_type: Indicates the data which should be parsed from the latest report. Valid types are:
           'generation', 'trade', and 'load'. See ParserFormat class.
        :return: A list of dicts, with keys according to the pyiso format for the request type.
        :rtype: list
        """
        response = self.request(url=self.LATEST_REPORT_URL)
        response_body = BytesIO(response.content)
        response_df = read_csv(response_body, names=['label', 'col1', 'col2', 'col3'], skiprows=1)
        if request_type == ParserFormat.generation:
            return self._parse_latest_generation(latest_df=response_df)
        elif request_type == ParserFormat.trade:
            return self._parse_latest_trade(latest_df=response_df)
        elif request_type == ParserFormat.load:
            return self._parse_latest_load(latest_df=response_df)
        else:
            raise RuntimeError('Unknown request type: ' + request_type)

    def _get_load_for_date_range(self, start_at, end_at):
        """
        Request historical/forecast reports for Alberta Internal Load from the "Actual Forecast" Report.
        :param datetime start_at: Timezone-aware local datetime indicating the lower-bound (inclusive) of data returned.
        :param datetime end_at: Timezone-aware local datetime indicating the upper-bound (inclusive) of data returned.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, load_MW]``.
           Timestamps are in UTC.
        :rtype: list
        """
        load_ts = list([])
        af_base_url = self.REPORT_URL_BASE + '/ActualForecastWMRQHReportServlet?contentType=csv'
        begin_param_fmt = '&beginDate=%m%d%Y'
        end_param_fmt = '&endDate=%m%d%Y'

        iter_date = start_at.date() - timedelta(days=1) if self.is_prev_hr_ending_24_reqd(start_at) else start_at.date()
        while iter_date <= end_at.date():
            # Report lower bound must be at least one day in the past to request current day.
            lower_bound = iter_date if iter_date != self.local_now().date() else iter_date - timedelta(days=1)
            # Report request upper bound is not inclusive; add one day. Max report time range is 31 days.
            upper_bound = min(end_at.date() + timedelta(days=1), lower_bound + timedelta(days=31))
            af_url = af_base_url + lower_bound.strftime(begin_param_fmt) + upper_bound.strftime(end_param_fmt)
            response = self.request(url=af_url)
            response_body = BytesIO(response.content)
            response_df = read_csv(response_body, skiprows=4)
            for idx, row in response_df.iterrows():
                row_local_dt = self._datetime_from_actual_forecast_date_column(row['Date'])

                # Rows exist when no load or forecast is available (denoted by '-').
                load_mw = None
                market = None
                if row['Actual AIL'] != '-':
                    load_mw = float(row['Actual AIL'].replace(',', ''))
                    market = self.MARKET_CHOICES.hourly
                elif row['Day-Ahead Forecasted AIL'] != '-':
                    load_mw = float(row['Day-Ahead Forecasted AIL'].replace(',', ''))
                    market = self.MARKET_CHOICES.dam

                if load_mw and start_at <= row_local_dt <= end_at and row_local_dt.date() >= iter_date:
                    load_ts.append({
                        'ba_name': self.NAME,
                        'timestamp': row_local_dt.astimezone(pytz.utc),
                        'freq': self.FREQUENCY_CHOICES.hourly,
                        'market': market,
                        'load_MW': load_mw
                    })
            iter_date = upper_bound
        return load_ts

    def _datetime_from_actual_forecast_date_column(self, date_col):
        """
        Parses the 'Date' column from the ActualForecastWMRQHReportServlet CSV.

        To get a correct datetime from a date + "hour ending" we simply add the hours to the date such that:
            2017-12-31, hour ending 1 is 2017-12-31 01:00:00
            2017-12-31, hour ending 24 is 2018-01-01 00:00:00

        Furthermore, change between daylight savings and standard time requires special handling. The change back to
        standard time in the autumn contains hour ending "02" and "02*". See
        https://www.aeso.ca/market/market-updates/daylight-savings-time-notice-4/ for details.

        :param str date_col: The 'Date' column value ActualForecastWMRQHReportServlet CSV.
        :return: A timezone-aware datetime parsed from the 'Date' column.
        :rtype: datetime
        """
        hr_ending_str = date_col[11:]
        day_str = date_col[:10]
        if hr_ending_str == "24":  # Hour ending 24 is 00:00:00 the next day.
            return self.mtn_tz.localize(datetime.strptime(day_str, '%m/%d/%Y')) + timedelta(hours=24)
        elif hr_ending_str == "02*":  # Change from daylight savings to standard time. Indicates 02:00 standard time.
            return self.mtn_tz.localize(datetime.strptime(day_str, '%m/%d/%Y')) + timedelta(hours=3)
        elif hr_ending_str == "02":  # Quirk which still handles 02:00, even the hour in DST before standard time.
            return self.mtn_tz.localize(datetime.strptime(day_str, '%m/%d/%Y')) + timedelta(hours=2)
        else:  # All other hours, including change from standard time to daylight savings.
            return self.mtn_tz.localize(datetime.strptime(day_str + ' ' + hr_ending_str, '%m/%d/%Y %H'))

    def _parse_latest_generation(self, latest_df):
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

        generation_ts = list([])
        for idx, row in fuels_df.iterrows():
            generation_ts.append({
                'ba_name': self.NAME,
                'timestamp': local_dt.astimezone(pytz.utc),
                'freq': self.FREQUENCY_CHOICES.fivemin,
                'market': self.MARKET_CHOICES.fivemin,
                'fuel_name': row.label,
                'gen_MW': float(row.tng)
            })

        return generation_ts

    def _parse_latest_trade(self, latest_df):
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

        trade_ts = [{
            'ba_name': self.NAME,
            'timestamp': local_dt.astimezone(pytz.utc),
            'freq': self.FREQUENCY_CHOICES.fivemin,
            'market': self.MARKET_CHOICES.fivemin,
            'net_exp_MW': net_actual_interchange
        }]
        return trade_ts

    def _parse_latest_load(self, latest_df):
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

        load_ts = [{
            'ba_name': self.NAME,
            'timestamp': local_dt.astimezone(pytz.utc),
            'freq': self.FREQUENCY_CHOICES.fivemin,
            'market': self.MARKET_CHOICES.fivemin,
            'load_MW': alberta_internal_load
        }]
        return load_ts

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

    @staticmethod
    def is_prev_hr_ending_24_reqd(local_dt):
        """
        :param datetime local_dt: A timezone-aware local datetime.
        :return: Whether the "hour ending" 24 is required from the previous day's report to populate data for the
           provided datetime.
        :rtype bool
        """
        return local_dt.hour == 0


class ParserFormat:
    generation = 'generation'
    load = 'load'
    trade = 'trade'
    lmp = 'lmp'
