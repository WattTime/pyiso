from collections import OrderedDict
from datetime import datetime
from datetime import timedelta

import pytz
from lxml import objectify

from pyiso import LOGGER
from pyiso.base import BaseClient


class IESOClient(BaseClient):
    """
    The Independent Electricity System Operator (IESO) of Ontario publishes a variety of public XML reports at
    http://reports.ieso.ca/public/ which can be stitched together to implement WattTime's pyiso API.
    """

    NAME = 'IESO'
    TZ_NAME = 'EST'  # IESO is always in standard time.

    base_url = 'http://reports.ieso.ca/public/'

    fuels = {
        'NUCLEAR': 'nuclear',
        'GAS': 'natgas',
        'HYDRO': 'hydro',
        'WIND': 'wind',
        'SOLAR': 'solar',
        'BIOFUEL': 'biomass',
        'OTHER': 'other'
    }

    def __init__(self):
        super(IESOClient, self).__init__()
        self.local_now = self.local_now()  # timezone aware
        self.local_start_of_day = self.local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        self.local_end_of_day = self.local_now.replace(hour=23, minute=59, second=59, microsecond=999999)

    def handle_options(self, **kwargs):
        super(IESOClient, self).handle_options(**kwargs)

        if self.options.get('latest', None):
            self.options['historical'] = False
            self.options['forecast'] = False
        elif self.options['start_at'] < self.local_now:
            self.options['historical'] = True

    def get_generation(self, latest=False, yesterday=False, start_at=None, end_at=None, **kwargs):
        generation_ts = list([])
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, **kwargs)

        gen_out_cap_handler = GeneratorOutputCapabilityReportHandler(ieso_client=self)
        gen_out_by_fuel_handler = GeneratorOutputByFuelHourlyReportHandler(ieso_client=self)
        adequacy_handler = AdequacyReportHandler(ieso_client=self)

        if self.options.get('latest', False):
            self._get_latest_report_trimmed(result_ts=generation_ts, report_handler=gen_out_cap_handler,
                                            parser_format=ParserFormat.generation)
        elif self.options.get('start_at', None) and self.options.get('end_at', None):
            # For long time ranges more than hour ending 1, seven days in the past, it is more efficient to request the
            # Generator Output by Fuel Type Hourly Report rather than repeated calls to the Generator Output and
            # Capability Report.
            # TODO Minor optimization, but this actually check if the start/end range is greater than 7 days.
            if self.options['start_at'] < self.local_start_of_day.replace(hour=1) - timedelta(days=7):
                self.timeout_seconds = 90  # These reports can get rather large ~7MB for a full year.
                range_start = max(self.options['start_at'], gen_out_by_fuel_handler.earliest_available_datetime())
                range_end = min(self.options['end_at'], gen_out_by_fuel_handler.latest_available_datetime())
                self._get_report_range(result_ts=generation_ts, report_handler=gen_out_by_fuel_handler,
                                       parser_format=ParserFormat.generation, range_start=range_start,
                                       range_end=range_end)
            elif self.options.get('historical', False):
                range_start = max(self.options['start_at'], gen_out_cap_handler.earliest_available_datetime())
                range_end = min(self.options['end_at'], gen_out_cap_handler.latest_available_datetime())
                self._get_report_range(result_ts=generation_ts, report_handler=gen_out_cap_handler,
                                       parser_format=ParserFormat.generation, range_start=range_start,
                                       range_end=range_end)

            if self.options.get('forecast', False):
                range_start = max(self.options['start_at'], self.local_now)
                range_end = min(self.options['end_at'], adequacy_handler.latest_available_datetime())
                self._get_report_range(result_ts=generation_ts, report_handler=adequacy_handler,
                                       parser_format=ParserFormat.generation, range_start=range_start,
                                       range_end=range_end)
        else:
            LOGGER.warn('No valid options were supplied.')
        return generation_ts

    def get_load(self, latest=False, yesterday=False, start_at=None, end_at=None, **kwargs):
        load_ts = list([])
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, **kwargs)
        rt_const_totals_handler = RealTimeConstrainedTotalsReportHandler(ieso_client=self)
        predisp_const_totals_handler = PredispatchConstrainedTotalsReportHandler(ieso_client=self)

        if self.options.get('latest', False):
            self._get_latest_report_trimmed(result_ts=load_ts, report_handler=rt_const_totals_handler,
                                            parser_format=ParserFormat.load)
        elif self.options.get('start_at', None) and self.options.get('end_at', None):
            if self.options.get('historical', False):
                range_start = max(self.options['start_at'], rt_const_totals_handler.earliest_available_datetime())
                range_end = min(self.options['end_at'], rt_const_totals_handler.latest_available_datetime())
                self._get_report_range(result_ts=load_ts, report_handler=rt_const_totals_handler,
                                       parser_format=ParserFormat.load, range_start=range_start, range_end=range_end)
            if self.options.get('forecast', False):
                range_start = max(self.options['start_at'], rt_const_totals_handler.latest_available_datetime(),
                                  predisp_const_totals_handler.earliest_available_datetime())
                range_end = min(self.options['end_at'], predisp_const_totals_handler.latest_available_datetime())
                self._get_report_range(result_ts=load_ts, report_handler=predisp_const_totals_handler,
                                       parser_format=ParserFormat.load, range_start=range_start, range_end=range_end)
        else:
            LOGGER.warn('No valid options were supplied.')
        return load_ts

    def get_trade(self, latest=False, yesterday=False, start_at=None, end_at=None, **kwargs):
        trade_ts = list([])
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, **kwargs)
        inter_sched_flow_handler = IntertieScheduleFlowReportHandler(ieso_client=self)
        adequacy_handler = AdequacyReportHandler(ieso_client=self)

        if self.options.get('latest', False):
            self._get_latest_report_trimmed(result_ts=trade_ts, report_handler=inter_sched_flow_handler,
                                            parser_format=ParserFormat.trade)
        elif self.options.get('start_at', None) and self.options.get('end_at', None):
            if self.options.get('historical', False):
                range_start = max(self.options['start_at'], inter_sched_flow_handler.earliest_available_datetime())
                range_end = min(self.options['end_at'], inter_sched_flow_handler.latest_available_datetime())
                self._get_report_range(result_ts=trade_ts, report_handler=inter_sched_flow_handler,
                                       parser_format=ParserFormat.trade, range_start=range_start, range_end=range_end)
            if self.options.get('forecast', False):
                range_start = max(self.options['start_at'], inter_sched_flow_handler.latest_available_datetime(),
                                  adequacy_handler.earliest_available_datetime())
                range_end = min(self.options['end_at'], adequacy_handler.latest_available_datetime())
                self._get_report_range(result_ts=trade_ts, report_handler=adequacy_handler,
                                       parser_format=ParserFormat.trade, range_start=range_start, range_end=range_end)
        else:
            LOGGER.warn('No valid options were supplied.')
        return trade_ts

    def _get_report_range(self, result_ts, report_handler, parser_format, range_start, range_end):
        """
        :param list result_ts: The timeseries which results which data will be appended to.
        :param BaseIesoReportHandler report_handler: The report handler to be used for the time range.
        :param str parser_format: The WattTime client format the data should be parsed into.
        :param datetime range_start: The start of the time range that report data should be requested for.
        :param datetime range_end: The end of the time range that report data should be requested for.
        """
        report_datetime = range_start.astimezone(pytz.timezone(self.TZ_NAME))
        while report_datetime <= min(range_end, report_handler.latest_available_datetime()):
            report_url = report_handler.report_url(report_datetime=report_datetime)
            response = self.request(url=report_url)
            report_handler.parse_report(xml_content=response.content, result_ts=result_ts, parser_format=parser_format,
                                        min_datetime=range_start, max_datetime=range_end)
            report_datetime = report_handler.datetime_for_next_report_request(tz_aware_dt=report_datetime)

    def _get_latest_report_trimmed(self, result_ts, report_handler, parser_format):
        """
        :param list result_ts: The timeseries which results which data will be appended to. Results will be trimmed to
            the latest record.
        :param BaseIesoReportHandler report_handler:
        :param str parser_format: One of the ParserFormat enum strings.
        """
        report_url = report_handler.report_url()
        response = self.request(url=report_url)
        report_handler.parse_report(xml_content=response.content, result_ts=result_ts, parser_format=parser_format,
                                    min_datetime=report_handler.earliest_available_datetime(),
                                    max_datetime=report_handler.latest_available_datetime())
        last_timestamp = result_ts[-1].get('timestamp', None)
        i = len(result_ts) - 1
        while i >= 0:
            if result_ts[i].get('timestamp', None) != last_timestamp:
                del result_ts[i]
            i -= 1


class BaseIesoReportHandler(object):
    """
    Base class to standardize how IESO market reports are parsed and to define date-related attributes.
    """
    BASE_URL = 'http://reports.ieso.ca/public/'

    def __init__(self, ieso_client):
        """
        :param IESOClient ieso_client: The WattTime client that this report handler is parsing data for.
        """
        self.ieso_client = ieso_client

    def frequency(self):
        """
        The frequency of the report data.
        :return: One of BaseClient.FREQUENCY_CHOICES
        """
        raise NotImplementedError('Derived classes must implement the frequency method.')

    def market(self):
        """
        The market which the report data is for.
        :return: One of BaseClient.MARKET_CHOICES
        """
        raise NotImplementedError('Derived classes must implement the market method.')

    def report_url(self, report_datetime=None):
        """
        :param datetime report_datetime: If provided, report URL for that date will be constructed. If no datetime is
            provided, the current report URL will be constructed.
        :return: The fully-qualified request URL.
        :rtype: str
        """
        raise NotImplementedError('Derived classes must implement the request_report method.')

    def earliest_available_datetime(self):
        """
        :return: A tz-aware datetime representing the earliest EST datetime that (historical) report data is publicly
            available.
        :rtype: datetime
        """
        raise NotImplementedError('Derived classes must implement the earliest_available_datetime method.')

    def latest_available_datetime(self):
        """
        :return: A tz-aware datetime representing the latest EST datetime that (current/future) report data is publicly
            available.
        :rtype: datetime
        """
        raise NotImplementedError('Derived classes must implement the latest_available_datetime method.')

    def report_interval(self):
        """
        The amount of time time between reports.
        :return: One of the ReportFileInterval enum strings.
        """
        raise NotImplementedError('Derived classes must implement the report_interval method.')

    def parse_report(self, xml_content, result_ts, parser_format, min_datetime, max_datetime):
        """
        Parses the report content and appends them to a timeseries of results, in one of several WattTime client
        formats.

        :param str xml_content: The XML response body of the report.
        :param list result_ts: The timeseries (a list of dicts) which results should be appended to. Timestamps are in
            UTC.
        :param str parser_format: The parser format used to append results.
        :param datetime min_datetime: The minimum datetime that can be appended to the results.
        :param datetime max_datetime: The maximum datetime that can be appended to the results.
        """
        raise NotImplementedError('Derived classes must implement the parse_report method.')

    def append_generation(self, result_ts, tz_aware_dt, gen_mw, fuel):
        """
        Appends a dict to the results list, with the keys [ba_name, timestamp, freq, market, fuel_name, gen_MW].
        Timestamps are in UTC.
        :param list result_ts: The timeseries (a list of dicts) which results should be appended to.
        :param datetime tz_aware_dt: The datetime of the data being appended (timezone-aware).
        :param float gen_mw: Electricity generation in megawatts (MW)
        :param str fuel: IESO fuel name (will be converted to WattTime name).
        """
        result_ts.append({
            'ba_name': IESOClient.NAME,
            'timestamp': tz_aware_dt.astimezone(pytz.utc),
            'freq': self.frequency(),
            'market': self.market(),
            'fuel_name': IESOClient.fuels[fuel],
            'gen_MW': gen_mw
        })

    def append_load(self, result_ts, tz_aware_dt, load_mw):
        """
        Appends a dict to the results list, with the keys [ba_name, timestamp, freq, market, load_MW]. Timestamps are
        in UTC.
        :param list result_ts: The timeseries (a list of dicts) which results should be appended to.
        :param datetime tz_aware_dt: The datetime of the data being appended (timezone-aware).
        :param float load_mw: Electricity load in megawatts (MW).
        """
        result_ts.append({
            'ba_name': IESOClient.NAME,
            'timestamp': tz_aware_dt.astimezone(pytz.utc),
            'freq': self.frequency(),
            'market': self.market(),
            'load_MW': load_mw
        })

    def append_trade(self, result_ts, tz_aware_dt, net_exp_mw):
        """
        Appends a dict to the results list, with the keys [ba_name, timestamp, freq, market, net_exp_MW]. Timestamps
        are in UTC.
        :param list result_ts: The timeseries (a list of dicts) which results should be appended to.
        :param datetime tz_aware_dt: The datetime of the data being appended (timezone-aware).
        :param float net_exp_mw: The net exported megawatts (MW) (i.e. export - import). Negative values indicate that
            more electricity was imported than exported.
        """
        result_ts.append({
            'ba_name': IESOClient.NAME,
            'timestamp': tz_aware_dt.astimezone(pytz.utc),
            'freq': self.frequency(),
            'market': self.market(),
            'net_exp_MW': net_exp_mw
        })

    def datetime_for_report_request(self, tz_aware_dt):
        """
        This method converts a timezone-aware datetime to EST and makes necessary "hour ending" considerations.

        The hourly IESO reports follow the convention of "hour ending" for reporting data. This means that hour ending 1
        corresponds to the time 01:00 and hour ending 23 corresponds to 23:00. The time 00:00 for a given day
        is represented by hour ending 24 contained in the previous day's report.

        :param datetime tz_aware_dt: A timezone-aware datetime.
        :return: A date which should be used when requesting the date-formatted URL to retrieve a report containing
           data for the datetime.
        :rtype: datetime
        """
        est_datetime = tz_aware_dt.astimezone(pytz.timezone(self.ieso_client.TZ_NAME))
        if self.report_interval() in [ReportFileInterval.daily, ReportFileInterval.yearly] and est_datetime.hour == 0:
            return est_datetime - timedelta(hours=1)
        elif self.report_interval() == ReportFileInterval.hourly and est_datetime.hour == 0 and est_datetime.minute < 5:
            return est_datetime - timedelta(minutes=1)
        return est_datetime

    def datetime_for_next_report_request(self, tz_aware_dt):
        """
        When requesting reports for a time range, some scenarios (e.g. hour ending 23 and report boundaries) require
        that the datetime for the next report request not be based off the full report interval. This convenience
        method helps determine the next datetime that should be used when iterating over report requests
        chronologically to retrieve data for a time range.

        :param tz_aware_dt: The timezone-aware datetime of the of the report that has already been requested.
        :return: A timezone-aware datetime that should be used for the next report request when requesting reports for
            a time range chronologically.
        """
        report_interval = self.report_interval()
        est_datetime = tz_aware_dt.astimezone(pytz.timezone(self.ieso_client.TZ_NAME))
        if report_interval == ReportFileInterval.yearly:
            if self.is_start_of_year(est_datetime):
                return tz_aware_dt + timedelta(hours=1)
            else:
                return tz_aware_dt.replace(year=tz_aware_dt.year + 1)
        elif report_interval == ReportFileInterval.daily:
            if self.is_start_of_day(est_datetime):
                return tz_aware_dt + timedelta(hours=1)
            else:
                return tz_aware_dt + timedelta(days=1)
        elif report_interval == ReportFileInterval.hourly:
            if self.is_start_of_hour(est_datetime):
                return tz_aware_dt + timedelta(minutes=5)
            else:
                return tz_aware_dt + timedelta(hours=1)
        else:
            raise RuntimeError('Unexpected report interval.')

    @staticmethod
    def is_start_of_year(dt):
        """
        :param datetime dt: Any datetime.
        :return: True/False indicating if the datetime is exactly 00:00:00.000 on January 1st.
        :rtype: bool
        """
        dt_diff = dt - dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        return dt_diff == timedelta(microseconds=0)

    @staticmethod
    def is_start_of_day(dt):
        """
        :param datetime dt: Any datetime.
        :return: True/False indicating if the time is exactly 00:00:00.000.
        :rtype: bool
        """
        dt_diff = dt - dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return dt_diff == timedelta(microseconds=0)

    @staticmethod
    def is_start_of_hour(dt):
        """
        :param datetime dt: Any datetime.
        :return: True/False indicating if the datetime is the start of an hour (i.e. HH:00:00.000).
        :rtype: bool
        """
        dt_diff = dt - dt.replace(minute=0, second=0, microsecond=0)
        return dt_diff == timedelta(microseconds=0)


class IntertieScheduleFlowReportHandler(BaseIesoReportHandler):
    def frequency(self):
        return BaseClient.FREQUENCY_CHOICES.fivemin

    def market(self):
        return BaseClient.MARKET_CHOICES.hourly

    def report_url(self, report_datetime=None):
        filename = 'PUB_IntertieScheduleFlow.xml'
        if report_datetime is not None:
            request_dt = self.datetime_for_report_request(report_datetime)
            filename = request_dt.strftime('PUB_IntertieScheduleFlow_%Y%m%d.xml')
        return self.BASE_URL + 'IntertieScheduleFlow/' + filename

    def earliest_available_datetime(self):
        # Earliest historical data available is hour ending 1, three months in the past.
        return self.ieso_client.local_now.replace(hour=1, minute=0, second=0, microsecond=0) - timedelta(days=90)

    def latest_available_datetime(self):
        # A brief look at versioned reports indicate that they're typically posted with ~30 minute lag. Returning
        # 45 minutes in the past to be conservative.
        return self.ieso_client.local_now - timedelta(minutes=45)

    def report_interval(self):
        return ReportFileInterval.daily

    def parse_report(self, xml_content, result_ts, parser_format, min_datetime, max_datetime):
        if parser_format == ParserFormat.trade:
            document = objectify.fromstring(xml_content)
            doc_body = document.IMODocBody
            doc_date_local = doc_body.Date  # %Y%m%d

            for actual in doc_body.Totals.Actuals.Actual:
                net_exp_mw = actual.Flow
                hour_local = str(actual.Hour - 1).zfill(2)
                minutes = actual.Interval * 5  # Interval 1 is minute 05. Interval 12 is minute 60 (ie. 00:00 next hour)
                hr_start_str = doc_date_local + ' ' + hour_local + ':00'
                row_datetime = self.ieso_client.utcify(local_ts_str=hr_start_str) + timedelta(minutes=minutes)

                # For the current day the report fills  "Actual" elements in the future with the value 0. Batches of
                # 5-minute observations are posted hourly, at the end of the hour. Furthermore, the time between the
                # end of an hour and the report's availability online is typically 30 minutes. The end-of-hour
                # reporting schedule combined with time lag before the report is available online means that
                # ~1.5 hours of "recent" observations could be filled with 0 values. Although a bit hacky, it's
                # unlikely that net exports are exactly 0MW for an interval, so skip recording data in these cases.
                skip = False
                if net_exp_mw == 0 and row_datetime > (self.ieso_client.local_now - timedelta(hours=2)):
                    skip = True

                if min_datetime <= row_datetime <= max_datetime and not skip:
                    self.append_trade(result_ts=result_ts, tz_aware_dt=row_datetime, net_exp_mw=net_exp_mw)
        else:
            raise RuntimeError('Intertie Schedule Flow Report can only be parsed using trade format.')


class AdequacyReportHandler(BaseIesoReportHandler):
    def report_interval(self):
        return ReportFileInterval.daily

    def report_url(self, report_datetime=None):
        filename = 'PUB_Adequacy2.xml'
        if report_datetime is not None:
            request_dt = self.datetime_for_report_request(report_datetime)
            filename = request_dt.strftime('PUB_Adequacy2_%Y%m%d.xml')
        return self.BASE_URL + 'Adequacy2/' + filename

    def earliest_available_datetime(self):
        # Earliest historical data available is hour ending 1, three months in the past.
        return self.ieso_client.local_now.replace(hour=1, minute=0, second=0, microsecond=0) - timedelta(days=90)

    def latest_available_datetime(self):
        # Although reports exist ~1 month into the future, only the current and next days contain the "Schedules"
        # element, which is used during parsing. The IESO states that the schedules for the next day are posted by
        # approximately 11:15am (see http://reports.ieso.ca/docrefs/helpfile/Adequacy2_h2.pdf). Anecdotally, I've seen
        # "approximate" mean a little after 11:20am. The algorithm below uses 11:30am to be conservative.
        local_now = self.ieso_client.local_now
        next_day_availability = local_now.replace(hour=11, minute=30, second=0, microsecond=0)
        start_of_tomorrow = self.ieso_client.local_start_of_day + timedelta(days=1)
        if local_now >= next_day_availability:
            return start_of_tomorrow + timedelta(days=1)
        else:
            return start_of_tomorrow

    def market(self):
        return BaseClient.MARKET_CHOICES.dam

    def parse_report(self, xml_content, result_ts, parser_format, min_datetime, max_datetime):
        document = objectify.fromstring(xml_content)
        doc_body = document.DocBody
        day_start_str = doc_body.DeliveryDate + ' 00:00:00'
        if parser_format == ParserFormat.generation:
            # InternalResources is misleading. Each fuel is an internal resource, and we iterate hours of each fuel.
            for internal_resource in doc_body.ForecastSupply.InternalResources.InternalResource:
                fuel = str.upper(internal_resource.FuelType.text)
                if fuel != 'DISPATCHABLE LOAD':  # TODO What to do about dispatchable load? Skipping for now.
                    for schedule in internal_resource.Schedules.Schedule:
                        fuel_gen_mw = schedule.EnergyMW.pyval
                        hr_ending = schedule.DeliveryHour.pyval
                        row_datetime = self.ieso_client.utcify(local_ts_str=day_start_str) + timedelta(hours=hr_ending)
                        if min_datetime <= row_datetime <= max_datetime:
                            self.append_generation(result_ts=result_ts, tz_aware_dt=row_datetime, fuel=fuel,
                                                   gen_mw=fuel_gen_mw)
        elif parser_format == ParserFormat.trade:
            imports_exports = OrderedDict()  # {'ts_local':{'import'|'export',val_mw}}
            for import_schedule in doc_body.ForecastSupply.ZonalImports.TotalImports.Schedules.Schedule:
                hr_ending = import_schedule.DeliveryHour.pyval
                row_datetime = self.ieso_client.utcify(local_ts_str=day_start_str) + timedelta(hours=hr_ending)
                imports_exports[row_datetime] = {'import': import_schedule.EnergyMW.pyval}
            for export_schedule in doc_body.ForecastDemand.ZonalExports.TotalExports.Schedules.Schedule:
                hr_ending = export_schedule.DeliveryHour.pyval
                row_datetime = self.ieso_client.utcify(local_ts_str=day_start_str) + timedelta(hours=hr_ending)
                hr_entry = imports_exports.get(row_datetime)
                hr_entry.update({'export': export_schedule.EnergyMW.pyval})
                imports_exports[row_datetime] = hr_entry
            for row_datetime, imp_exp in imports_exports.items():
                # Handle export passed as positive/negative value.
                net_exp_mw = abs(imp_exp.get('export', 0)) - abs(imp_exp.get('import', 0))
                if min_datetime <= row_datetime <= max_datetime:
                    self.append_trade(result_ts=result_ts, tz_aware_dt=row_datetime, net_exp_mw=net_exp_mw)
        else:
            raise RuntimeError('Adequacy Report should only be parsed using generation or trade formats.')

    def frequency(self):
        return BaseClient.FREQUENCY_CHOICES.hourly


class RealTimeConstrainedTotalsReportHandler(BaseIesoReportHandler):
    def report_interval(self):
        return ReportFileInterval.hourly

    def frequency(self):
        return BaseClient.FREQUENCY_CHOICES.fivemin

    def market(self):
        return BaseClient.MARKET_CHOICES.fivemin

    def earliest_available_datetime(self):
        # Hour ending 1, 31 days in the past.
        return self.ieso_client.local_now.replace(hour=1, minute=0, second=0, microsecond=0) - timedelta(days=31)

    def latest_available_datetime(self):
        return self.ieso_client.local_now

    def parse_report(self, xml_content, result_ts, parser_format, min_datetime, max_datetime):
        document = objectify.fromstring(xml_content)
        doc_body = document.DocBody
        day = doc_body.DeliveryDate
        if parser_format == ParserFormat.load:
            hour_local = str(doc_body.DeliveryHour - 1).zfill(2)
            for interval_energy in doc_body.Energies.IntervalEnergy:
                # Interval 1 is minute 05. Interval 12 is minute 60 (ie. 00:00 next hour)
                minutes = interval_energy.Interval * 5
                hr_start_str = day + ' ' + hour_local + ':00'
                for mq in interval_energy.MQ:
                    if mq.MarketQuantity == 'ONTARIO DEMAND':
                        load_mw = mq.EnergyMW.pyval
                        row_datetime = self.ieso_client.utcify(local_ts_str=hr_start_str) + timedelta(minutes=minutes)
                        if min_datetime <= row_datetime <= max_datetime:
                            self.append_load(result_ts=result_ts, tz_aware_dt=row_datetime, load_mw=load_mw)
        else:
            raise RuntimeError('Realtime Constrained Totals Report can only be parsed using load format.')

    def report_url(self, report_datetime=None):
        filename = 'PUB_RealtimeConstTotals.xml'
        if report_datetime is not None:
            request_dt = self.datetime_for_report_request(report_datetime)
            hour_ending = request_dt.hour if request_dt.minute < 5 else request_dt.hour + 1
            url_hour = str(hour_ending).zfill(2)
            filename = request_dt.strftime('PUB_RealtimeConstTotals_%Y%m%d' + url_hour + '.xml')
        return self.BASE_URL + 'RealtimeConstTotals/' + filename


class PredispatchConstrainedTotalsReportHandler(BaseIesoReportHandler):
    def report_interval(self):
        return ReportFileInterval.daily

    def market(self):
        return BaseClient.MARKET_CHOICES.hourly

    def parse_report(self, xml_content, result_ts, parser_format, min_datetime, max_datetime):
        document = objectify.fromstring(xml_content)
        doc_body = document.DocBody
        day_start_str = doc_body.DeliveryDate + ' 00:00:00'
        if parser_format == ParserFormat.load:
            for hrly_const_energy in doc_body.Energies.HourlyConstrainedEnergy:
                hr_ending = hrly_const_energy.DeliveryHour.pyval
                for mq in hrly_const_energy.MQ:
                    if mq.MarketQuantity == 'Total Load':
                        load_mw = mq.EnergyMW.pyval
                        row_datetime = self.ieso_client.utcify(local_ts_str=day_start_str) + timedelta(hours=hr_ending)
                        if min_datetime <= row_datetime <= max_datetime:
                            self.append_load(result_ts=result_ts, tz_aware_dt=row_datetime, load_mw=load_mw)
        else:
            raise RuntimeError('Predispatch Constrained Totals Report can only be parsed using load format.')

    def report_url(self, report_datetime=None):
        filename = 'PUB_PredispConstTotals.xml'
        if report_datetime is not None:
            request_dt = self.datetime_for_report_request(report_datetime)
            filename = request_dt.strftime('PUB_PredispConstTotals_%Y%m%d.xml')
        return self.BASE_URL + 'PredispConstTotals/' + filename

    def latest_available_datetime(self):
        # Predispatch data for the next day is posted at approximately 15:15. Anecdotally, I've seen as late as 15:18.
        # The algorithm below uses 15:30 to be conservative.
        local_now = self.ieso_client.local_now
        next_day_availability = local_now.replace(hour=15, minute=30, second=0, microsecond=0)
        start_of_tomorrow = self.ieso_client.local_start_of_day + timedelta(days=1)
        if local_now >= next_day_availability:
            return start_of_tomorrow + timedelta(days=1)
        else:
            return start_of_tomorrow

    def earliest_available_datetime(self):
        # Hour ending 1, 31 days in the past.
        return self.ieso_client.local_now.replace(hour=1, minute=0, second=0, microsecond=0) - timedelta(days=31)

    def frequency(self):
        return BaseClient.FREQUENCY_CHOICES.hourly


class GeneratorOutputCapabilityReportHandler(BaseIesoReportHandler):
    def market(self):
        return BaseClient.MARKET_CHOICES.hourly

    def parse_report(self, xml_content, result_ts, parser_format, min_datetime, max_datetime):
        if parser_format == ParserFormat.generation:
            imo_document = objectify.fromstring(xml_content)
            imo_doc_body = imo_document.IMODocBody
            day_start_str = imo_doc_body.Date + ' 00:00:00'
            fuels_hourly = {}
            for fuel in IESOClient.fuels.keys():
                fuels_hourly[fuel] = {}

            # Iterate over each hourly value for each generator, creating a dictionary keyed by fuel and values are
            # lists containing generation by hour-of-day.
            for generator in imo_doc_body.Generators.Generator:
                fuel_type = generator.FuelType
                for output in generator.Outputs.Output:
                    hr_ending = output.Hour.pyval
                    try:
                        gen_mw = output.EnergyMW
                    except AttributeError:  # Inexplicably, some 'Output' elements are missing 'EnergyMW' child element.
                        gen_mw = 0
                    fuel_hour_endings = fuels_hourly[fuel_type]
                    existing_gen_mw = fuel_hour_endings.get(hr_ending, 0)
                    fuel_hour_endings[hr_ending] = existing_gen_mw + gen_mw

            # Iterate over aggregated results to create generation fuel mix format
            for fuel, fuel_hour_endings in fuels_hourly.items():
                for fuel_hr_ending, fuel_gen_mw in fuel_hour_endings.items():
                    row_datetime = self.ieso_client.utcify(local_ts_str=day_start_str) + timedelta(hours=fuel_hr_ending)
                    if min_datetime <= row_datetime <= max_datetime:
                        self.append_generation(result_ts=result_ts, tz_aware_dt=row_datetime, fuel=fuel,
                                               gen_mw=fuel_gen_mw)
        else:
            raise RuntimeError('Generator Output Capability Report can only be parsed using generation format.')

    def report_url(self, report_datetime=None):
        filename = 'PUB_GenOutputCapability.xml'
        if report_datetime is not None:
            request_dt = self.datetime_for_report_request(report_datetime)
            filename = request_dt.strftime('PUB_GenOutputCapability_%Y%m%d.xml')
        return self.BASE_URL + 'GenOutputCapability/' + filename

    def report_interval(self):
        return ReportFileInterval.daily

    def earliest_available_datetime(self):
        # Earliest historical data available is hour ending 1, three months in the past.
        return self.ieso_client.local_now.replace(hour=1, minute=0, second=0, microsecond=0) - timedelta(days=90)

    def latest_available_datetime(self):
        # A brief look at versioned reports indicate that they're typically posted with ~15 minute lag. Returning
        # 30 minutes in the past to be conservative.
        return self.ieso_client.local_now - timedelta(minutes=30)

    def frequency(self):
        return BaseClient.FREQUENCY_CHOICES.hourly


class GeneratorOutputByFuelHourlyReportHandler(BaseIesoReportHandler):
    def market(self):
        return BaseClient.MARKET_CHOICES.hourly

    def frequency(self):
        return BaseClient.FREQUENCY_CHOICES.hourly

    def report_interval(self):
        return ReportFileInterval.yearly

    def report_url(self, report_datetime=None):
        filename = 'PUB_GenOutputbyFuelHourly.xml'
        if report_datetime is not None:
            request_dt = self.datetime_for_report_request(report_datetime)
            filename = request_dt.strftime('PUB_GenOutputbyFuelHourly_%Y.xml')
        return self.BASE_URL + 'GenOutputbyFuelHourly/' + filename

    def earliest_available_datetime(self):
        # Hour ending 1, two years in the past.
        earliest_year = self.ieso_client.local_now.year - 2
        return self.ieso_client.local_now.replace(year=earliest_year, month=1, day=1, hour=1, minute=0, second=0,
                                                  microsecond=0)

    def latest_available_datetime(self):
        return self.ieso_client.local_now

    def parse_report(self, xml_content, result_ts, parser_format, min_datetime, max_datetime):
        document = objectify.fromstring(xml_content)
        doc_body = document.DocBody
        for daily_data in doc_body.DailyData:
            day_start_str = daily_data.Day + ' 00:00:00'
            for hourly_data in daily_data.HourlyData:
                hr_ending = hourly_data.Hour.pyval
                row_datetime = self.ieso_client.utcify(local_ts_str=day_start_str) + timedelta(hours=hr_ending)
                if min_datetime <= row_datetime <= max_datetime:
                    for fuel_total in hourly_data.FuelTotal:
                        fuel = fuel_total.Fuel
                        try:
                            fuel_gen_mw = fuel_total.EnergyValue.Output
                        except AttributeError:  # When 'OutputQuality' value is -1, there is not 'Output' element.
                            fuel_gen_mw = 0
                        self.append_generation(result_ts=result_ts, tz_aware_dt=row_datetime, fuel=fuel,
                                               gen_mw=fuel_gen_mw)


class ParserFormat:
    """
    Since report handlers can parse the XML reports into a variety of formats, this enum facilitates passing which
    pyiso output format should be used between IESOClient and the BaseIesoReportHandler implementations.
    """
    generation = 'generation'
    load = 'load'
    trade = 'trade'
    lmp = 'lmp'


class ReportFileInterval:
    """
    The report files are published with filenames containing date/time information. Some reports are broken up
    hourly, some daily, and ony yearly.
    """
    hourly = 'hourly'
    daily = 'daily'
    yearly = 'yearly'
