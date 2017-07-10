from copy import copy
from collections import OrderedDict
from collections import namedtuple

import pytz
from datetime import datetime
from datetime import timedelta
from lxml import objectify

from pyiso import LOGGER
from pyiso.base import BaseClient


ParserFormat = namedtuple('ParserFormat', ['generation', 'load', 'trade', 'lmp'])
ReportInterval = namedtuple('ReportInterval', ['hourly', 'daily', 'yearly'])


class IESOClient(BaseClient):
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

    PARSER_FORMATS = ParserFormat(generation='generation', load='load', trade='trade', lmp='lmp')

    def handle_options(self, **kwargs):
        # regular handle options
        super(IESOClient, self).handle_options(**kwargs)

        local_now = self.local_now()  # timezone aware
        local_start_of_day = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        local_end_of_day = local_now.replace(hour=23, minute=59, second=59, microsecond=999999)

        if self.options.get('latest', None):
            self.options['current_day'] = True
            self.options['historical'] = False
            self.options['forecast'] = False
        else:
            if local_start_of_day <= self.options.get('start_at', None) <= local_end_of_day:
                self.options['current_day'] = True
            if local_start_of_day <= self.options.get('end_at', None) <= local_end_of_day:
                self.options['current_day'] = True
            if self.options.get('start_at', None) < local_start_of_day and \
               self.options.get('end_at', None) > local_end_of_day:
                self.options['current_day'] = True
            if self.options['start_at'] < local_start_of_day:
                self.options['historical'] = True

    def get_generation(self, latest=False, yesterday=False, start_at=None, end_at=None, **kwargs):
        generation_ts = list([])
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, **kwargs)

        gen_out_cap_handler = GeneratorOutputCapabilityReportHandler(ieso_client=self)
        gen_out_by_fuel_handler = GeneratorOutputByFuelHourlyReportHandler(ieso_client=self)
        adequacy_handler = AdequacyReportHandler(ieso_client=self)

        if self.options.get('latest', False):
            self._get_latest_report_trimmed(result_ts=generation_ts, report_handler=gen_out_cap_handler,
                                            parser_format=IESOClient.PARSER_FORMATS.generation)
        elif self.options.get('start_at', None) and self.options.get('end_at', None):
            if self.options.get('current_day', False) and not self.options.get('historical', False):
                range_start = max(self.options['start_at'], gen_out_cap_handler.earliest_available_datetime())
                range_end = min(self.options['end_at'], gen_out_cap_handler.latest_available_datetime())
                self._get_report_range(result_ts=generation_ts, report_handler=gen_out_cap_handler,
                                       parser_format=IESOClient.PARSER_FORMATS.generation, range_start=range_start,
                                       range_end=range_end)
            elif self.options.get('yesterday', False):
                range_start = max(self.options['start_at'], gen_out_cap_handler.earliest_available_datetime())
                range_end = min(self.options['end_at'], gen_out_cap_handler.latest_available_datetime())
                self._get_report_range(result_ts=generation_ts, report_handler=gen_out_cap_handler,
                                       parser_format=IESOClient.PARSER_FORMATS.generation, range_start=range_start,
                                       range_end=range_end)
            elif self.options.get('historical', False):
                # For long time ranges at least one day in the past, it is more efficient to request the  Generator
                # Output by Fuel Type Hourly Report rather than the detailed Generator Output and Capability Report.
                range_start = max(self.options['start_at'], gen_out_by_fuel_handler.earliest_available_datetime())
                range_end = min(self.options['end_at'], gen_out_by_fuel_handler.latest_available_datetime())
                self._get_report_range(result_ts=generation_ts, report_handler=gen_out_by_fuel_handler,
                                       parser_format=IESOClient.PARSER_FORMATS.generation, range_start=range_start,
                                       range_end=range_end)

            if self.options.get('forecast', False):
                range_start = max(self.options['start_at'], self.local_now())
                range_end = min(self.options['end_at'], adequacy_handler.latest_available_datetime())
                self._get_report_range(result_ts=generation_ts, report_handler=adequacy_handler,
                                       parser_format=IESOClient.PARSER_FORMATS.generation, range_start=range_start,
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
                                            parser_format=IESOClient.PARSER_FORMATS.load)
        elif self.options.get('start_at', None) and self.options.get('end_at', None):
            if self.options.get('historical', False) or self.options.get('current_day', False):
                range_start = max(self.options['start_at'], rt_const_totals_handler.earliest_available_datetime())
                range_end = min(self.options['end_at'], rt_const_totals_handler.latest_available_datetime())
                self._get_report_range(result_ts=load_ts, report_handler=rt_const_totals_handler,
                                       parser_format=IESOClient.PARSER_FORMATS.load, range_start=range_start,
                                       range_end=range_end)
            if self.options.get('forecast', False):
                range_start = max(self.options['start_at'], rt_const_totals_handler.latest_available_datetime(),
                                  predisp_const_totals_handler.earliest_available_datetime())
                range_end = min(self.options['end_at'], predisp_const_totals_handler.latest_available_datetime())
                self._get_report_range(result_ts=load_ts, report_handler=predisp_const_totals_handler,
                                       parser_format=IESOClient.PARSER_FORMATS.load, range_start=range_start,
                                       range_end=range_end)
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
                                            parser_format=IESOClient.PARSER_FORMATS.trade)
        elif self.options.get('start_at', None) and self.options.get('end_at', None):
            if self.options.get('historical', False) or self.options.get('current_day', False):
                range_start = max(self.options['start_at'], inter_sched_flow_handler.earliest_available_datetime())
                range_end = min(self.options['end_at'], inter_sched_flow_handler.latest_available_datetime())
                self._get_report_range(result_ts=trade_ts, report_handler=inter_sched_flow_handler,
                                       parser_format=IESOClient.PARSER_FORMATS.trade, range_start=range_start,
                                       range_end=range_end)
            if self.options.get('forecast', False):
                range_start = max(self.options['start_at'], inter_sched_flow_handler.latest_available_datetime(),
                                  adequacy_handler.earliest_available_datetime())
                range_end = min(self.options['end_at'], adequacy_handler.latest_available_datetime())
                self._get_report_range(result_ts=trade_ts, report_handler=adequacy_handler,
                                       parser_format=IESOClient.PARSER_FORMATS.trade, range_start=range_start,
                                       range_end=range_end)
        else:
            LOGGER.warn('No valid options were supplied.')
        return trade_ts

    def get_lmp(self, latest=False, yesterday=False, start_at=None, end_at=None, **kwargs):
        raise NotImplementedError('The IESO does not use locational marginal pricing. See '
                                  'https://www.oeb.ca/oeb/_Documents/MSP/MSP_CMSC_Report_201612.pdf for details.')

    def _get_report_range(self, result_ts, report_handler, parser_format, range_start, range_end):
        """
        :param list result_ts: The timeseries which results which data will be appended to.
        :param BaseIesoReportHandler report_handler: The report handler to be used for the time range.
        :param str parser_format: The WattTime client format the data should be parsed into.
        :param datetime range_start: The start of the time range that report data should be requested for.
        :param datetime range_end: The end of the time range that report data should be requested for.
        """
        report_interval = report_handler.report_interval()
        report_datetime = range_start.astimezone(pytz.timezone(self.TZ_NAME))

        if report_interval == report_handler.REPORT_INTERVALS.daily:
            report_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
        elif report_interval == report_handler.REPORT_INTERVALS.hourly:
            report_datetime.replace(minute=0, second=0, microsecond=0)
        elif report_interval == report_handler.REPORT_INTERVALS.yearly:
            report_datetime.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

        while report_datetime <= min(range_end, report_handler.latest_available_datetime()):
            report_url = report_handler.report_url(report_datetime=report_datetime)
            response = self.request(url=report_url)
            report_handler.parse_report(xml_content=response.content, result_ts=result_ts, parser_format=parser_format,
                                        min_datetime=range_start, max_datetime=range_end)
            if report_interval == report_handler.REPORT_INTERVALS.yearly:
                report_datetime.replace(year=report_datetime.year + 1)
            else:
                report_datetime += report_handler.interval_timedelta()

    def _get_latest_report_trimmed(self, result_ts, report_handler, parser_format):
        """
        :param list result_ts: The timeseries which results which data will be appended to. Results will be trimmed to
            the latest record.
        :param BaseIesoReportHandler report_handler:
        :param str parser_format: One of IESOBaseClient.PARSER_FORMATS
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
    REPORT_INTERVALS = ReportInterval(hourly='hourly', daily='daily', yearly='yearly')

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
        :return: One of BaseIesoReportHandler.REPORT_INTERVALS
        """
        raise NotImplementedError('Derived classes must implement the report_interval method.')

    def interval_timedelta(self):
        """
        :return: The timedelta used to stop through a series of report requests.
        :rtype: timedelta
        """
        if self.report_interval() == self.REPORT_INTERVALS.hourly:
            return timedelta(hours=1)
        elif self.report_interval() == self.REPORT_INTERVALS.daily:
            return timedelta(days=1)
        else:
            raise NotImplementedError('The timedelta is only appropriate for hourly or daily report intervals.')

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

    def append_generation(self, result_ts, ts_local, gen_mw, fuel):
        """
        Appends a dict to the results list, with the keys [ba_name, timestamp, freq, market, fuel_name, gen_MW].
        Timestamps are in UTC.
        :param list result_ts: The timeseries (a list of dicts) which results should be appended to.
        :param str ts_local: The local datetime of the data.
        :param float gen_mw: Electricity generation in megawatts (MW)
        :param str fuel: IESO fuel name (will be converted to WattTime name).
        """
        report_dt_utc = self.ieso_client.utcify(local_ts_str=ts_local)
        result_ts.append({
            'ba_name': IESOClient.NAME,
            'timestamp': report_dt_utc,
            'freq': self.frequency(),
            'market': self.market(),
            'fuel_name': IESOClient.fuels[fuel],
            'gen_MW': gen_mw
        })

    def append_load(self, result_ts, ts_local, load_mw):
        """
        Appends a dict to the results list, with the keys [ba_name, timestamp, freq, market, load_MW]. Timestamps are
        in UTC.
        :param list result_ts: The timeseries (a list of dicts) which results should be appended to.
        :param str ts_local: The local datetime of the data.
        :param float load_mw: Electricity load in megawatts (MW).
        """
        report_dt_utc = self.ieso_client.utcify(local_ts_str=ts_local)
        result_ts.append({
            'ba_name': IESOClient.NAME,
            'timestamp': report_dt_utc,
            'freq': self.frequency(),
            'market': self.market(),
            'load_MW': load_mw
        })

    def append_trade(self, result_ts, ts_local, net_exp_mw):
        """
        Appends a dict to the results list, with the keys [ba_name, timestamp, freq, market, net_exp_MW]. Timestamps
        are in UTC.
        :param list result_ts: The timeseries (a list of dicts) which results should be appended to.
        :param str ts_local: The local datetime of the data.
        :param float net_exp_mw: The net exported megawatts (MW) (i.e. export - import). Negative values indicate that
            more electricity was imported than exported.
        """
        report_dt_utc = self.ieso_client.utcify(local_ts_str=ts_local)
        result_ts.append({
            'ba_name': IESOClient.NAME,
            'timestamp': report_dt_utc,
            'freq': self.frequency(),
            'market': self.market(),
            'net_exp_MW': net_exp_mw
        })


class IntertieScheduleFlowReportHandler(BaseIesoReportHandler):
    def frequency(self):
        return BaseClient.FREQUENCY_CHOICES.fivemin

    def market(self):
        return BaseClient.MARKET_CHOICES.hourly

    def report_url(self, report_datetime=None):
        filename = 'PUB_IntertieScheduleFlow.xml'
        if report_datetime is not None:
            est_datetime = report_datetime.astimezone(pytz.timezone(self.ieso_client.TZ_NAME))
            filename = est_datetime.strftime('PUB_IntertieScheduleFlow_%Y%m%d.xml')
        return self.BASE_URL + 'IntertieScheduleFlow/' + filename

    def earliest_available_datetime(self):
        # Earliest historical data available is three months in the past.
        return self.ieso_client.local_now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=90)

    def latest_available_datetime(self):
        # Latest five-minute date is the current time. Hourly forecast data should be pulled from the Adequacy Report.
        return self.ieso_client.local_now()

    def report_interval(self):
        return self.REPORT_INTERVALS.daily

    def parse_report(self, xml_content, result_ts, parser_format, min_datetime, max_datetime):
        if parser_format == IESOClient.PARSER_FORMATS.trade:
            document = objectify.fromstring(xml_content)
            doc_body = document.IMODocBody
            doc_date_local = doc_body.Date  # %Y%m%d

            # Actuals child elements used for historical and times <= now.
            for actual in doc_body.Totals.Actuals.Actual:
                hour_local = str(actual.Hour - 1).zfill(2)
                minute_local = str((actual.Interval - 1) * 5).zfill(2)  # Interval 1 is minute 00
                ts_local = doc_date_local + ' ' + hour_local + ':' + minute_local
                net_exp_mw = actual.Flow
                row_datetime = self.ieso_client.utcify(local_ts_str=ts_local)

                # Batches of 5-minute observations are posted hourly, at the end of the hour. Time between the end of
                # an hour and the report's availability online is typically 30 minutes. The lag between a row's
                # datetime and the availability of observations for that datetime can be as long as ~1.5 hours.
                # The report fills "Actual" elements in the future with 0, so treat 0 as missing data.
                if (min_datetime <= row_datetime <= max_datetime) and net_exp_mw > 0:
                    self.append_trade(result_ts=result_ts, ts_local=ts_local, net_exp_mw=net_exp_mw)
        else:
            raise NotImplementedError('Intertie Schedule Flow Report can only be parsed using trade format.')


class AdequacyReportHandler(BaseIesoReportHandler):
    def report_interval(self):
        return self.REPORT_INTERVALS.daily

    def report_url(self, report_datetime=None):
        filename = 'PUB_Adequacy2.xml'
        if report_datetime is not None:
            est_datetime = report_datetime.astimezone(pytz.timezone(self.ieso_client.TZ_NAME))
            filename = est_datetime.strftime('PUB_Adequacy2_%Y%m%d.xml')
        return self.BASE_URL + 'Adequacy2/' + filename

    def earliest_available_datetime(self):
        # Earliest historical data available is three months in the past.
        return self.ieso_client.local_now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=90)

    def latest_available_datetime(self):
        # Although reports exist ~1 month into the future, only the current and next days contain the "Schedules"
        # element, which is used during parsing. The IESO states that the schedules for the next day are posted by
        # approximately 11:15am (see http://reports.ieso.ca/docrefs/helpfile/Adequacy2_h2.pdf). Anecdotally, I've seen
        # "approximate" mean a little after 11:20am. The algorithm below uses 11:30am to be conservative.
        local_now = self.ieso_client.local_now()
        next_day_availability = copy(local_now).replace(hour=11, minute=30, second=0, microsecond=0)
        end_of_day = copy(local_now).replace(hour=23, minute=59, second=59, microsecond=999999)
        if local_now >= next_day_availability:
            return end_of_day + timedelta(days=1)
        else:
            return end_of_day

    def market(self):
        return BaseClient.MARKET_CHOICES.dam

    def parse_report(self, xml_content, result_ts, parser_format, min_datetime, max_datetime):
        document = objectify.fromstring(xml_content)
        doc_body = document.DocBody
        day = doc_body.DeliveryDate
        if parser_format == IESOClient.PARSER_FORMATS.generation:
            # InternalResources is misleading. Each fuel is an internal resource, and we iterate hours of each fuel.
            for internal_resource in doc_body.ForecastSupply.InternalResources.InternalResource:
                fuel = str.upper(internal_resource.FuelType.text)
                if fuel != 'DISPATCHABLE LOAD':  # TODO What to do about dispatchable load? Skipping for now.
                    for schedule in internal_resource.Schedules.Schedule:
                        ts_local = day + ' ' + str(schedule.DeliveryHour - 1).zfill(2) + ':00'
                        fuel_gen_mw = schedule.EnergyMW.pyval
                        if min_datetime <= self.ieso_client.utcify(local_ts_str=ts_local) <= max_datetime:
                            self.append_generation(result_ts=result_ts, ts_local=ts_local, fuel=fuel,
                                                   gen_mw=fuel_gen_mw)
        elif parser_format == IESOClient.PARSER_FORMATS.trade:
            imports_exports = OrderedDict()  # {'ts_local':{'import'|'export',val_mw}}
            for import_schedule in doc_body.ForecastSupply.ZonalImports.TotalImports.Schedules.Schedule:
                ts_local = day + ' ' + str(import_schedule.DeliveryHour - 1).zfill(2) + ':00'
                imports_exports[ts_local] = {'import': import_schedule.EnergyMW.pyval}
            for export_schedule in doc_body.ForecastDemand.ZonalExports.TotalExports.Schedules.Schedule:
                ts_local = day + ' ' + str(export_schedule.DeliveryHour - 1).zfill(2) + ':00'
                hr_entry = imports_exports.get(ts_local)
                hr_entry.update({'export': export_schedule.EnergyMW.pyval})
                imports_exports[ts_local] = hr_entry
            for ts_local, imp_exp in imports_exports.iteritems():
                # Handle export passed as positive/negative value.
                net_exp_mw = abs(imp_exp.get('export', 0)) - abs(imp_exp.get('import', 0))
                if min_datetime <= self.ieso_client.utcify(local_ts_str=ts_local) <= max_datetime:
                    self.append_trade(result_ts=result_ts, ts_local=ts_local, net_exp_mw=net_exp_mw)
        else:
            raise NotImplementedError('Adequacy Report should only be parsed using generation or trade formats.')

    def frequency(self):
        return BaseClient.FREQUENCY_CHOICES.hourly


class RealTimeConstrainedTotalsReportHandler(BaseIesoReportHandler):
    def report_interval(self):
        return self.REPORT_INTERVALS.hourly

    def frequency(self):
        return BaseClient.FREQUENCY_CHOICES.fivemin

    def market(self):
        return BaseClient.MARKET_CHOICES.fivemin

    def earliest_available_datetime(self):
        return self.ieso_client.local_now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=31)

    def parse_report(self, xml_content, result_ts, parser_format, min_datetime, max_datetime):
        document = objectify.fromstring(xml_content)
        doc_body = document.DocBody
        day = doc_body.DeliveryDate
        if parser_format == IESOClient.PARSER_FORMATS.load:
            hour = doc_body.DeliveryHour - 1
            for interval_energy in doc_body.Energies.IntervalEnergy:
                minute = (interval_energy.Interval - 1) * 5
                ts_local = day + ' ' + str(hour).zfill(2) + ':' + (str(minute).zfill(2))
                for mq in interval_energy.MQ:
                    if mq.MarketQuantity == 'ONTARIO DEMAND':
                        load_mw = mq.EnergyMW.pyval
                        if min_datetime <= self.ieso_client.utcify(local_ts_str=ts_local) <= max_datetime:
                            self.append_load(result_ts=result_ts, ts_local=ts_local, load_mw=load_mw)
        else:
            raise NotImplementedError('Realtime Constrained Totals Report can only be parsed using load format.')

    def latest_available_datetime(self):
        return self.ieso_client.local_now()

    def report_url(self, report_datetime=None):
        filename = 'PUB_RealtimeConstTotals.xml'
        if report_datetime is not None:
            est_datetime = report_datetime.astimezone(pytz.timezone(self.ieso_client.TZ_NAME))
            delivery_hour = est_datetime.hour + 1
            filename = est_datetime.strftime('PUB_RealtimeConstTotals_%Y%m%d' + str(delivery_hour).zfill(2) + '.xml')
        return self.BASE_URL + 'RealtimeConstTotals/' + filename


class PredispatchConstrainedTotalsReportHandler(BaseIesoReportHandler):
    def report_interval(self):
        return self.REPORT_INTERVALS.daily

    def market(self):
        return BaseClient.MARKET_CHOICES.hourly

    def parse_report(self, xml_content, result_ts, parser_format, min_datetime, max_datetime):
        document = objectify.fromstring(xml_content)
        doc_body = document.DocBody
        day = doc_body.DeliveryDate
        if parser_format == IESOClient.PARSER_FORMATS.load:
            for hrly_const_energy in doc_body.Energies.HourlyConstrainedEnergy:
                ts_local = day + ' ' + str(hrly_const_energy.DeliveryHour - 1).zfill(2) + ':00'
                for mq in hrly_const_energy.MQ:
                    if mq.MarketQuantity == 'Total Load':
                        load_mw = mq.EnergyMW.pyval
                        if min_datetime <= self.ieso_client.utcify(local_ts_str=ts_local) <= max_datetime:
                            self.append_load(result_ts=result_ts, ts_local=ts_local, load_mw=load_mw)
        else:
            raise NotImplementedError('Predispatch Constrained Totals Report can only be parsed using load format.')

    def report_url(self, report_datetime=None):
        filename = 'PUB_PredispConstTotals.xml'
        if report_datetime is not None:
            est_datetime = report_datetime.astimezone(pytz.timezone(self.ieso_client.TZ_NAME))
            filename = est_datetime.strftime('PUB_PredispConstTotals_%Y%m%d.xml')
        return self.BASE_URL + 'PredispConstTotals/' + filename

    def latest_available_datetime(self):
        # Predispatch data for the next day is posted at approximately 15:15. Anecdotally, I've seen as late as 15:18.
        # The algorithm below uses 15:30 to be conservative.
        local_now = self.ieso_client.local_now()
        next_day_availability = copy(local_now).replace(hour=15, minute=30, second=0, microsecond=0)
        end_of_day = copy(local_now).replace(hour=23, minute=59, second=59, microsecond=999999)
        if local_now >= next_day_availability:
            return end_of_day + timedelta(days=1)
        else:
            return end_of_day

    def earliest_available_datetime(self):
        return self.ieso_client.local_now().replace(hour=23, minute=59, second=59, microsecond=0) - timedelta(days=31)

    def frequency(self):
        return BaseClient.FREQUENCY_CHOICES.hourly


class GeneratorOutputCapabilityReportHandler(BaseIesoReportHandler):
    def market(self):
        return BaseClient.MARKET_CHOICES.hourly

    def parse_report(self, xml_content, result_ts, parser_format, min_datetime, max_datetime):
        if parser_format == IESOClient.PARSER_FORMATS.generation:
            imo_document = objectify.fromstring(xml_content)
            imo_doc_body = imo_document.IMODocBody
            report_date = imo_doc_body.Date
            fuels_hourly = {key: list([]) for key in IESOClient.fuels.keys()}

            # Iterate over each hourly value for each generator, creating a dictionary keyed by fuel and values are
            # lists containing generation by hour-of-day.
            for generator in imo_doc_body.Generators.Generator:
                fuel_type = generator.FuelType
                for output in generator.Outputs.Output:
                    hour_of_day = output.Hour - 1  # Hour 1 is output from 00:00:00
                    try:
                        gen_mw = output.EnergyMW
                    except AttributeError:  # Inexplicably, some 'Output' elements are missing 'EnergyMW' child element.
                        gen_mw = 0
                    fuel_hours = fuels_hourly[fuel_type]
                    try:
                        fuel_hours[hour_of_day] += gen_mw
                    except IndexError:
                        # Assumes that hours are processed in order
                        fuel_hours.append(gen_mw)

            # Iterate over aggregated results to create generation fuel mix format
            for fuel, fuel_hours in fuels_hourly.iteritems():
                for idx, fuel_gen_mw in enumerate(fuel_hours):
                    ts_local = report_date + ' ' + str(idx).zfill(2) + ':00'
                    if min_datetime <= self.ieso_client.utcify(local_ts_str=ts_local) <= max_datetime:
                        self.append_generation(result_ts=result_ts, ts_local=ts_local, fuel=fuel, gen_mw=fuel_gen_mw)
        else:
            raise NotImplementedError('Generator Output Capability Report can only be parsed using generation format.')

    def report_url(self, report_datetime=None):
        filename = 'PUB_GenOutputCapability.xml'
        if report_datetime is not None:
            est_datetime = report_datetime.astimezone(pytz.timezone(self.ieso_client.TZ_NAME))
            filename = est_datetime.strftime('PUB_GenOutputCapability_%Y%m%d.xml')
        return self.BASE_URL + 'GenOutputCapability/' + filename

    def report_interval(self):
        return self.REPORT_INTERVALS.daily

    def earliest_available_datetime(self):
        # Earliest historical data available is three months in the past.
        return self.ieso_client.local_now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=90)

    def latest_available_datetime(self):
        return self.ieso_client.local_now()

    def frequency(self):
        return BaseClient.FREQUENCY_CHOICES.hourly


class GeneratorOutputByFuelHourlyReportHandler(BaseIesoReportHandler):
    def market(self):
        return BaseClient.MARKET_CHOICES.hourly

    def frequency(self):
        return BaseClient.FREQUENCY_CHOICES.hourly

    def report_interval(self):
        return self.REPORT_INTERVALS.yearly

    def report_url(self, report_datetime=None):
        filename = 'PUB_GenOutputbyFuelHourly.xml'
        if report_datetime is not None:
            est_datetime = report_datetime.astimezone(pytz.timezone(self.ieso_client.TZ_NAME))
            filename = est_datetime.strftime('PUB_GenOutputbyFuelHourly_%Y.xml')
        return self.BASE_URL + 'GenOutputbyFuelHourly/' + filename

    def earliest_available_datetime(self):
        return self.ieso_client.local_now().replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    def latest_available_datetime(self):
        return self.ieso_client.local_now()

    def parse_report(self, xml_content, result_ts, parser_format, min_datetime, max_datetime):
        document = objectify.fromstring(xml_content)
        doc_body = document.DocBody
        for daily_data in doc_body.DailyData:
            day = daily_data.Day
            for hourly_data in daily_data.HourlyData:
                ts_local = day + ' ' + str(hourly_data.Hour - 1).zfill(2) + ':00'
                for fuel_total in hourly_data.FuelTotal:
                    fuel = fuel_total.Fuel
                    try:
                        fuel_gen_mw = fuel_total.EnergyValue.Output
                    except AttributeError:  # When 'OutputQuality' value is -1, there is not 'Output' element.
                        fuel_gen_mw = 0
                    self.append_generation(result_ts=result_ts, ts_local=ts_local, fuel=fuel, gen_mw=fuel_gen_mw)
