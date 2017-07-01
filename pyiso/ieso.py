from copy import copy
from collections import OrderedDict

import pytz
from datetime import datetime
from datetime import timedelta
from lxml import objectify

from pyiso import LOGGER
from pyiso.base import BaseClient


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

    def handle_options(self, **kwargs):
        # regular handle options
        super(IESOClient, self).handle_options(**kwargs)

        local_now_dt = self.local_now()  # timezone aware
        local_current_day_start_dt = local_now_dt.replace(hour=0, minute=0, second=0, microsecond=0)

        if self.options.get('latest', None):
            self.options['end_at'] = local_now_dt
            self.options['start_at'] = local_now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            self.options['current_day'] = True
            self.options['historical'] = False
            self.options['forecast'] = False
        elif self.options.get('start_at', None) and self.options.get('end_at', None):
            if self.options['start_at'] < local_current_day_start_dt:
                self.options['historical'] = True
            if self.options['end_at'] >= local_current_day_start_dt:
                self.options['current_day'] = True

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        fuel_mix = list([])
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, **kwargs)
        self.options['data'] = 'gen'

        if self.options.get('latest', None):
            # TODO Latest should result in only one value.
            fuel_mix = self._day_generation_mix(dt=self.options['start_at'])
        elif self.options.get('start_at', None) and self.options.get('end_at', None):
            if self.options.get('historical', False):
                self._generation_historical(fuel_mix=fuel_mix)
            if self.options.get('current_day', False):
                fuel_mix += self._day_generation_mix(dt=self.local_now())
            if self.options.get('forecast', False):
                self._generation_forecast(ts_data=fuel_mix)
        else:
            LOGGER.warn('No valid options were supplied.')

        return fuel_mix

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        load_ts = list([])
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, **kwargs)
        self.options['data'] = 'load'

        # TODO Latest should result in only one value.
        if self.options.get('start_at', None) and self.options.get('end_at', None):
            if self.options.get('historical', False) or self.options.get('current_day', False):
                # TODO 5-minute forecasts for the remainder of current day can use Predispatch Constrained Totals.
                self._5min_demand(start_at=self.options['start_at'], end_at=self.options['end_at'], ts_data=load_ts)
            if self.options.get('forecast', False):
                # TODO If start_at/end_at contains both historical and forecast data, there is an overlap of timestamps.
                self._generation_forecast(ts_data=load_ts)
        else:
            LOGGER.warn('No valid options were supplied.')

        return load_ts

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        trade_ts = list([])
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, **kwargs)
        self.options['data'] = 'trade'

        if self.options.get('latest', None):
            LOGGER.warn('Not implemented yet.')
        elif self.options.get('start_at', None) and self.options.get('end_at', None):
            if self.options.get('historical', False):
                LOGGER.warn('Not implemented yet.')
            if self.options.get('current_day', False):
                LOGGER.warn('Not implemented yet.')
            if self.options.get('forecast', False):
                self._generation_forecast(ts_data=trade_ts)
        else:
            LOGGER.warn('No valid options were supplied.')

        return trade_ts

    def get_lmp(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        raise NotImplementedError('The IESO does not use locational marginal pricing. See '
                                  'https://www.oeb.ca/oeb/_Documents/MSP/MSP_CMSC_Report_201612.pdf for details.')

    @staticmethod
    def _adequacy_filename(local_date=None):
        """
        :param datetime local_date: An optional local date object. If provided the filename for that date will be built.
            If not, the latest report filename will be built.
        :return: Adequacy Report filename.
        :rtype: str
        """
        if local_date is not None:
            return local_date.strftime('PUB_Adequacy2_%Y%m%d.xml')
        else:
            return 'PUB_Adequacy2.xml'

    def _append_fuel_mix(self, fuel_mix, ts_local, gen_mw, fuel, market):
        """
        Conditionally appends a generation mix value to a list if it falls within the 'start_at' and 'end_at' datetime
        options.

        :param list fuel_mix: The generation fuel mix list to have a value appended.
        :param str ts_local: A local (EST) timestamp in 'yyyy-MM-dd hh:mm' format.
        :param float gen_mw: Electricity generation in megawatts (MW)
        :param str fuel: IESO fuel name (will be converted to WattTime name).
        """
        report_dt_utc = self.utcify(local_ts_str=ts_local)
        if (report_dt_utc >= self.options['start_at']) & (report_dt_utc <= self.options['end_at']):
            fuel_mix.append({
                'ba_name': self.NAME,
                'timestamp': report_dt_utc,
                'freq': self.FREQUENCY_CHOICES.hourly,
                'market': market,
                'fuel_name': self.fuels[fuel],
                'gen_MW': gen_mw
            })

    def _append_load(self, loads, ts_local, load_mw, frequency, market):
        """
        Appends a dict to the provided list, each with the keys [ba_name, timestamp, freq, market, load_MW].
        Timestamps are in UTC.
        :param list loads:
        :param str ts_local:
        :param float load_mw:
        :param str frequency:
        :param str market:
        """
        report_dt_utc = self.utcify(local_ts_str=ts_local)
        if (report_dt_utc >= self.options['start_at']) & (report_dt_utc <= self.options['end_at']):
            loads.append({
                'ba_name': self.NAME,
                'timestamp': report_dt_utc,
                'freq': frequency,
                'market': market,
                'load_MW': load_mw
            })

    def _append_trade(self, trades, imports_exports, market):
        """
        Appends a dict to the provided list, each with the keys [ba_name, timestamp, freq, market, net_exp_MW].
        Timestamps are in UTC.
        :param list trades:
        :param OrderedDict imports_exports: An ordered dictionary of the form {'ts_local':{'import'|'export',val_mw}}
        :param str market:
        """
        for ts_local, imp_exp in imports_exports.iteritems():
            report_dt_utc = self.utcify(local_ts_str=ts_local)
            if (report_dt_utc >= self.options['start_at']) & (report_dt_utc <= self.options['end_at']):
                # Handle export passed as positive/negative value
                net_exp_mw = abs(imp_exp.get('export', 0)) - abs(imp_exp.get('import', 0))
                trades.append({
                    'ba_name': self.NAME,
                    'timestamp': report_dt_utc,
                    'freq': self.FREQUENCY_CHOICES.hourly,
                    'market': market,
                    'net_exp_MW': net_exp_mw
                })

    @staticmethod
    def _realtime_constrained_totals_filename(local_date=None):
        """
        :param datetime local_date: An optional local date object. If provided the filename for that hour will be built.
            If not, the latest report filename will be built.
        :return: Realtime Constrained Totals Report filename.
        :rtype: str
        """
        if local_date is not None:
            delivery_hour = local_date.hour + 1
            return local_date.strftime('PUB_RealtimeConstTotals_%Y%m%d' + str(delivery_hour).zfill(2) + '.xml')
        else:
            return 'PUB_RealtimeConstTotals.xml'

    def _generation_forecast(self, ts_data):
        """
        Iterate over calls to generator forecast reports for hours prior to the end_at date or until no more forecast
        information is available.

        :param list ts_data: The timeseries list of dicts to append results to.
        """
        local_last_forecast_dt = self.local_now().replace(hour=23, minute=59, second=59, microsecond=999999) \
            + timedelta(days=1)  # Last possible forecast is the end of the next day.
        iter_dt = copy(self.local_now())
        while iter_dt <= self.options['end_at'].astimezone(pytz.timezone(self.TZ_NAME)) \
                and iter_dt <= local_last_forecast_dt:
            ts_data += self._forecast_generation_mix(dt=iter_dt)
            iter_dt = iter_dt + timedelta(days=1)

    def _generation_historical(self, fuel_mix):
        """
        Iterate over calls to historical generator output reports for hours in the range start_at to end_at.

        :param list fuel_mix: The list of dicts to append results to.
        """
        iter_dt = copy(self.options['start_at'])
        while iter_dt.astimezone(pytz.timezone(self.TZ_NAME)).year <= \
                self.options['end_at'].astimezone(pytz.timezone(self.TZ_NAME)).year:
            if self._is_in_generation_mix_time_range(iter_dt):
                fuel_mix += self._year_generation_mix(local_year=iter_dt.astimezone(pytz.timezone(self.TZ_NAME)).year)
            iter_dt = iter_dt.replace(year=iter_dt.year + 1)

    @staticmethod
    def _output_capability_filename(local_date=None):
        """
        :param datetime local_date: An optional local date object. If provided the filename for that date will be built.
            If not, the latest report filename will be built.
        :return: Generator Output and Capability Report filename.
        :rtype: str
        """
        if local_date is not None:
            return local_date.strftime('PUB_GenOutputCapability_%Y%m%d.xml')
        else:
            return 'PUB_GenOutputCapability.xml'

    @staticmethod
    def _output_by_fuel_filename(local_year=None):
        """
        :param int local_year: An optional local year value. If provided the filename for that date will be built.
            If not, the latest report filename will be built.
        :return: Generator Output by Fuel Type Hourly Report filename.
        :rtype: str
        """
        if local_year is not None:
            return 'PUB_GenOutputbyFuelHourly_' + str(local_year) + '.xml'
        else:
            return 'PUB_GenOutputbyFuelHourly.xml'

    def _parse_fuel_mix_from_adequacy_report(self, xml_content):
        """
        Parse generation and fuel type information from forecast data contained in the Adequacy Report.

        :param str xml_content: The XML content of the Adequacy Report.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, load_MW]``. Timestamps are in UTC.
        :rtype: list
        """
        document = objectify.fromstring(xml_content)
        doc_body = document.DocBody

        fuel_mix = list([])
        day = doc_body.DeliveryDate
        # InternalResources is misleading. Each fuel is an internal resource, and we iterate hours of each fuel.
        for internal_resource in doc_body.ForecastSupply.InternalResources.InternalResource:
            fuel = str.upper(internal_resource.FuelType.text)
            if fuel != 'DISPATCHABLE LOAD':  # TODO What to do about dispatchable load? Skipping for now.
                for schedule in internal_resource.Schedules.Schedule:
                    ts_local = day + ' ' + str(schedule.DeliveryHour - 1).zfill(2) + ':00'
                    fuel_gen_mw = schedule.EnergyMW.pyval
                    self._append_fuel_mix(fuel_mix=fuel_mix, ts_local=ts_local, fuel=fuel, gen_mw=fuel_gen_mw,
                                          market=self.MARKET_CHOICES.dam)
        return fuel_mix

    def _parse_load_from_adequacy_report(self, xml_content):
        """
        Parse load data from forecast data contained in the Adequacy Report.
        :param xml_content: The XML content of the Adequacy Report.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, gen_MW]``.
        """
        document = objectify.fromstring(xml_content)
        doc_body = document.DocBody

        loads = list([])
        day = doc_body.DeliveryDate
        for demand in doc_body.ForecastDemand.OntarioDemand.ForecastOntDemand.Demand:
            ts_local = day + ' ' + str(demand.DeliveryHour - 1).zfill(2) + ':00'
            load_mw = demand.EnergyMW.pyval
            self._append_load(loads=loads, ts_local=ts_local, load_mw=load_mw,
                              frequency=self.FREQUENCY_CHOICES.hourly, market=self.MARKET_CHOICES.dam)
        return loads

    def _parse_output_capability_report(self, xml_content):
        """
        Parse the Generator Output and Capability Report, aggregating output hourly by fuel type.

        :param str xml_content: The XML content of the Generator Output and Capability Report.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, fuel_name, gen_MW]``.
            Timestamps are in UTC.
        :rtype: list
        """
        imo_document = objectify.fromstring(xml_content)
        imo_doc_body = imo_document.IMODocBody
        report_date = imo_doc_body.Date
        fuels_hourly = {key: list([]) for key in self.fuels.keys()}

        # Iterate over each hourly value for each generator, creating a dictionary keyed by fuel and values are lists
        # containing generation by hour-of-day.
        for generator in imo_doc_body.Generators.Generator:
            fuel_type = generator.FuelType
            for output in generator.Outputs.Output:
                hour_of_day = output.Hour - 1  # Hour 1 is output from 00:00:00 to 00:59:59
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
        fuel_mix = list([])
        for fuel in fuels_hourly.keys():
            fuel_hourly = fuels_hourly[fuel]
            if self.options.get('latest'):
                idx = len(fuel_hourly) - 1
                latest_fuel_gen_mw = fuel_hourly[idx]
                report_ts_local = report_date + ' ' + str(idx).zfill(2) + ':00'
                self._append_fuel_mix(fuel_mix=fuel_mix, ts_local=report_ts_local, fuel=fuel, gen_mw=latest_fuel_gen_mw,
                                      market=self.MARKET_CHOICES.hourly)
            else:
                for idx, fuel_gen_mw in enumerate(fuels_hourly[fuel]):
                    report_ts_local = report_date + ' ' + str(idx).zfill(2) + ':00'
                    self._append_fuel_mix(fuel_mix=fuel_mix, ts_local=report_ts_local, fuel=fuel, gen_mw=fuel_gen_mw,
                                          market=self.MARKET_CHOICES.hourly)
        return fuel_mix

    def _parse_realtime_constrained_totals_report(self, xml_content):
        """
        Parse the Realtime Constrained Totals Report.

        :param str xml_content: The XML content of the Realtime Constrained Totals Report.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, gen_MW]``. Timestamps are in UTC.
        :rtype: list
        """
        document = objectify.fromstring(xml_content)
        doc_body = document.DocBody

        loads = list([])
        day = doc_body.DeliveryDate
        hour = doc_body.DeliveryHour - 1
        # TODO Ask maintainers, do we parse total demand or total load (i.e. demand + losses)?
        # Currently, using ONTARIO DEMAND aligns with the "Forcast Demand, Total Requirement" from the Adequacy Report.
        for interval_energy in doc_body.Energies.IntervalEnergy:
            minute = (interval_energy.Interval - 1) * 5
            ts_local = day + ' ' + str(hour).zfill(2) + ':' + (str(minute).zfill(2))
            for mq in interval_energy.MQ:
                if mq.MarketQuantity == 'ONTARIO DEMAND':
                    load_mw = mq.EnergyMW.pyval
                    self._append_load(loads=loads, ts_local=ts_local, load_mw=load_mw,
                                      frequency=self.FREQUENCY_CHOICES.fivemin, market=self.MARKET_CHOICES.fivemin)
        return loads

    def _parse_output_by_fuel_report(self, xml_content):
        """
        Parse the Generator Output by Fuel Type Hourly Report.

        :param str xml_content: The XML content of the Generator Output by Fuel Type Hourly Report.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, fuel_name, gen_MW]``.
            Timestamps are in UTC.
        :rtype: list
        """
        document = objectify.fromstring(xml_content)
        doc_body = document.DocBody

        fuel_mix = list([])
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
                    self._append_fuel_mix(fuel_mix=fuel_mix, ts_local=ts_local, fuel=fuel, gen_mw=fuel_gen_mw,
                                          market=self.MARKET_CHOICES.hourly)

        return fuel_mix

    def _parse_trade_from_adequacy_report(self, xml_content):
        """
        Parse import/export data from forecast data contained in the Adequacy Report.
        :param xml_content: The XML content of the Adequacy Report.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, net_exp_MW]``.
        """
        document = objectify.fromstring(xml_content)
        doc_body = document.DocBody

        trades = list([])
        day = doc_body.DeliveryDate
        imports_exports = OrderedDict()  # {'ts_local':{'import'|'export',val_mw}}
        for import_schedule in doc_body.ForecastSupply.ZonalImports.TotalImports.Schedules.Schedule:
            ts_local = day + ' ' + str(import_schedule.DeliveryHour - 1).zfill(2) + ':00'
            imports_exports[ts_local] = {'import': import_schedule.EnergyMW.pyval}
        for export_schedule in doc_body.ForecastDemand.ZonalExports.TotalExports.Schedules.Schedule:
            ts_local = day + ' ' + str(export_schedule.DeliveryHour - 1).zfill(2) + ':00'
            hr_entry = imports_exports.get(ts_local)
            hr_entry.update({'export': export_schedule.EnergyMW.pyval})
            imports_exports[ts_local] = hr_entry

        self._append_trade(trades=trades, imports_exports=imports_exports, market=self.MARKET_CHOICES.dam)
        return trades

    def _is_in_generation_mix_time_range(self, dt):
        """
        :param datetime dt: A timezone-aware datetime value.
        :return: Whether a datetime falls within the reporting time range for generation fuel mix, the current year or
            previous year.
        :rtype: bool
        """
        return dt.astimezone(pytz.timezone(self.TZ_NAME)).year >= self.local_now().year - 1

    def _day_generation_mix(self, dt):
        """
        Request the generation fuel mix for a single day. Assumes local options variable has been updated.

        :param datetime dt: A timezone-aware datetime value.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, fuel_name, gen_MW]``.
            Timestamps are in UTC. Trimmed to fall between start_at and end_at options.
        :rtype: list
        """
        base_output_capability_url = self.base_url + 'GenOutputCapability/'
        filename = self._output_capability_filename(local_date=dt.astimezone(pytz.timezone(self.TZ_NAME)))
        response = self.request(url=base_output_capability_url + filename)
        return self._parse_output_capability_report(response.content)

    def _5min_demand(self, start_at, end_at, ts_data):
        """
        Request the five-minute demand the time range start_at to end_at.

        :param datetime start_at:
        :param datetime end_at:
        :param list ts_data: The timeseries list of dicts to append results to, each with keys
            ``[ba_name, timestamp, freq, market, gen_MW]``. Timestamps are in UTC. Only values falling between start_at
            and end_at will be appended.
        """
        earliest_historical_dt = self.local_now().replace(hour=0, minute=0, second=0, microsecond=0) \
            - timedelta(days=31)  # Earliest historical data available is 31 days in the past.
        iter_dt = copy(start_at)
        while earliest_historical_dt < iter_dt <= self.local_now() and iter_dt <= end_at:
            base_rt_constrained_totals_url = self.base_url + 'RealtimeConstTotals/'
            filename = self._realtime_constrained_totals_filename(
                local_date=iter_dt.astimezone(pytz.timezone(self.TZ_NAME)))
            response = self.request(url=base_rt_constrained_totals_url + filename)
            ts_data += self._parse_realtime_constrained_totals_report(response.content)
            iter_dt += timedelta(hours=1)

    def _forecast_generation_mix(self, dt):
        """
        For generation mix times in the future, the Adequacy Report must be parsed for forecast values.

        :param datetime dt: The datetime (local) of the Adequacy Report to parse.
        :return: List of dicts, trimmed to fall between start_at and end_at options. The keys in dict elements vary
            depending on request type.
        :rtype: list
        """
        base_adequacy_url = self.base_url + 'Adequacy2/'
        filename = self._adequacy_filename(local_date=dt.astimezone(pytz.timezone(self.TZ_NAME)))
        response = self.request(url=base_adequacy_url + filename)
        if self.options['data'] == 'gen':
            return self._parse_fuel_mix_from_adequacy_report(response.content)
        elif self.options['data'] == 'load':
            return self._parse_load_from_adequacy_report(response.content)
        elif self.options['data'] == 'trade':
            return self._parse_trade_from_adequacy_report(response.content)
        else:
            LOGGER.warn('Return data type is unknown, skipping Adequacy Report parsing')

    def _year_generation_mix(self, local_year):
        """
        For long generation mix time ranges at least one day in the past, it is most efficient to request the Generator
        Output by Fuel Type Hourly Report summary rather than the detailed Generator Output and Capability Report.

        :param local_year: The year (local) of the Generator Output and Capability Report to parse.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, fuel_name, gen_MW]``.
            Timestamps are in UTC. Trimmed to fall between start_at and end_at options.
        :rtype: list
        """
        base_output_by_fuel_url = self.base_url + 'GenOutputbyFuelHourly/'
        filename = self._output_by_fuel_filename(local_year=local_year)
        response = self.request(url=base_output_by_fuel_url + filename)
        return self._parse_output_by_fuel_report(response.content)


def main():
    client = IESOClient()
    local_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('EST'))
    start_at = local_now - timedelta(hours=10)
    end_at = local_now + timedelta(days=1)
    client.get_load(start_at=start_at, end_at=end_at)

if __name__ == '__main__':
    main()
