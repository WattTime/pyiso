from copy import copy
from datetime import datetime

import pytz
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
        'BIOFUEL': 'biomass'
    }

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        fuel_mix = list([])
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, **kwargs)
        local_now_dt = self.local_now()  # timezone aware
        if latest:
            self.options['end_at'] = local_now_dt
            self.options['start_at'] = local_now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            fuel_mix = self._day_generation_mix(dt=self.options['start_at'])
        elif yesterday:
            fuel_mix = self._day_generation_mix(dt=self.options['start_at'])
        elif (start_at != False) & (end_at != False) & (self._is_in_generation_mix_time_range(self.options['end_at'])):
            # using options values in all cases, known to be timezone aware and in UTC
            if local_now_dt < self.options['start_at']:  # forecast only
                raise NotImplementedError('Generation forecast not currently implemented')
            elif local_now_dt < self.options['end_at']:  # history with forecast
                raise NotImplementedError('Generation forecast not currently implemented')
            elif self._is_same_day_local(self.options['start_at'], self.options['end_at']):  # same day is only one call
                fuel_mix = self._day_generation_mix(dt=self.options['start_at'])
            else:  # not a forecast and not the same day, guaranteed to at least be 2nd day of the year
                iter_dt = copy(self.options['start_at'])
                while iter_dt.astimezone(pytz.timezone(self.TZ_NAME)).year <= \
                        self.options['end_at'].astimezone(pytz.timezone(self.TZ_NAME)).year:
                    if self._is_in_generation_mix_time_range(iter_dt):
                        fuel_mix += self._year_generation_mix(local_year=
                                                              iter_dt.astimezone(pytz.timezone(self.TZ_NAME)).year)
                    iter_dt = iter_dt.replace(year=iter_dt.year + 1)
                if self._is_same_day_local(local_now_dt, self.options['end_at']):
                    # end_at is in the current day, which requires requesting the current output capability report
                    fuel_mix = self._day_generation_mix(dt=self.options['end_at'])
        elif (end_at != False) & (self._is_in_generation_mix_time_range(self.options['end_at']) == False):
            LOGGER.warn('Generator Output and Capability Report can only be requested up to one year in the past.')
        else:
            LOGGER.warn('No valid options were supplied.')
        return fuel_mix

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

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
                self._append_fuel_mix(fuel_mix=fuel_mix, ts_local=report_ts_local, fuel=fuel, gen_mw=latest_fuel_gen_mw)
            else:
                for idx, fuel_gen_mw in enumerate(fuels_hourly[fuel]):
                    report_ts_local = report_date + ' ' + str(idx).zfill(2) + ':00'
                    self._append_fuel_mix(fuel_mix=fuel_mix, ts_local=report_ts_local, fuel=fuel, gen_mw=fuel_gen_mw)
        return fuel_mix

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
                    fuel_gen_mw = fuel_total.EnergyValue.Output
                    self._append_fuel_mix(fuel_mix=fuel_mix, ts_local=ts_local, fuel=fuel, gen_mw=fuel_gen_mw)

        return fuel_mix

    def _append_fuel_mix(self, fuel_mix, ts_local, gen_mw, fuel):
        """
        Conditionally appends a generation mix value to a list if it falls within the 'start_at' and 'end_at' datetime
        options.

        :param list fuel_mix: The generation fuel mix list to have a value appended.
        :param str ts_local: A local (EST) timestamp in 'yyyy-MM-dd hh:mm' format.
        :param float gen_mw: Electricity generation in megawatts (MW)
        :param string fuel: IESO fuel name (will be converted to WattTime name).
        """
        report_dt_utc = self.utcify(local_ts_str=ts_local)
        if (report_dt_utc >= self.options['start_at']) & (report_dt_utc <= self.options['end_at']):
            fuel_mix.append({
                'ba_name': self.NAME,
                'timestamp': report_dt_utc,
                'freq': self.FREQUENCY_CHOICES.hourly,
                'market': self.MARKET_CHOICES.hourly,
                'fuel_name': self.fuels[fuel],
                'gen_MW': gen_mw
            })

    def _is_same_day_local(self, dt1, dt2):
        """
        :param datetime dt1: A timezone-aware datetime value.
        :param datetime dt2: A timezone-aware datetime value.
        :return: Whether or not the two datetimes fall on the same day when converted to the local timezone.
        :rtype: bool
        """
        return dt1.astimezone(pytz.timezone(self.TZ_NAME)).date() == dt2.astimezone(pytz.timezone(self.TZ_NAME)).date()

    def _is_in_generation_mix_time_range(self, dt):
        """
        :param datetime dt: A timezone-aware datetime value.
        :return: Whether a datetime falls within the reporting time range for generation fuel mix, the curren year or
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

    def _year_generation_mix(self, local_year):
        """
        For long generation mix time ranges at least one day in the past, it is most efficient to request the Generator
        Output by Fuel Type Hourly Report summary rather than the detailed Generator Output and Capability Report.

        :param local_year:
        :return:
        """
        base_output_by_fuel_url = self.base_url + 'GenOutputbyFuelHourly/'
        filename = self._output_by_fuel_filename(local_year=local_year)
        response = self.request(url=base_output_by_fuel_url + filename)
        return self._parse_output_by_fuel_report(response.content)

# def main():
#     client = IESOClient()
#     client.get_generation(start_at=client.utcify(local_ts_str='2016-02-03 15:00:00'),
#                           end_at=client.utcify(local_ts_str='2016-03-15 01:00:00'))
#
# if __name__ == '__main__':
#     main()
