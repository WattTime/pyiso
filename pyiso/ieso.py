from datetime import datetime

from lxml import objectify

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
        self.handle_options(latest=latest, yesterday=yesterday, start_at=start_at, end_at=end_at, **kwargs)

        if self.options.get('latest'):
            filename = self._output_capability_filename()
        elif self.options.get('yesterday'):
            yesterday_date = self.options.get('start_at').date()
            filename = self._output_capability_filename(local_date=yesterday_date)
        else:
            raise NotImplementedError('Only the latest generation fuel mix data is currently implemented.')

        output_capability_url = self.base_url + 'GenOutputCapability/' + filename
        response = self.request(url=output_capability_url)
        fuel_mix = self._parse_output_capability_report(response.content, latest=latest)
        return fuel_mix

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    @staticmethod
    def _output_capability_filename(local_date=None):
        """
        Builds a filename usable for requesting a day's Generator Output and Capability Report.

        :param datetime local_date: An optional local date object. If provided the filename for that date will be built.
            If not, the latest report will be built.
        :return: Generator Output and Capability Report filename.
        :rtype: str
        """
        if local_date is not None:
            return local_date.strftime('PUB_GenOutputCapability_%Y%m%d.xml')
        else:
            return 'PUB_GenOutputCapability.xml'

    def _parse_output_capability_report(self, xml_content, latest=False):
        """
        Parse the Generator Output and Capability Report, aggregating output hourly by fuel type.

        :param str xml_content: The XML content of the Generator Output and Capability Report.
        :param bool latest: Indicates whether the returned fule mix should be trimmed to only contain the latest values.
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
            if latest:
                idx = len(fuel_hourly) - 1
                latest_fuel_gen_mw = fuel_hourly[idx]
                report_ts_local = report_date + ' ' + str(idx).zfill(2) + ':00'
                self._append_fuel_mix(fuel_mix=fuel_mix, ts_local=report_ts_local, fuel=fuel, gen_mw=latest_fuel_gen_mw)
            else:
                for idx, fuel_gen_mw in enumerate(fuels_hourly[fuel]):
                    report_ts_local = report_date + ' ' + str(idx).zfill(2) + ':00'
                    self._append_fuel_mix(fuel_mix=fuel_mix, ts_local=report_ts_local, fuel=fuel, gen_mw=fuel_gen_mw)

        return fuel_mix

    def _append_fuel_mix(self, fuel_mix, ts_local, gen_mw, fuel):
        """
        Parse the Generator Output and Capability Report, aggregating output hourly by fuel type.

        :param list fuel_mix: The generation fuel mix list to have a value appended.
        :param str ts_local: A local (EST) timestamp in 'yyyy-MM-dd hh:mm' format.
        :param float gen_mw: Electricity generation in megawatts (MW)
        :param string fuel: IESO fuel name (will be converted to WattTime name).
        """
        report_ts_utc = self.utcify(local_ts_str=ts_local)
        fuel_mix.append({
            'ba_name': self.NAME,
            'timestamp': report_ts_utc,
            'freq': self.FREQUENCY_CHOICES.hourly,
            'market': self.MARKET_CHOICES.hourly,
            'fuel_name': self.fuels[fuel],
            'gen_MW': gen_mw
        })


# def main():
#     client = IESOClient()
#     client.get_generation(yesterday=True)
#
# if __name__ == '__main__':
#     main()
