from lxml import objectify

from pyiso.base import BaseClient


class IESOClient(BaseClient):
    NAME = 'IESO'
    TZ_NAME = 'America/Toronto'

    base_url = 'http://reports.ieso.ca/public/'
    output_capability_url = base_url + 'GenOutputCapability/'
    output_capability_latest_url = output_capability_url + 'PUB_GenOutputCapability.xml'

    # In Ontario, references to SOLAR are all solar PV.
    fuels = {
        'NUCLEAR': 'nuclear',
        'GAS': 'natgas',
        'HYDRO': 'hydro',
        'WIND': 'wind',
        'SOLAR': 'solarpv',
        'BIOFUEL': 'biomass'
    }

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        if latest:
            response = self.request(url=self.output_capability_latest_url)
            fuel_mix = self.parse_output_capability_report(response.content)
            return fuel_mix
        else:
            raise NotImplementedError('Only the latest generation fuel mix data is currently implemented.')

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def parse_output_capability_report(self, xml_content):
        """
        Parse the Generator Output and Capability Report, aggregating output hourly by fuel type.

        :param str xml_content: An XML string of the Generator Output and Capability Report.
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
            for idx, fuel_gen_mw in enumerate(fuels_hourly[fuel]):
                report_ts_local = report_date + ' ' + str(idx).zfill(2) + ':00'
                report_ts_utc = self.utcify(local_ts_str=report_ts_local, is_dst=False)
                fuel_mix.append({
                    'ba_name': self.NAME,
                    'timestamp': report_ts_utc,
                    'freq': self.FREQUENCY_CHOICES.hourly,
                    'market': self.MARKET_CHOICES.hourly,
                    'fuel_name': self.fuels[fuel],
                    'gen_MW': fuel_gen_mw
                })

        return fuel_mix


# def main():
#     client = IESOClient()
#     client.get_generation(latest=True)
#
# if __name__ == '__main__':
#     main()