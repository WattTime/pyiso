from lxml import objectify

from pyiso.base import BaseClient


class IESOClient(BaseClient):
    NAME = 'IESO'
    TZ_NAME = 'America/Toronto'

    base_url = 'http://reports.ieso.ca/public/'

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
        pass

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def parse_output_capability_report(self, xml_content):
        """
        Parse the Generators Output and Capability report, aggregating output hourly by fuel type.

        :param str xml_content: An XML string of the IESO Output Capability report.
        :return: Dict keyed by fuel and values are generation by hour-of-day
        :rtype: dict
        """
        imo_document = objectify.fromstring(xml_content)
        imo_doc_body = imo_document.IMODocBody
        fuels_hourly = {key: list([]) for key in self.fuels.keys()}

        # Iterate over each hourly value for each generator
        for generator in imo_doc_body.Generators.Generator:
            fuel_type = generator.FuelType
            for output in generator.Outputs.Output:
                hour_of_day = output.Hour - 1 # Hour 1 is output from 00:00:00 to 00:59:59
                gen_mw = output.EnergyMW
                fuel_hours = fuels_hourly[fuel_type]
                try:
                    fuel_hours[hour_of_day] += gen_mw
                except IndexError:
                    # Assumes that hours or processed in order
                    fuel_hours.append(gen_mw)

        return fuels_hourly
