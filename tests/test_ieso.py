from unittest import TestCase

from pyiso import client_factory


class TestIESO(TestCase):
    def setUp(self):
        self.c = client_factory('IESO')

    def test_parse_output_capability_report_for_complete_day(self):
        # Open a simplified, offline copy of complete April 29, 2016 report
        xml = open('./resources/ieso/reduced_GenOutputCapability_20160429.xml')

        fuels_hourly = self.c.parse_output_capability_report(xml.read())

        self.assertEquals(len(fuels_hourly['NUCLEAR']), 24)
        self.assertEquals(fuels_hourly['NUCLEAR'][0], 1250)
        self.assertEquals(fuels_hourly['NUCLEAR'][23], 1555)
        self.assertEquals(len(fuels_hourly['WIND']), 24)
        self.assertEquals(fuels_hourly['WIND'][0], 59)
        self.assertEquals(fuels_hourly['WIND'][23], 26)
        self.assertEquals(len(fuels_hourly['BIOFUEL']), 24)
        self.assertEquals(fuels_hourly['BIOFUEL'][0], 0)
        self.assertEquals(fuels_hourly['BIOFUEL'][23], 0)

    def test_parse_output_capability_report_for_partial_day(self):
        # Open a simplified, offline copy of partial May 1, 2016 report
        xml = open('./resources/ieso/reduced_GenOutputCapability_20160501.xml')

        fuels_hourly = self.c.parse_output_capability_report(xml.read())

        self.assertEquals(len(fuels_hourly['NUCLEAR']), 4)
        self.assertEquals(fuels_hourly['NUCLEAR'][0], 716)
        self.assertEquals(fuels_hourly['NUCLEAR'][3], 678)
        self.assertEquals(len(fuels_hourly['GAS']), 4)
        self.assertEquals(fuels_hourly['GAS'][0], 120)
        self.assertEquals(fuels_hourly['GAS'][3], 120)
        self.assertEquals(len(fuels_hourly['HYDRO']), 4)
        self.assertEquals(fuels_hourly['HYDRO'][0], 165)
        self.assertEquals(fuels_hourly['HYDRO'][3], 165)