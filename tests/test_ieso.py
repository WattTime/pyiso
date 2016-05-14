from datetime import datetime
from unittest import TestCase

from pytz import timezone

from pyiso import client_factory


class TestIESO(TestCase):
    def setUp(self):
        self.c = client_factory('IESO')

    def test_parse_output_capability_report_for_complete_day(self):
        start_at = datetime(year=2016, month=4, day=29, hour=0, minute=0, second=0, tzinfo=timezone(self.c.TZ_NAME))
        end_at = datetime(year=2016, month=4, day=29, hour=23, minute=59, second=59, tzinfo=timezone(self.c.TZ_NAME))
        self.c.handle_options(start_at=start_at, end_at=end_at)
        # Simplified, offline copy of complete April 29, 2016 report
        xml = open('./resources/ieso/reduced_GenOutputCapability_20160429.xml')

        fuel_mix = self.c._parse_output_capability_report(xml.read())

        self.assertEquals(len(fuel_mix), 72)  # 24 hours of three fuels
        for val in fuel_mix:  # Spot check known values
            if (val['fuel_name'] == 'nuclear') & (val['timestamp'].day == 29) & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 1250)
            elif (val['fuel_name'] == 'nuclear') & (val['timestamp'].day == 30) & (val['timestamp'].hour == 4):
                self.assertEquals(val['gen_MW'], 1555)
            elif (val['fuel_name'] == 'wind') & (val['timestamp'].day == 29) & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 59)
            elif (val['fuel_name'] == 'wind') & (val['timestamp'].day == 30) & (val['timestamp'].hour == 4):
                self.assertEquals(val['gen_MW'], 26)
            elif (val['fuel_name'] == 'biomass') & (val['timestamp'].day == 29) & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 0)
            elif (val['fuel_name'] == 'biomass') & (val['timestamp'].day == 30) & (val['timestamp'].hour == 4):
                self.assertEquals(val['gen_MW'], 0)

    def test_parse_output_capability_report_for_partial_day(self):
        start_at = datetime(year=2016, month=5, day=1, hour=0, minute=0, second=0, tzinfo=timezone(self.c.TZ_NAME))
        end_at = datetime(year=2016, month=5, day=1, hour=23, minute=59, second=59, tzinfo=timezone(self.c.TZ_NAME))
        self.c.handle_options(start_at=start_at, end_at=end_at)
        # Simplified, offline copy of partial May 1, 2016 report
        xml = open('./resources/ieso/reduced_GenOutputCapability_20160501.xml')

        fuel_mix = self.c._parse_output_capability_report(xml.read())

        self.assertEquals(len(fuel_mix), 12)  # 4 hours of three fuels
        for val in fuel_mix:  # Spot check known values
            if (val['fuel_name'] == 'nuclear') & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 716)
            elif (val['fuel_name'] == 'nuclear') & (val['timestamp'].hour == 8):
                self.assertEquals(val['gen_MW'], 678)
            elif (val['fuel_name'] == 'natgas') & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 120)
            elif (val['fuel_name'] == 'natgas') & (val['timestamp'].hour == 8):
                self.assertEquals(val['gen_MW'], 120)
            elif (val['fuel_name'] == 'hydro') & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 165)
            elif (val['fuel_name'] == 'hydro') & (val['timestamp'].hour == 8):
                self.assertEquals(val['gen_MW'], 165)
