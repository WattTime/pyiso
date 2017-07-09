from pyiso import ieso
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

        # Reduced, offline copy of complete April 29, 2016 report
        xml = open('./fixtures/ieso_reduced_GenOutputCapability_20160429.xml')
        fuel_mix = self.c._parse_output_capability_report(xml.read())

        self.assertEquals(len(fuel_mix), 72)  # 24 hours of three fuels
        for val in fuel_mix:  # Spot check fuel summations using known values
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

        # Reduced, offline copy of partial May 1, 2016 report
        xml = open('./fixtures/ieso_reduced_GenOutputCapability_20160501.xml')
        fuel_mix = self.c._parse_output_capability_report(xml.read())

        self.assertEquals(len(fuel_mix), 12)  # 4 hours of three fuels
        for val in fuel_mix:  # Spot check fuel summations using known values
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

    def test_parse_output_by_fuel_report(self):
        start_at = datetime(year=2016, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone(self.c.TZ_NAME))
        end_at = datetime(year=2016, month=1, day=7, hour=23, minute=59, second=59, tzinfo=timezone(self.c.TZ_NAME))
        self.c.handle_options(start_at=start_at, end_at=end_at)

        # Offline copy of 2016 report, as if it were requested on January 8th.
        xml = open('./fixtures/ieso_reduced_GenOutputbyFuelHourly_2016.xml')
        fuel_mix = self.c._parse_output_by_fuel_report(xml.read())

        self.assertEquals(len(fuel_mix), 1008)  # 6 fuels * 24 hours * 7 days
        for val in fuel_mix:  # Spot check known values
            if (val['fuel_name'] == 'nuclear') & (val['timestamp'].day == 1) & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 11188)
            elif (val['fuel_name'] == 'biomass') & (val['timestamp'].day == 2) & (val['timestamp'].hour == 4):
                self.assertEquals(val['gen_MW'], 2)
            elif (val['fuel_name'] == 'wind') & (val['timestamp'].day == 7) & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 809)
            elif (val['fuel_name'] == 'hydro') & (val['timestamp'].day == 8) & (val['timestamp'].hour == 4):
                self.assertEquals(val['gen_MW'], 4749)


class TestIntertieScheduleFlowReport(TestCase):
    def setUp(self):
        self.report_handler = ieso.IntertieScheduleFlowReportHandler(ieso_client=client_factory('IESO'))

    def test_parse_report(self):
        start_at = datetime(year=2017, month=6, day=30, hour=0, minute=0, second=0,
                            tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        end_at = datetime(year=2017, month=6, day=30, hour=23, minute=59, second=59,
                          tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        # Offline copy of June 30, 2017 report requested as if it were July 1st.
        xml = open('./fixtures/ieso_full_IntertieScheduleFlow_20170630.xml')
        trades = list([])

        self.report_handler.parse_report(xml_content=xml.read(), result_ts=trades,
                                         parser_format=ieso.IESOClient.PARSER_FORMATS.trade,
                                         min_datetime=start_at, max_datetime=end_at)

        self.assertEquals(len(trades), 288)  # 12 five-minute intervals * 24 hours.
        # Spot check fuel summations using known values
        self.assertEquals(trades[0]['net_exp_MW'], 2269.4)
        self.assertEquals(trades[287]['net_exp_MW'], 2242)


class TestAdequacyReport(TestCase):
    def setUp(self):
        self.report_handler = ieso.AdequacyReportHandler(ieso_client=client_factory('IESO'))

    def test_parse_report_for_trade(self):
        start_at = datetime(year=2017, month=6, day=18, hour=0, minute=0, second=0,
                            tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        end_at = datetime(year=2017, month=6, day=18, hour=23, minute=59, second=59,
                          tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        xml = open('./fixtures/ieso_full_Adequacy2_20170618.xml')
        trades = list([])

        self.report_handler.parse_report(xml_content=xml.read(), result_ts=trades,
                                         parser_format=ieso.IESOClient.PARSER_FORMATS.trade,
                                         min_datetime=start_at, max_datetime=end_at)

        self.assertEquals(len(trades), 24)  # 24 hours
        # Spot check net exports using known values
        self.assertEquals(trades[0]['net_exp_MW'], 2383)
        self.assertEquals(trades[23]['net_exp_MW'], 2051)

    def test_parse_report_for_fuel(self):
        start_at = datetime(year=2017, month=6, day=18, hour=0, minute=0, second=0,
                            tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        end_at = datetime(year=2017, month=6, day=18, hour=23, minute=59, second=59,
                              tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        xml = open('./fixtures/ieso_full_Adequacy2_20170618.xml')
        fuels = list([])

        self.report_handler.parse_report(xml_content=xml.read(), result_ts=fuels,
                                         parser_format=ieso.IESOClient.PARSER_FORMATS.generation,
                                         min_datetime=start_at, max_datetime=end_at)

        self.assertEquals(len(fuels), 168)  # 7 fuels * 24 hours
        for val in fuels:  # Spot check fuel summations using known values
            if (val['fuel_name'] == 'nuclear') & (val['timestamp'].day == 18) & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 11130)
            elif (val['fuel_name'] == 'wind') & (val['timestamp'].day == 18) & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 744)
            elif (val['fuel_name'] == 'biomass') & (val['timestamp'].day == 19) & (val['timestamp'].hour == 4):
                self.assertEquals(val['gen_MW'], 0)
            elif (val['fuel_name'] == 'hydro') & (val['timestamp'].day == 19) & (val['timestamp'].hour == 4):
                self.assertEquals(val['gen_MW'], 3570)

    def test_parse_report_for_load(self):
        start_at = datetime(year=2017, month=6, day=18, hour=0, minute=0, second=0,
                            tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        end_at = datetime(year=2017, month=6, day=18, hour=23, minute=59, second=59,
                          tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        xml = open('./fixtures/ieso_full_Adequacy2_20170618.xml')
        loads = list([])

        self.report_handler.parse_report(xml_content=xml.read(), result_ts=loads,
                                         parser_format=ieso.IESOClient.PARSER_FORMATS.load,
                                         min_datetime=start_at, max_datetime=end_at)

        self.assertEquals(len(loads), 24)  # 24 hours
        # Spot check loads using known values
        self.assertEquals(loads[0]['load_MW'], 13266)
        self.assertEquals(loads[23]['load_MW'], 14280)


class TestRealtimeConstrainedTotalsReport(TestCase):
    def setUp(self):
        self.report_handler = ieso.RealTimeConstrainedTotalsReportHandler(ieso_client=client_factory('IESO'))

    def test_parse_report(self):
        start_at = datetime(year=2017, month=7, day=1, hour=0, minute=0, second=0,
                            tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        end_at = datetime(year=2017, month=7, day=1, hour=0, minute=59, second=59,
                          tzinfo=timezone(ieso.IESOClient.TZ_NAME))

        # Offline copy of July 1, 2017 report for delivery hour #1.
        xml = open('./fixtures/ieso_full_RealtimeConstTotals_2017070101.xml')
        loads = list([])

        self.report_handler.parse_report(xml_content=xml.read(), result_ts=loads,
                                         parser_format=ieso.IESOClient.PARSER_FORMATS.load,
                                         min_datetime=start_at, max_datetime=end_at)

        self.assertEquals(len(loads), 12)  # 12 five-minute intervals in 1 hour.
        # Spot check fuel summations using known values
        self.assertEquals(loads[0]['load_MW'], 13531.9)
        self.assertEquals(loads[1]['load_MW'], 13629.1)
        self.assertEquals(loads[2]['load_MW'], 13520.8)
        self.assertEquals(loads[3]['load_MW'], 13417.6)
        self.assertEquals(loads[4]['load_MW'], 13297.8)
        self.assertEquals(loads[5]['load_MW'], 13231.5)
        self.assertEquals(loads[6]['load_MW'], 13177.3)
        self.assertEquals(loads[7]['load_MW'], 13160.7)
        self.assertEquals(loads[8]['load_MW'], 13135.3)
        self.assertEquals(loads[9]['load_MW'], 13083.5)
        self.assertEquals(loads[10]['load_MW'], 12985.5)
        self.assertEquals(loads[11]['load_MW'], 12971.7)
