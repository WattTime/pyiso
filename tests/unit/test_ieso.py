import os
from datetime import datetime
from datetime import timedelta
from unittest import TestCase

from pytz import timezone

from pyiso import client_factory
from pyiso import ieso
from pyiso.ieso import ParserFormat

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), '../fixtures/ieso')


class TestIESO(TestCase):
    def setUp(self):
        self.ieso_client = client_factory('IESO')

    def test_handle_options_sets_historical_and_forecast(self):
        start_at = self.ieso_client.local_now - timedelta(days=1)
        end_at = self.ieso_client.local_now + timedelta(days=1)

        self.ieso_client.handle_options(start_at=start_at, end_at=end_at)

        self.assertTrue(self.ieso_client.options.get('historical', False))
        self.assertTrue(self.ieso_client.options.get('forecast', False))

    def test_handle_options_sets_historical_only(self):
        start_at = self.ieso_client.local_now - timedelta(days=2)
        end_at = self.ieso_client.local_now - timedelta(days=1)

        self.ieso_client.handle_options(start_at=start_at, end_at=end_at)

        self.assertTrue(self.ieso_client.options.get('historical', False))
        self.assertFalse(self.ieso_client.options.get('forecast', False))

    def test_handle_options_sets_forecast_only(self):
        start_at = self.ieso_client.local_now
        end_at = self.ieso_client.local_now + timedelta(days=1)

        self.ieso_client.handle_options(start_at=start_at, end_at=end_at)

        self.assertFalse(self.ieso_client.options.get('historical', False))
        self.assertTrue(self.ieso_client.options.get('forecast', False))

    def test_handle_options_latest(self):
        self.ieso_client.handle_options(latest=True)
        self.assertTrue(self.ieso_client.options.get('latest', False))


class TestIntertieScheduleFlowReportHandler(TestCase):
    def setUp(self):
        self.report_handler = ieso.IntertieScheduleFlowReportHandler(ieso_client=client_factory('IESO'))

    def test_parse_report(self):
        start_at = datetime(year=2017, month=6, day=30, hour=0, minute=5, second=0,
                            tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        end_at = datetime(year=2017, month=7, day=1, hour=0, minute=0, second=59,
                          tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        # Offline copy of June 30, 2017 report requested as if it were July 1st.
        xml_content = open(FIXTURES_DIR + '/full_IntertieScheduleFlow_20170630.xml').read().encode('utf8')
        trades = list([])

        self.report_handler.parse_report(xml_content=xml_content, result_ts=trades,
                                         parser_format=ParserFormat.trade,
                                         min_datetime=start_at, max_datetime=end_at)

        self.assertEquals(len(trades), 288)  # 12 five-minute intervals * 24 hours.
        # Spot check fuel summations using known values
        self.assertEquals(trades[0]['net_exp_MW'], 2269.4)
        self.assertEquals(trades[287]['net_exp_MW'], 2242)

    def test_report_url_for_default(self):
        url = self.report_handler.report_url()
        self.assertEquals(url, 'http://reports.ieso.ca/public/IntertieScheduleFlow/PUB_IntertieScheduleFlow.xml')

    def test_report_url_for_hour_ending_in_same_day(self):
        report_datetime = datetime(year=2016, month=12, day=31, hour=1, minute=0, second=0,
                                   tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        url = self.report_handler.report_url(report_datetime)
        self.assertEquals(url,
                          'http://reports.ieso.ca/public/IntertieScheduleFlow/PUB_IntertieScheduleFlow_20161231.xml')

    def test_report_url_for_hour_ending_24_in_previous_day(self):
        report_datetime = datetime(year=2016, month=12, day=31, hour=0, minute=0, second=0,
                                   tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        url = self.report_handler.report_url(report_datetime)
        # Hour ending 24 is in previous day's report
        self.assertEquals(url,
                          'http://reports.ieso.ca/public/IntertieScheduleFlow/PUB_IntertieScheduleFlow_20161230.xml')


class TestAdequacyReportHandler(TestCase):
    def setUp(self):
        self.report_handler = ieso.AdequacyReportHandler(ieso_client=client_factory('IESO'))

    def test_parse_report_for_trade(self):
        start_at = datetime(year=2017, month=6, day=18, hour=1, minute=0, second=0,
                            tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        end_at = datetime(year=2017, month=6, day=19, hour=0, minute=59, second=59,
                          tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        xml_content = open(FIXTURES_DIR + '/full_Adequacy2_20170618.xml').read().encode('utf8')
        trades = list([])

        self.report_handler.parse_report(xml_content=xml_content, result_ts=trades, parser_format=ParserFormat.trade,
                                         min_datetime=start_at, max_datetime=end_at)

        self.assertEquals(len(trades), 24)  # 24 hours
        # Spot check net exports using known values
        self.assertEquals(trades[0]['net_exp_MW'], 2383)
        self.assertEquals(trades[23]['net_exp_MW'], 2051)

    def test_parse_report_for_fuel(self):
        start_at = datetime(year=2017, month=6, day=18, hour=1, minute=0, second=0,
                            tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        end_at = datetime(year=2017, month=6, day=19, hour=0, minute=59, second=59,
                          tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        xml_content = open(FIXTURES_DIR + '/full_Adequacy2_20170618.xml').read().encode('utf8')
        generation_ts = list([])

        self.report_handler.parse_report(xml_content=xml_content, result_ts=generation_ts,
                                         parser_format=ParserFormat.generation,
                                         min_datetime=start_at, max_datetime=end_at)

        self.assertEquals(len(generation_ts), 168)  # 7 fuels * 24 hours
        for val in generation_ts:  # Spot check fuel summations using known values
            if (val['fuel_name'] == 'nuclear') & (val['timestamp'].day == 18) & (val['timestamp'].hour == 6):
                self.assertEquals(val['gen_MW'], 11130)
            elif (val['fuel_name'] == 'wind') & (val['timestamp'].day == 18) & (val['timestamp'].hour == 6):
                self.assertEquals(val['gen_MW'], 744)
            elif (val['fuel_name'] == 'biomass') & (val['timestamp'].day == 19) & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 0)
            elif (val['fuel_name'] == 'hydro') & (val['timestamp'].day == 19) & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 3570)

    def test_report_url_for_default(self):
        url = self.report_handler.report_url()
        self.assertEquals(url, 'http://reports.ieso.ca/public/Adequacy2/PUB_Adequacy2.xml')

    def test_report_url_for_hour_ending_in_same_day(self):
        report_datetime = datetime(year=2016, month=12, day=31, hour=1, minute=0, second=0,
                                   tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        url = self.report_handler.report_url(report_datetime)
        self.assertEquals(url, 'http://reports.ieso.ca/public/Adequacy2/PUB_Adequacy2_20161231.xml')

    def test_report_url_for_hour_ending_24_in_previous_day(self):
        report_datetime = datetime(year=2016, month=12, day=31, hour=0, minute=0, second=0,
                                   tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        url = self.report_handler.report_url(report_datetime)
        # Hour ending 24 is in previous day's report
        self.assertEquals(url, 'http://reports.ieso.ca/public/Adequacy2/PUB_Adequacy2_20161230.xml')


class TestRealtimeConstrainedTotalsReportHandler(TestCase):
    def setUp(self):
        self.report_handler = ieso.RealTimeConstrainedTotalsReportHandler(ieso_client=client_factory('IESO'))

    def test_parse_report(self):
        start_at = datetime(year=2017, month=7, day=1, hour=0, minute=5, second=0,
                            tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        end_at = datetime(year=2017, month=7, day=1, hour=1, minute=0, second=59,
                          tzinfo=timezone(ieso.IESOClient.TZ_NAME))

        # Offline copy of July 1, 2017 report for delivery hour #1.
        xml_content = open(FIXTURES_DIR + '/full_RealtimeConstTotals_2017070101.xml').read().encode('utf8')
        load_ts = list([])

        self.report_handler.parse_report(xml_content=xml_content, result_ts=load_ts, parser_format=ParserFormat.load,
                                         min_datetime=start_at, max_datetime=end_at)

        self.assertEquals(len(load_ts), 12)  # 12 five-minute intervals in 1 hour.
        # Spot check fuel summations using known values
        self.assertEquals(load_ts[0]['load_MW'], 13531.9)
        self.assertEquals(load_ts[1]['load_MW'], 13629.1)
        self.assertEquals(load_ts[2]['load_MW'], 13520.8)
        self.assertEquals(load_ts[3]['load_MW'], 13417.6)
        self.assertEquals(load_ts[4]['load_MW'], 13297.8)
        self.assertEquals(load_ts[5]['load_MW'], 13231.5)
        self.assertEquals(load_ts[6]['load_MW'], 13177.3)
        self.assertEquals(load_ts[7]['load_MW'], 13160.7)
        self.assertEquals(load_ts[8]['load_MW'], 13135.3)
        self.assertEquals(load_ts[9]['load_MW'], 13083.5)
        self.assertEquals(load_ts[10]['load_MW'], 12985.5)
        self.assertEquals(load_ts[11]['load_MW'], 12971.7)

    def test_report_url_for_default(self):
        url = self.report_handler.report_url()
        self.assertEquals(url, 'http://reports.ieso.ca/public/RealtimeConstTotals/PUB_RealtimeConstTotals.xml')

    def test_report_url_for_interval_12_of_hour_ending_in_same_day(self):
        """ Hour 1, Interval 12 is 01:00. """
        report_datetime = datetime(year=2016, month=12, day=31, hour=1, minute=0, second=0,
                                   tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        url = self.report_handler.report_url(report_datetime)
        self.assertEquals(url,
                          'http://reports.ieso.ca/public/RealtimeConstTotals/PUB_RealtimeConstTotals_2016123101.xml')

    def test_report_url_for_interval_1_of_hour_ending_in_same_day(self):
        """ Hour 1, Interval 1 is 00:05. """
        report_datetime = datetime(year=2016, month=12, day=31, hour=0, minute=5, second=0,
                                   tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        url = self.report_handler.report_url(report_datetime)
        self.assertEquals(url,
                          'http://reports.ieso.ca/public/RealtimeConstTotals/PUB_RealtimeConstTotals_2016123101.xml')

    def test_report_url_for_interval_12_of_hour_ending_24_in_previous_day(self):
        """ Hour 24, Interval 12 in prior day's report is 00:00 of requested day. """
        report_datetime = datetime(year=2016, month=12, day=31, hour=0, minute=0, second=0,
                                   tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        url = self.report_handler.report_url(report_datetime)
        self.assertEquals(url,
                          'http://reports.ieso.ca/public/RealtimeConstTotals/PUB_RealtimeConstTotals_2016123024.xml')

    def test_report_url_for_interval_1_of_hour_ending_24(self):
        """ Hour 24, Interval 1 is 23:05 """
        report_datetime = datetime(year=2016, month=12, day=30, hour=23, minute=5, second=0,
                                   tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        url = self.report_handler.report_url(report_datetime)
        self.assertEquals(url,
                          'http://reports.ieso.ca/public/RealtimeConstTotals/PUB_RealtimeConstTotals_2016123024.xml')


class TestPredispatchConstrainedTotalsReportHandler(TestCase):
    def setUp(self):
        self.report_handler = ieso.PredispatchConstrainedTotalsReportHandler(ieso_client=client_factory('IESO'))

    def test_parse_report(self):
        start_at = datetime(year=2017, month=7, day=8, hour=1, minute=0, second=0,
                            tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        end_at = datetime(year=2017, month=7, day=9, hour=0, minute=59, second=59,
                          tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        xml_content = open(FIXTURES_DIR + '/full_PredispConstTotals_20170708.xml').read().encode('utf8')
        load_ts = list([])

        self.report_handler.parse_report(xml_content=xml_content, result_ts=load_ts, parser_format=ParserFormat.load,
                                         min_datetime=start_at, max_datetime=end_at)

        self.assertEquals(len(load_ts), 24)  # 24 hours
        # Spot check loads using known values
        self.assertEquals(load_ts[0]['load_MW'], 15361.1)
        self.assertEquals(load_ts[23]['load_MW'], 15440.1)

    def test_report_url_for_default(self):
        url = self.report_handler.report_url()
        self.assertEquals(url, 'http://reports.ieso.ca/public/PredispConstTotals/PUB_PredispConstTotals.xml')

    def test_report_url_for_hour_ending_in_same_day(self):
        report_datetime = datetime(year=2016, month=12, day=31, hour=1, minute=0, second=0,
                                   tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        url = self.report_handler.report_url(report_datetime)
        self.assertEquals(url,
                          'http://reports.ieso.ca/public/PredispConstTotals/PUB_PredispConstTotals_20161231.xml')

    def test_report_url_for_hour_ending_24_in_previous_day(self):
        report_datetime = datetime(year=2016, month=12, day=31, hour=0, minute=0, second=0,
                                   tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        url = self.report_handler.report_url(report_datetime)
        self.assertEquals(url,
                          'http://reports.ieso.ca/public/PredispConstTotals/PUB_PredispConstTotals_20161230.xml')


class TestGeneratorOutputCapabilityReportHandler(TestCase):
    def setUp(self):
        self.report_handler = ieso.GeneratorOutputCapabilityReportHandler(ieso_client=client_factory('IESO'))

    def test_parse_report_for_complete_day(self):
        start_at = datetime(year=2016, month=4, day=29, hour=1, minute=0, second=0,
                            tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        end_at = datetime(year=2016, month=4, day=30, hour=0, minute=59, second=59,
                          tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        xml_content = open(FIXTURES_DIR + '/reduced_GenOutputCapability_20160429.xml').read().encode('utf8')
        generation_ts = list([])

        self.report_handler.parse_report(xml_content=xml_content, result_ts=generation_ts,
                                         parser_format=ParserFormat.generation,
                                         min_datetime=start_at, max_datetime=end_at)

        self.assertEquals(len(generation_ts), 72)  # 24 hours of three fuels
        for val in generation_ts:  # Spot check fuel summations using known values
            if (val['fuel_name'] == 'nuclear') & (val['timestamp'].day == 29) & (val['timestamp'].hour == 6):
                self.assertEquals(val['gen_MW'], 1250)
            elif (val['fuel_name'] == 'nuclear') & (val['timestamp'].day == 30) & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 1555)
            elif (val['fuel_name'] == 'wind') & (val['timestamp'].day == 29) & (val['timestamp'].hour == 6):
                self.assertEquals(val['gen_MW'], 59)
            elif (val['fuel_name'] == 'wind') & (val['timestamp'].day == 30) & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 26)
            elif (val['fuel_name'] == 'biomass') & (val['timestamp'].day == 29) & (val['timestamp'].hour == 6):
                self.assertEquals(val['gen_MW'], 0)
            elif (val['fuel_name'] == 'biomass') & (val['timestamp'].day == 30) & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 0)

    def test_parse_report_for_partial_day(self):
        start_at = datetime(year=2016, month=5, day=1, hour=1, minute=0, second=0,
                            tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        end_at = datetime(year=2016, month=5, day=2, hour=0, minute=59, second=59,
                          tzinfo=timezone(ieso.IESOClient.TZ_NAME))

        # Reduced, offline copy of partial May 1, 2016 report
        xml_content = open(FIXTURES_DIR + '/reduced_GenOutputCapability_20160501.xml').read().encode('utf8')
        generation_ts = list([])

        self.report_handler.parse_report(xml_content=xml_content, result_ts=generation_ts,
                                         parser_format=ParserFormat.generation,
                                         min_datetime=start_at, max_datetime=end_at)

        self.assertEquals(len(generation_ts), 12)  # 4 hours of three fuels
        for val in generation_ts:  # Spot check fuel summations using known values
            if (val['fuel_name'] == 'nuclear') & (val['timestamp'].hour == 6):
                self.assertEquals(val['gen_MW'], 716)
            elif (val['fuel_name'] == 'nuclear') & (val['timestamp'].hour == 9):
                self.assertEquals(val['gen_MW'], 678)
            elif (val['fuel_name'] == 'natgas') & (val['timestamp'].hour == 6):
                self.assertEquals(val['gen_MW'], 120)
            elif (val['fuel_name'] == 'natgas') & (val['timestamp'].hour == 9):
                self.assertEquals(val['gen_MW'], 120)
            elif (val['fuel_name'] == 'hydro') & (val['timestamp'].hour == 6):
                self.assertEquals(val['gen_MW'], 165)
            elif (val['fuel_name'] == 'hydro') & (val['timestamp'].hour == 9):
                self.assertEquals(val['gen_MW'], 165)

    def test_report_url_for_default(self):
        url = self.report_handler.report_url()
        self.assertEquals(url, 'http://reports.ieso.ca/public/GenOutputCapability/PUB_GenOutputCapability.xml')

    def test_report_url_for_hour_ending_in_same_day(self):
        report_datetime = datetime(year=2016, month=12, day=31, hour=1, minute=0, second=0,
                                   tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        url = self.report_handler.report_url(report_datetime)
        self.assertEquals(url,
                          'http://reports.ieso.ca/public/GenOutputCapability/PUB_GenOutputCapability_20161231.xml')

    def test_report_url_for_hour_ending_24_in_previous_day(self):
        report_datetime = datetime(year=2016, month=12, day=31, hour=0, minute=0, second=0,
                                   tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        url = self.report_handler.report_url(report_datetime)
        self.assertEquals(url,
                          'http://reports.ieso.ca/public/GenOutputCapability/PUB_GenOutputCapability_20161230.xml')


class TestGeneratorOutputByFuelHourlyReportHandler(TestCase):
    def setUp(self):
        self.report_handler = ieso.GeneratorOutputByFuelHourlyReportHandler(ieso_client=client_factory('IESO'))

    def test_parse_report(self):
        start_at = datetime(year=2016, month=1, day=1, hour=1, minute=0, second=0,
                            tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        end_at = datetime(year=2016, month=1, day=8, hour=0, minute=59, second=59,
                          tzinfo=timezone(ieso.IESOClient.TZ_NAME))

        # Offline copy of 2016 report, as if it were requested on January 8th.
        xml_content = open(FIXTURES_DIR + '/reduced_GenOutputbyFuelHourly_2016.xml').read().encode('utf8')
        generation_ts = list([])

        self.report_handler.parse_report(xml_content=xml_content, result_ts=generation_ts,
                                         parser_format=ParserFormat.generation,
                                         min_datetime=start_at, max_datetime=end_at)

        self.assertEquals(len(generation_ts), 1008)  # 6 fuels * 24 hours * 7 days
        for val in generation_ts:  # Spot check known values
            if (val['fuel_name'] == 'nuclear') & (val['timestamp'].day == 1) & (val['timestamp'].hour == 6):
                self.assertEquals(val['gen_MW'], 11188)
            elif (val['fuel_name'] == 'biomass') & (val['timestamp'].day == 2) & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 2)
            elif (val['fuel_name'] == 'wind') & (val['timestamp'].day == 7) & (val['timestamp'].hour == 6):
                self.assertEquals(val['gen_MW'], 809)
            elif (val['fuel_name'] == 'hydro') & (val['timestamp'].day == 8) & (val['timestamp'].hour == 5):
                self.assertEquals(val['gen_MW'], 4749)

    def test_report_url_for_default(self):
        url = self.report_handler.report_url()
        self.assertEquals(url, 'http://reports.ieso.ca/public/GenOutputbyFuelHourly/PUB_GenOutputbyFuelHourly.xml')

    def test_report_url_for_hour_ending_in_same_year(self):
        report_datetime = datetime(year=2016, month=1, day=1, hour=1, minute=0, second=0,
                                   tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        url = self.report_handler.report_url(report_datetime)
        self.assertEquals(url,
                          'http://reports.ieso.ca/public/GenOutputbyFuelHourly/PUB_GenOutputbyFuelHourly_2016.xml')

    def test_report_url_for_hour_ending_24_in_previous_year(self):
        report_datetime = datetime(year=2016, month=1, day=1, hour=0, minute=0, second=0,
                                   tzinfo=timezone(ieso.IESOClient.TZ_NAME))
        url = self.report_handler.report_url(report_datetime)
        self.assertEquals(url,
                          'http://reports.ieso.ca/public/GenOutputbyFuelHourly/PUB_GenOutputbyFuelHourly_2015.xml')
