from pyiso import client_factory
from unittest import TestCase
from datetime import date

from tests import read_fixture


class TestNYISOBase(TestCase):
    def setUp(self):
        self.c = client_factory('NYISO')

    def test_parse_load_rtm(self):
        self.c.options = {'data': 'dummy'}
        actual_load_csv = read_fixture(ba_name='nyiso', filename='20171122pal.csv')
        df = self.c.parse_load_rtm(actual_load_csv.encode('utf-8'))
        for idx, row in df.iterrows():
            self.assertIn(idx.date(), [date(2017, 11, 22), date(2017, 11, 23)])
            self.assertGreater(row['load_MW'], 13000)
            self.assertLess(row['load_MW'], 22000)
        self.assertEqual(df.index.name, 'timestamp')

    def test_parse_load_forecast(self):
        self.c.options = {'data': 'dummy'}
        load_forecast_csv = read_fixture(ba_name='nyiso', filename='20171122isolf.csv')
        data = self.c.parse_load_forecast(load_forecast_csv.encode('utf-8'))
        for idx, row in data.iterrows():
            self.assertGreaterEqual(idx.date(), date(2017, 11, 22))
            self.assertLessEqual(idx.date(), date(2017, 11, 28))
            self.assertGreater(row['load_MW'], 12000)
            self.assertLess(row['load_MW'], 22000)
        # should have 6 days of hourly data
        self.assertEqual(len(data), 24*6)

    def test_parse_trade(self):
        self.c.options = {'data': 'dummy'}
        external_limits_flows_csv = read_fixture(ba_name='nyiso', filename='20171122ExternalLimitsFlows.csv')
        df = self.c.parse_trade(external_limits_flows_csv.encode('utf-8'))

        for idx, row in df.iterrows():
            self.assertIn(idx.date(), [date(2017, 11, 22), date(2017, 11, 23)])
            self.assertLess(row['net_exp_MW'], -1400)
            self.assertGreater(row['net_exp_MW'], -6300)
        self.assertEqual(df.index.name, 'timestamp')

    def test_parse_genmix(self):
        self.c.options = {'data': 'dummy'}
        rtfuelmix_csv = read_fixture(ba_name='nyiso', filename='20171122rtfuelmix.csv')
        df = self.c.parse_genmix(rtfuelmix_csv.encode('utf-8'))

        for idx, row in df.iterrows():
            self.assertIn(idx.date(), [date(2017, 11, 22), date(2017, 11, 23)])
            self.assertLess(row['gen_MW'], 5500)
            self.assertIn(row['fuel_name'], self.c.fuel_names.values())
        self.assertEqual(df.index.name, 'timestamp')

    def test_parse_legacy_genmix(self):
        """
        Tests that legacy generation mix data format can still be parsed if someone requests a historical time range.
        """
        self.c.options = {'data': 'dummy'}
        legacy_ftfuelmix_csv = read_fixture(ba_name='nyiso', filename='20160119rtfuelmix.csv')
        df = self.c.parse_genmix(legacy_ftfuelmix_csv.encode('utf-8'))

        for idx, row in df.iterrows():
            self.assertIn(idx.date(), [date(2016, 1, 19), date(2016, 1, 20)])
            self.assertLess(row['gen_MW'], 5500)
            self.assertIn(row['fuel_name'], self.c.fuel_names.values())
        self.assertEqual(df.index.name, 'timestamp')
