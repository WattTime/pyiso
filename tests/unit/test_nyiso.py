import os

from pyiso import client_factory
from unittest import TestCase
from datetime import date

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), '../fixtures/nyiso')


class TestNYISOBase(TestCase):
    def setUp(self):
        self.c = client_factory('NYISO')

        self.load_csv = open(FIXTURES_DIR + '/load.csv').read().encode('utf8')
        self.load_forecast_csv = open(FIXTURES_DIR + '/load_forecast.csv').read().encode('utf8')
        self.trade_csv = open(FIXTURES_DIR + '/trade.csv').read().encode('utf8')
        self.genmix_csv = open(FIXTURES_DIR + '/genmix.csv').read().encode('utf8')
        self.lmp_csv = open(FIXTURES_DIR + '/lmp.csv').read().encode('utf8')

    def test_parse_load_rtm(self):
        self.c.options = {'data': 'dummy'}
        data = self.c.parse_load_rtm(self.load_csv)
        for idx, row in data.iterrows():
            self.assertEqual(idx.date(), date(2014, 9, 10))
            self.assertGreater(row['load_MW'], 15700)
            self.assertLess(row['load_MW'], 16100)

        # should have 4 dps, even though file has 5 (last one has no data)
        self.assertEqual(len(data), 4)

    def test_parse_load_forecast(self):
        self.c.options = {'data': 'dummy'}
        data = self.c.parse_load_forecast(self.load_forecast_csv)
        for idx, row in data.iterrows():
            self.assertGreaterEqual(idx.date(), date(2015, 11, 22))
            self.assertLessEqual(idx.date(), date(2015, 11, 28))
            self.assertGreater(row['load_MW'], 12000)
            self.assertLess(row['load_MW'], 20200)

        # should have 6 days of hourly data
        self.assertEqual(len(data), 24*6)

    def test_parse_trade(self):
        self.c.options = {'data': 'dummy'}
        df = self.c.parse_trade(self.trade_csv)

        for idx, row in df.iterrows():
            self.assertEqual(idx.date(), date(2014, 9, 10))

            self.assertLess(row['net_exp_MW'], -1400)
            self.assertGreater(row['net_exp_MW'], -6300)

        # should have 3 timestamps
        self.assertEqual(len(df), 3)

        self.assertEqual(df.index.name, 'timestamp')

    def test_parse_genmix(self):
        self.c.options = {'data': 'dummy'}
        df = self.c.parse_genmix(self.genmix_csv)

        for idx, row in df.iterrows():
            self.assertEqual(idx.date(), date(2016, 1, 19))

            self.assertLess(row['gen_MW'], 5500)
            self.assertGreater(row['gen_MW'], 100)
            self.assertIn(row['fuel_name'], self.c.fuel_names.values())

        # should have 3 timestamps with 7 fuels
        self.assertEqual(len(df), 3*len(self.c.fuel_names))

        self.assertEqual(df.index.name, 'timestamp')

    def test_parse_lmp(self):
        self.c.options = {'data': 'lmp', 'node_id': ['LONGIL'], 'latest': True}
        df = self.c.parse_lmp(self.lmp_csv)

        for idx, row in df.iterrows():
            self.assertEqual(idx.date(), date(2016, 2, 18))

            self.assertLess(row['lmp'], 1000)
            self.assertGreater(row['lmp'], -100)

            self.assertEqual(row['node_id'], 'LONGIL')

        self.assertEqual(df.index.name, 'timestamp')
