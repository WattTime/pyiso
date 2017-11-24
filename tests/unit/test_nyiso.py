import os

from pyiso import client_factory
from unittest import TestCase
from datetime import date

from tests import read_fixture


class TestNYISOBase(TestCase):
    def setUp(self):
        self.c = client_factory('NYISO')
        self.load_csv = read_fixture(ba_name='nyiso', filename='load.csv')
        self.load_forecast_csv = read_fixture(ba_name='nyiso', filename='load_forecast.csv')
        self.trade_csv = read_fixture(ba_name='nyiso', filename='trade.csv')
        self.rtfuelmix_csv = read_fixture(ba_name='nyiso', filename='20171122rtfuelmix.csv')
        self.lmp_csv = read_fixture(ba_name='nyiso', filename='lmp.csv')

    def test_parse_load_rtm(self):
        self.c.options = {'data': 'dummy'}
        data = self.c.parse_load_rtm(self.load_csv.encode('utf-8'))
        for idx, row in data.iterrows():
            self.assertEqual(idx.date(), date(2014, 9, 10))
            self.assertGreater(row['load_MW'], 15700)
            self.assertLess(row['load_MW'], 16100)

        # should have 4 dps, even though file has 5 (last one has no data)
        self.assertEqual(len(data), 4)

    def test_parse_load_forecast(self):
        self.c.options = {'data': 'dummy'}
        data = self.c.parse_load_forecast(self.load_forecast_csv.encode('utf-8'))
        for idx, row in data.iterrows():
            self.assertGreaterEqual(idx.date(), date(2015, 11, 22))
            self.assertLessEqual(idx.date(), date(2015, 11, 28))
            self.assertGreater(row['load_MW'], 12000)
            self.assertLess(row['load_MW'], 20200)

        # should have 6 days of hourly data
        self.assertEqual(len(data), 24*6)

    def test_parse_trade(self):
        self.c.options = {'data': 'dummy'}
        df = self.c.parse_trade(self.trade_csv.encode('utf-8'))

        for idx, row in df.iterrows():
            self.assertEqual(idx.date(), date(2014, 9, 10))

            self.assertLess(row['net_exp_MW'], -1400)
            self.assertGreater(row['net_exp_MW'], -6300)

        # should have 3 timestamps
        self.assertEqual(len(df), 3)

        self.assertEqual(df.index.name, 'timestamp')

    def test_parse_genmix(self):
        self.c.options = {'data': 'dummy'}
        df = self.c.parse_genmix(self.rtfuelmix_csv.encode('utf-8'))

        for idx, row in df.iterrows():
            self.assertIn(idx.date(), [date(2017, 11, 22), date(2017, 11, 23)])
            self.assertLess(row['gen_MW'], 5500)
            self.assertIn(row['fuel_name'], self.c.fuel_names.values())

        self.assertEqual(df.index.name, 'timestamp')

    def test_parse_lmp(self):
        self.c.options = {'data': 'lmp', 'node_id': ['LONGIL'], 'latest': True}
        df = self.c.parse_lmp(self.lmp_csv.encode('utf-8'))

        for idx, row in df.iterrows():
            self.assertEqual(idx.date(), date(2016, 2, 18))

            self.assertLess(row['lmp'], 1000)
            self.assertGreater(row['lmp'], -100)

            self.assertEqual(row['node_id'], 'LONGIL')

        self.assertEqual(df.index.name, 'timestamp')
