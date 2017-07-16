import os
from unittest import TestCase

import requests_mock

from pyiso import client_factory
from pyiso.base import BaseClient

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), 'fixtures')


class TestAESOClient(TestCase):
    def setUp(self):
        self.aeso_client = client_factory('AESO')

    def test_aeso_retrievable_from_client_factory(self):
        self.assertIsInstance(self.aeso_client, BaseClient)

    @requests_mock.Mocker()
    def test_nominal_get_generation(self, req_expectation):
        csv_content = open(FIXTURES_DIR + '/aeso_latest_electricity_market_report.csv').read().encode('ascii')

        req_expectation.get(self.aeso_client.REPORT_URL, content=csv_content)

        generation_ts = self.aeso_client.get_generation(latest=True)

        self.assertEqual(len(generation_ts), 5)  # Five fuels
        for row in generation_ts:
            fuel_name = row.get('fuel_name', None)
            self.assertNotEqual(fuel_name, None)
            if fuel_name == 'COAL':
                self.assertEqual(row.get('gen_MW', None), 4504)
            elif fuel_name == 'GAS':
                self.assertEqual(row.get('gen_MW', None), 4321)
            elif fuel_name == 'HYDRO':
                self.assertEqual(row.get('gen_MW', None), 426)
            elif fuel_name == 'OTHER':
                self.assertTrue(row.get('gen_MW', None), 261)
            elif fuel_name == 'WIND':
                self.assertTrue(row.get('gen_MW', None), 542)
            else:
                self.fail('Unexpected fuel name found in generation timeseries')

    @requests_mock.Mocker()
    def test_nominal_get_trade(self, req_expectation):
        csv_content = open(FIXTURES_DIR + '/aeso_latest_electricity_market_report.csv').read().encode('ascii')

        req_expectation.get(self.aeso_client.REPORT_URL, content=csv_content)

        trade_ts = self.aeso_client.get_trade(latest=True)

        self.assertEqual(len(trade_ts), 1)
        self.assertEqual(trade_ts[0].get('net_exp_MW', None), -216)

    @requests_mock.Mocker()
    def test_nominal_get_load(self, req_expectation):
        csv_content = open(FIXTURES_DIR + '/aeso_latest_electricity_market_report.csv').read().encode('ascii')

        req_expectation.get(self.aeso_client.REPORT_URL, content=csv_content)

        load_ts = self.aeso_client.get_load(latest=True)

        self.assertEqual(len(load_ts), 1)
        self.assertEqual(load_ts[0].get('load_MW', None), 10270)
