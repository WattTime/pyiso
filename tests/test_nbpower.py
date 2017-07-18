import os
from unittest import TestCase

import requests_mock

from pyiso import client_factory
from pyiso.base import BaseClient

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), 'fixtures')


class TestNBPowerClient(TestCase):
    def setUp(self):
        self.nbpower_client = client_factory('NBP')

    def test_nbpower_from_client_factory(self):
        self.assertIsInstance(self.nbpower_client, BaseClient)

    @requests_mock.Mocker()
    def test_get_load_latest(self, req_expectation):
        html_content = open(FIXTURES_DIR + '/nbpower_SystemInformation_realtime.html').read().encode('utf8')
        req_expectation.get(self.nbpower_client.LATEST_REPORT_URL, content=html_content)

        load_ts = self.nbpower_client.get_load(latest=True)

        self.assertEqual(len(load_ts), 1)
        self.assertEqual(load_ts[0].get('load_MW', None), 1150)

    @requests_mock.Mocker()
    def test_get_trade_latest(self, req_expectation):
        html_content = open(FIXTURES_DIR + '/nbpower_SystemInformation_realtime.html').read().encode('utf8')
        req_expectation.get(self.nbpower_client.LATEST_REPORT_URL, content=html_content)

        trade_ts = self.nbpower_client.get_trade(latest=True)

        self.assertEqual(len(trade_ts), 1)
        self.assertEqual(trade_ts[0].get('net_exp_MW', None), 183)
