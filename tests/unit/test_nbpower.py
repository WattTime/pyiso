import os
from datetime import datetime
from unittest import TestCase

import pytz
import requests_mock
from freezegun import freeze_time

from pyiso import client_factory
from pyiso.base import BaseClient

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), '../fixtures')


class TestNBPowerClient(TestCase):
    def setUp(self):
        self.nbpower_client = client_factory('NBP')
    
    def test_nbpower_from_client_factory(self):
        self.assertIsInstance(self.nbpower_client, BaseClient)

    @requests_mock.Mocker()
    def test_get_load_latest(self, exptected_requests):
        mocked_html = open(FIXTURES_DIR + '/nbpower/SystemInformation_realtime.html').read().encode('utf8')
        exptected_requests.get(self.nbpower_client.LATEST_REPORT_URL, content=mocked_html)

        load_ts = self.nbpower_client.get_load(latest=True)

        self.assertEqual(len(load_ts), 1)
        self.assertEqual(load_ts[0].get('timestamp', None), datetime(year=2017, month=7, day=17, hour=1, minute=57,
                                                                     second=29, microsecond=0, tzinfo=pytz.utc))
        self.assertEqual(load_ts[0].get('load_MW', None), 1150)

    @requests_mock.Mocker()
    @freeze_time('2017-07-16 22:58:00-03:00')
    def test_get_load_dange_range_without_forecast(self, expected_requests):
        mocked_html = open(FIXTURES_DIR + '/nbpower/SystemInformation_realtime.html').read().encode('utf8')
        expected_requests.get(self.nbpower_client.LATEST_REPORT_URL, content=mocked_html)

        start_at = datetime(year=2017, month=7, day=16, hour=0, minute=0, second=0, microsecond=0,tzinfo=pytz.utc)
        # End time is the same as freeze_time (i.e. end_at = "now").
        end_at = datetime(year=2017, month=7, day=17, hour=1, minute=58, second=0, microsecond=0, tzinfo=pytz.utc)
        load_ts = self.nbpower_client.get_load(start_at=start_at, end_at=end_at)

        self.assertEqual(len(load_ts), 1)
        self.assertEqual(load_ts[0].get('timestamp', None), datetime(year=2017, month=7, day=17, hour=1, minute=57,
                                                                     second=29, microsecond=0, tzinfo=pytz.utc))
        self.assertEqual(load_ts[0].get('load_MW', None), 1150)

    @requests_mock.Mocker()
    @freeze_time('2017-07-16 22:58:00-03:00')
    def test_get_load_dange_range_with_forecast(self, expected_requests):
        self.nbpower_client = client_factory('NBP')
        exp_forect_url = 'http://tso.nbpower.com/reports%20%26%20assessments/load%20forecast/hourly/2017-07-16%2022.csv'
        mocked_csv = open(FIXTURES_DIR + '/nbpower/2017-07-16 22.csv').read().encode('utf8')
        mocked_html = open(FIXTURES_DIR + '/nbpower/SystemInformation_realtime.html').read().encode('utf8')

        expected_requests.get(self.nbpower_client.LATEST_REPORT_URL, content=mocked_html)
        expected_requests.get(exp_forect_url, content=mocked_csv)

        start_at = datetime(year=2017, month=7, day=16, hour=3, minute=0, second=0, microsecond=0, tzinfo=pytz.utc)
        end_at = datetime(year=2017, month=7, day=17, hour=4, minute=0, second=0, microsecond=0, tzinfo=pytz.utc)
        load_ts = self.nbpower_client.get_load(start_at=start_at, end_at=end_at)

        self.assertEqual(len(load_ts), 4)  # latest + 3 forecasts.
        # The first forecast is in the past, so the first timestamp should be from the latest report.
        self.assertEqual(load_ts[0].get('timestamp', None), datetime(year=2017, month=7, day=17, hour=1, minute=57,
                                                                     second=29, microsecond=0, tzinfo=pytz.utc))
        self.assertEqual(load_ts[0].get('load_MW', None), 1150)
        self.assertEqual(load_ts[1].get('timestamp', None), datetime(year=2017, month=7, day=17, hour=2, minute=0,
                                                                     second=0, microsecond=0, tzinfo=pytz.utc))
        self.assertEqual(load_ts[1].get('load_MW', None), 1160)
        self.assertEqual(load_ts[2].get('timestamp', None), datetime(year=2017, month=7, day=17, hour=3, minute=0,
                                                                     second=0, microsecond=0, tzinfo=pytz.utc))
        self.assertEqual(load_ts[2].get('load_MW', None), 1129)
        self.assertEqual(load_ts[3].get('timestamp', None), datetime(year=2017, month=7, day=17, hour=4, minute=0,
                                                                     second=0, microsecond=0, tzinfo=pytz.utc))
        self.assertEqual(load_ts[3].get('load_MW', None), 1089)

    @requests_mock.Mocker()
    def test_get_trade_latest(self, expected_requests):
        mocked_html = open(FIXTURES_DIR + '/nbpower/SystemInformation_realtime.html').read().encode('utf8')
        expected_requests.get(self.nbpower_client.LATEST_REPORT_URL, content=mocked_html)

        trade_ts = self.nbpower_client.get_trade(latest=True)

        self.assertEqual(len(trade_ts), 1)
        self.assertEqual(trade_ts[0].get('timestamp', None), datetime(year=2017, month=7, day=17, hour=1, minute=57,
                                                                      second=29, microsecond=0, tzinfo=pytz.utc))
        self.assertEqual(trade_ts[0].get('net_exp_MW', None), 183)
