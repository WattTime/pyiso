import os
from unittest import TestCase

import requests_mock
from dateutil.parser import parse
from freezegun import freeze_time
from pandas import Timestamp

from pyiso import client_factory
from pyiso.base import BaseClient
from tests import read_fixture


class TestNBPowerClient(TestCase):
    def setUp(self):
        self.nbpower_client = client_factory('NBP')
    
    def test_nbpower_from_client_factory(self):
        self.assertIsInstance(self.nbpower_client, BaseClient)

    @requests_mock.Mocker()
    def test_get_load_latest(self, exptected_requests):
        mocked_html = read_fixture('nbpower', 'SystemInformation_realtime.html').encode('utf8')
        exptected_requests.get(self.nbpower_client.LATEST_REPORT_URL, content=mocked_html)

        load_ts = self.nbpower_client.get_load(latest=True)

        self.assertEqual(len(load_ts), 1)
        self.assertEqual(load_ts[0].get('timestamp', None), Timestamp('2017-07-17T01:57:29.000Z'))
        self.assertEqual(load_ts[0].get('load_MW', None), 1150)

    @requests_mock.Mocker()
    @freeze_time('2017-07-16 22:58:00-03:00')
    def test_get_load_dange_range_without_forecast(self, expected_requests):
        mocked_html = read_fixture('nbpower', 'SystemInformation_realtime.html').encode('utf8')
        expected_requests.get(self.nbpower_client.LATEST_REPORT_URL, content=mocked_html)

        start_at = parse('2017-07-16T21:00:00-03:00')
        # End time is the same as freeze_time (i.e. end_at = "now").
        end_at = parse('2017-07-16T22:58:00-03:00')
        load_ts = self.nbpower_client.get_load(start_at=start_at, end_at=end_at)

        self.assertEqual(len(load_ts), 1)
        self.assertEqual(load_ts[0].get('timestamp', None), Timestamp('2017-07-17T01:57:29.000Z'))
        self.assertEqual(load_ts[0].get('load_MW', None), 1150)

    @requests_mock.Mocker()
    @freeze_time('2017-07-16 22:58:00-03:00')
    def test_get_load_dange_range_with_latest_and_forecast(self, expected_requests):
        self.nbpower_client = client_factory('NBP')
        exp_forect_url = 'http://tso.nbpower.com/reports%20%26%20assessments/load%20forecast/hourly/2017-07-16%2022.csv'
        mocked_csv = read_fixture('nbpower', '2017-07-16 22.csv').encode('utf8')
        mocked_html = read_fixture('nbpower', 'SystemInformation_realtime.html').encode('utf8')

        expected_requests.get(self.nbpower_client.LATEST_REPORT_URL, content=mocked_html)
        expected_requests.get(exp_forect_url, content=mocked_csv)

        start_at = parse('2017-07-16T00:00:00-03:00')
        end_at = parse('2017-07-17T01:00:00-03:00')
        load_ts = self.nbpower_client.get_load(start_at=start_at, end_at=end_at)

        self.assertEqual(len(load_ts), 4)  # latest + 3 forecasts.
        # The first forecast is in the past, so the first timestamp should be from the latest report.
        self.assertEqual(load_ts[0].get('timestamp', None), Timestamp('2017-07-17T01:57:29.000Z'))
        self.assertEqual(load_ts[0].get('load_MW', None), 1150)
        self.assertEqual(load_ts[1].get('timestamp', None), Timestamp('2017-07-17T02:00:00.000Z'))
        self.assertEqual(load_ts[1].get('load_MW', None), 1160)
        self.assertEqual(load_ts[2].get('timestamp', None), Timestamp('2017-07-17T03:00:00.000Z'))
        self.assertEqual(load_ts[2].get('load_MW', None), 1129)
        self.assertEqual(load_ts[3].get('timestamp', None), Timestamp('2017-07-17T04:00:00.000Z'))
        self.assertEqual(load_ts[3].get('load_MW', None), 1089)

    @requests_mock.Mocker()
    @freeze_time('2017-03-12 00:00:00-04:00')
    def test_get_load_forecast_dst_start(self, expected_requests):
        self.nbpower_client = client_factory('NBP')
        exp_forect_url = 'http://tso.nbpower.com/reports%20%26%20assessments/load%20forecast/hourly/2017-03-12%2000.csv'
        mocked_csv = read_fixture('nbpower', '2017-03-12 00.csv').encode('utf8')

        expected_requests.get(exp_forect_url, content=mocked_csv)

        start_at = parse('2017-03-12T00:00:00-04:00')
        end_at = parse('2017-03-12T04:00:00-03:00')
        load_ts = self.nbpower_client.get_load(start_at=start_at, end_at=end_at)

        self.assertEqual(len(load_ts), 3)  # 4 forecasts - 1 for quirky duplicate 2017-03-12T03:00-04:00 overwrite
        self.assertEqual(load_ts[0].get('timestamp', None), Timestamp('2017-03-12T04:00:00.000Z'))
        self.assertEqual(load_ts[0].get('load_MW', None), 2020)
        self.assertEqual(load_ts[1].get('timestamp', None), Timestamp('2017-03-12T05:00:00.000Z'))
        self.assertEqual(load_ts[1].get('load_MW', None), 1982)
        self.assertEqual(load_ts[2].get('timestamp', None), Timestamp('2017-03-12T06:00:00.000Z'))
        self.assertEqual(load_ts[2].get('load_MW', None), 1974)  # second T03:00-03:00 load value overwrites first

    @requests_mock.Mocker()
    @freeze_time('2017-03-13 00:00:00-03:00')
    def test_get_load_forecast_dst(self, expected_requests):
        self.nbpower_client = client_factory('NBP')
        exp_forect_url = 'http://tso.nbpower.com/reports%20%26%20assessments/load%20forecast/hourly/2017-03-13%2000.csv'
        mocked_csv = read_fixture('nbpower', '2017-03-13 00.csv').encode('utf8')

        expected_requests.get(exp_forect_url, content=mocked_csv)

        start_at = parse('2017-03-13T00:00:00-03:00')
        end_at = parse('2017-03-13T03:00:00-03:00')
        load_ts = self.nbpower_client.get_load(start_at=start_at, end_at=end_at)

        self.assertEqual(len(load_ts), 4)  # 4 hours of load forecasts
        self.assertEqual(load_ts[0].get('timestamp', None), Timestamp('2017-03-12T03:00:00.000Z'))
        self.assertEqual(load_ts[3].get('timestamp', None), Timestamp('2017-03-12T06:00:00.000Z'))

    @requests_mock.Mocker()
    @freeze_time('2017-11-05 00:00:00-03:00')
    def test_get_load_forecast_standard_time_start(self, expected_requests):
        self.nbpower_client = client_factory('NBP')
        exp_forect_url = 'http://tso.nbpower.com/reports%20%26%20assessments/load%20forecast/hourly/2017-11-05%2000.csv'
        mocked_csv = read_fixture('nbpower', '2017-11-05 00.csv').encode('utf8')

        expected_requests.get(exp_forect_url, content=mocked_csv)

        start_at = parse('2017-11-05T00:00:00-03:00')
        end_at = parse('2017-11-05T03:00:00-04:00')
        load_ts = self.nbpower_client.get_load(start_at=start_at, end_at=end_at)

        self.assertEqual(len(load_ts), 4)  # 4 hours of load forecasts
        self.assertEqual(load_ts[0].get('timestamp', None), Timestamp('2017-11-05T03:00:00.000Z'))
        self.assertEqual(load_ts[0].get('load_MW', None), 1293)
        self.assertEqual(load_ts[1].get('timestamp', None), Timestamp('2017-11-05T04:00:00.000Z'))
        self.assertEqual(load_ts[1].get('load_MW', None), 1266)
        self.assertEqual(load_ts[2].get('timestamp', None), Timestamp('2017-11-05T05:00:00.000Z'))
        self.assertEqual(load_ts[2].get('load_MW', None), 1261)
        # CSV skips time 20171105020000AS (i.e. 2017-11-05T02:00:00-04:00)
        self.assertEqual(load_ts[3].get('timestamp', None), Timestamp('2017-11-05T07:00:00.000Z'))
        self.assertEqual(load_ts[3].get('load_MW', None), 1262)

    @requests_mock.Mocker()
    @freeze_time('2017-11-06 00:00:00-04:00')
    def test_get_load_forecast_standard_time(self, expected_requests):
        self.nbpower_client = client_factory('NBP')
        exp_forect_url = 'http://tso.nbpower.com/reports%20%26%20assessments/load%20forecast/hourly/2017-11-06%2000.csv'
        mocked_csv = read_fixture('nbpower', '2017-11-06 00.csv').encode('utf8')

        expected_requests.get(exp_forect_url, content=mocked_csv)

        start_at = parse('2017-11-06T00:00:00-04:00')
        end_at = parse('2017-11-06T03:00:00-04:00')
        load_ts = self.nbpower_client.get_load(start_at=start_at, end_at=end_at)

        self.assertEqual(len(load_ts), 4)  # 4 hours of load forecasts
        self.assertEqual(load_ts[0].get('timestamp', None), Timestamp('2017-11-06T04:00:00.000Z'))
        self.assertEqual(load_ts[3].get('timestamp', None), Timestamp('2017-11-06T07:00:00.000Z'))

    @requests_mock.Mocker()
    def test_get_trade_latest(self, expected_requests):
        mocked_html = read_fixture('nbpower', 'SystemInformation_realtime.html').encode('utf8')
        expected_requests.get(self.nbpower_client.LATEST_REPORT_URL, content=mocked_html)

        trade_ts = self.nbpower_client.get_trade(latest=True)

        self.assertEqual(len(trade_ts), 1)
        self.assertEqual(trade_ts[0].get('timestamp', None), Timestamp('2017-07-17T01:57:29.000Z'))
        self.assertEqual(trade_ts[0].get('net_exp_MW', None), 183)
