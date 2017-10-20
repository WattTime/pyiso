from os import environ, path
from pyiso import client_factory
from unittest import TestCase
from datetime import datetime
import requests_mock
import pytz


def read_fixture(filename):
    fixtures_base_path = path.join(path.dirname(__file__), '../fixtures/isone')
    return open(path.join(fixtures_base_path, filename), 'r').read().encode('utf-8')


class TestISONE(TestCase):
    def setUp(self):
        environ['ISONE_USERNAME'] = 'test'
        environ['ISONE_PASSWORD'] = 'test'
        self.c = client_factory('ISONE')

    def test_auth(self):
        """Auth info should be set up from env during init"""
        self.assertEqual(len(self.c.auth), 2)

    def test_utcify(self):
        ts_str = '2014-05-03T02:32:44.000-04:00'
        ts = self.c.utcify(ts_str)
        self.assertEqual(ts.year, 2014)
        self.assertEqual(ts.month, 5)
        self.assertEqual(ts.day, 3)
        self.assertEqual(ts.hour, 2+4)
        self.assertEqual(ts.minute, 32)
        self.assertEqual(ts.second, 44)
        self.assertEqual(ts.tzinfo, pytz.utc)

    def test_handle_options_gen_latest(self):
        self.c.handle_options(data='gen', latest=True)
        self.assertEqual(self.c.options['market'], self.c.MARKET_CHOICES.na)
        self.assertEqual(self.c.options['frequency'], self.c.FREQUENCY_CHOICES.na)

    def test_handle_options_load_latest(self):
        self.c.handle_options(data='load', latest=True)
        self.assertEqual(self.c.options['market'], self.c.MARKET_CHOICES.fivemin)
        self.assertEqual(self.c.options['frequency'], self.c.FREQUENCY_CHOICES.fivemin)

    def test_handle_options_load_forecast(self):
        self.c.handle_options(data='load', forecast=True)
        self.assertEqual(self.c.options['market'], self.c.MARKET_CHOICES.dam)
        self.assertEqual(self.c.options['frequency'], self.c.FREQUENCY_CHOICES.dam)

    def test_handle_options_lmp_latest(self):
        self.c.handle_options(data='lmp', latest=True)
        self.assertEqual(self.c.options['market'], self.c.MARKET_CHOICES.fivemin)
        self.assertEqual(self.c.options['frequency'], self.c.FREQUENCY_CHOICES.fivemin)

    def test_endpoints_gen_latest(self):
        self.c.handle_options(data='gen', latest=True)
        endpoints = self.c.request_endpoints()
        self.assertEqual(len(endpoints), 1)
        self.assertIn('/genfuelmix/current.json', endpoints)

    def test_endpoints_load_latest(self):
        self.c.handle_options(data='load', latest=True)
        endpoints = self.c.request_endpoints()
        self.assertEqual(len(endpoints), 1)
        self.assertIn('/fiveminutesystemload/current.json', endpoints)

    def test_endpoints_gen_range(self):
        self.c.handle_options(data='gen',
                              start_at=pytz.utc.localize(datetime(2016, 5, 2, 12)),
                              end_at=pytz.utc.localize(datetime(2016, 5, 2, 14)))
        endpoints = self.c.request_endpoints()
        self.assertEqual(len(endpoints), 1)
        self.assertIn('/genfuelmix/day/20160502.json', endpoints)

    def test_endpoints_load_range(self):
        self.c.handle_options(data='load',
                              start_at=pytz.utc.localize(datetime(2016, 5, 2, 12)),
                              end_at=pytz.utc.localize(datetime(2016, 5, 2, 14)))
        endpoints = self.c.request_endpoints()
        self.assertEqual(len(endpoints), 1)
        self.assertIn('/fiveminutesystemload/day/20160502.json', endpoints)

    def test_endpoints_load_forecast(self):
        self.c.handle_options(data='load', forecast=True)
        endpoints = self.c.request_endpoints()
        self.assertGreaterEqual(len(endpoints), 3)
        self.assertIn('/hourlyloadforecast/day/', endpoints[0])

        now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Eastern'))
        date_str = now.strftime('%Y%m%d')
        self.assertIn(date_str, endpoints[0])

    def test_endpoints_lmp_latest(self):
        self.c.handle_options(data='lmp', latest=True)
        endpoints = self.c.request_endpoints(123)
        self.assertEqual(len(endpoints), 1)
        self.assertIn('/fiveminutelmp/current/location/123.json', endpoints)

    @requests_mock.Mocker()
    def test_get_morningreport(self, mock_request):
        expected_url = 'https://webservices.iso-ne.com/api/v1.1/morningreport/current.json'
        expected_response = read_fixture('morningreport_current.json')
        mock_request.get(expected_url, content=expected_response)

        resp = self.c.get_morningreport()
        assert "MorningReports" in resp

    @requests_mock.Mocker()
    def test_get_morningreport_for_day(self, mock_request):
        expected_url = 'https://webservices.iso-ne.com/api/v1.1/morningreport/day/20160101.json'
        expected_response = read_fixture('morningreport_20160101.json')
        mock_request.get(expected_url, content=expected_response)

        resp = self.c.get_morningreport(day="20160101")
        assert resp["MorningReports"]["MorningReport"][0]["BeginDate"] == "2016-01-01T00:00:00.000-05:00"

    def test_get_morningreport_bad_date(self):
        self.assertRaises(ValueError, self.c.get_morningreport, day="foo")

    @requests_mock.Mocker()
    def test_get_sevendayforecast(self, mock_request):
        expected_url = 'https://webservices.iso-ne.com/api/v1.1/sevendayforecast/current.json'
        expected_response = read_fixture('sevendayforecast_current.json')
        mock_request.get(expected_url, content=expected_response)

        resp = self.c.get_sevendayforecast()
        assert "SevenDayForecasts" in resp

    @requests_mock.Mocker()
    def test_get_sevendayforecast_for_day(self, mock_request):
        expected_url = 'https://webservices.iso-ne.com/api/v1.1/sevendayforecast/day/20160101.json'
        expected_response = read_fixture('sevendayforecast_20160101.json')
        mock_request.get(expected_url, content=expected_response)

        resp = self.c.get_sevendayforecast(day="20160101")
        assert resp["SevenDayForecasts"]["SevenDayForecast"][0]["BeginDate"] == "2016-01-01T00:00:00.000-05:00"

    def test_get_sevendayforecast_bad_date(self):
        self.assertRaises(ValueError, self.c.get_sevendayforecast, day="foo")
