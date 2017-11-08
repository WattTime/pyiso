import os

from pyiso import client_factory
from unittest import TestCase
import pytz
from datetime import datetime


FIXTURES_DIR = os.path.join(os.path.dirname(__file__), '../fixtures/ercot')


class TestERCOT(TestCase):

    def setUp(self):
        self.c = client_factory('ERCOT')
        self.rtm_html = open(FIXTURES_DIR + '/real_time_system_conditions.html').read().encode('utf8')

    def test_utcify(self):
        ts_str = '05/03/2014 02:00'
        ts = self.c.utcify(ts_str)
        self.assertEqual(ts.year, 2014)
        self.assertEqual(ts.month, 5)
        self.assertEqual(ts.day, 3)
        self.assertEqual(ts.hour, 2+5)
        self.assertEqual(ts.minute, 0)
        self.assertEqual(ts.tzinfo, pytz.utc)

    def test_parse_load(self):
        self.c.handle_options(data='load', latest=True)
        data = self.c.parse_rtm(self.rtm_html)
        self.assertEqual(len(data), 1)
        expected_keys = ['timestamp', 'ba_name', 'load_MW', 'freq', 'market']
        self.assertEqual(sorted(data[0].keys()), sorted(expected_keys))
        self.assertEqual(data[0]['timestamp'], pytz.utc.localize(datetime(2016, 4, 14, 23, 38, 40)))
        self.assertEqual(data[0]['load_MW'], 38850.0)

    def test_parse_genmix(self):
        self.c.handle_options(data='gen', latest=True)
        data = self.c.parse_rtm(self.rtm_html)

        self.assertEqual(len(data), 2)
        expected_keys = ['timestamp', 'ba_name', 'gen_MW', 'fuel_name', 'freq', 'market']
        self.assertEqual(sorted(data[0].keys()), sorted(expected_keys))

        self.assertEqual(data[0]['timestamp'], pytz.utc.localize(datetime(2016, 4, 14, 23, 38, 40)))
        self.assertEqual(data[0]['gen_MW'], 5242.0)
        self.assertEqual(data[0]['fuel_name'], 'wind')

        self.assertEqual(data[1]['timestamp'], pytz.utc.localize(datetime(2016, 4, 14, 23, 38, 40)))
        self.assertEqual(data[1]['gen_MW'], 38850 - 5242 + 31 - 1)
        self.assertEqual(data[1]['fuel_name'], 'nonwind')
