from pyiso import client_factory
from unittest import TestCase
import pytz


class TestMISO(TestCase):
    def setUp(self):
        self.c = client_factory('MISO')

    def test_utcify(self):
        ts_str = '2014-05-03T01:45:00'
        ts = self.c.utcify(ts_str)
        self.assertEqual(ts.year, 2014)
        self.assertEqual(ts.month, 5)
        self.assertEqual(ts.day, 3)
        self.assertEqual(ts.hour, 1+5)
        self.assertEqual(ts.minute, 45)
        self.assertEqual(ts.tzinfo, pytz.utc)

    def test_parse_latest_fuel_mix_bad(self):
        bad_content = b'header1,header2\nnotadate,2016-01-01'
        data = self.c.parse_latest_fuel_mix(bad_content)
        self.assertEqual(len(data), 0)
