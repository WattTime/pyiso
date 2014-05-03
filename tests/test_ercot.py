from pyiso import client_factory
from unittest import TestCase
import pytz
import logging


class TestERCOT(TestCase):
    def setUp(self):
        self.c = client_factory('ERCOT')
        handler = logging.StreamHandler()
        self.c.logger.addHandler(handler)
        self.c.logger.setLevel(logging.DEBUG)

    def test_utcify(self):
        ts_str = '05/03/2014 02:00'
        ts = self.c.utcify(ts_str)
        self.assertEqual(ts.year, 2014)
        self.assertEqual(ts.month, 5)
        self.assertEqual(ts.day, 3)
        self.assertEqual(ts.hour, 2+5-1)
        self.assertEqual(ts.minute, 0)
        self.assertEqual(ts.tzinfo, pytz.utc)
