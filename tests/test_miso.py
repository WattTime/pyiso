from pyiso import client_factory, LOG_LEVEL
from unittest import TestCase
import pytz
import logging
from os import environ


class TestMISO(TestCase):
    def setUp(self):
        self.c = client_factory('MISO')
        handler = logging.StreamHandler()
        self.c.logger.addHandler(handler)
        self.c.logger.setLevel(LOG_LEVEL)

    def test_utcify(self):
        ts_str = '2014-05-03T01:45:00'
        ts = self.c.utcify(ts_str)
        self.assertEqual(ts.year, 2014)
        self.assertEqual(ts.month, 5)
        self.assertEqual(ts.day, 3)
        self.assertEqual(ts.hour, 1+5)
        self.assertEqual(ts.minute, 45)
        self.assertEqual(ts.tzinfo, pytz.utc)
