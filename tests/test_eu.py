from pyiso import client_factory
from unittest import TestCase
from datetime import datetime
import pytz
import logging


class TestEU(TestCase):
    def setUp(self):
        self.c = client_factory('EU')
        handler = logging.StreamHandler()
        self.c.logger.addHandler(handler)
        self.c.logger.setLevel(logging.INFO)

    def test_auth(self):
        self.assertTrue(self.c.auth())

    def test_get_load(self):
        r = self.c.get_load('CTA|IT', start_at=datetime(2015, 9, 6, 0, tzinfo=pytz.utc),
                            end_at=datetime(2015, 9, 7, 0, tzinfo=pytz.utc))
        self.assertEqual(len(r), 24)

        self.assertEqual(r[12]['load_MW'], 26745)

    def test_get_forecast_load(self):
        r = self.c.get_load('CTA|IT', forecast=True,
                            start_at=datetime(2015, 9, 6, 0, tzinfo=pytz.utc),
                            end_at=datetime(2015, 9, 7, 0, tzinfo=pytz.utc),)
        self.assertEqual(len(r), 24)

        self.assertEqual(r[12]['load_MW'], 27780)
