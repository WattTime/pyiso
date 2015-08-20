from pyiso import client_factory
from unittest import TestCase
from io import StringIO
from datetime import datetime, timedelta
try:
    from urllib2 import HTTPError
except ImportError:
    from urllib.error import HTTPError
import logging
import pytz
import pprint


class TestSVERI(TestCase):
    def setUp(self):
        self.c = client_factory('SVERI')
        handler = logging.StreamHandler()
        self.c.logger.addHandler(handler)
        self.c.logger.setLevel(logging.INFO)

        self.now = pytz.utc.localize(datetime.utcnow())
        self.yesterday = self.now - timedelta(days=1)
        self.tomorrow = self.now + timedelta(days=1)
        self.last_month = self.now - timedelta(days=32)
        self.next_month = self.now + timedelta(days=32)
        self.pp = pprint.PrettyPrinter(indent=2)

    def test_get_generation_latest(self):
        lst = self.c.get_generation(latest=True)
        self.assertNotEquals(len(lst), 0)

    def test_get_generation_today(self):
        lst = self.c.get_generation(start_at=self.now, end_at=self.tomorrow)
        self.assertNotEquals(len(lst), 0)

    def test_get_load_latest(self):
        lst = self.c.get_load(latest=True)
        self.assertNotEquals(len(lst), 0)

    def test_get_load_today(self):
        lst = self.c.get_load(start_at=self.now, end_at=self.tomorrow)
        self.assertNotEquals(len(lst), 0)
