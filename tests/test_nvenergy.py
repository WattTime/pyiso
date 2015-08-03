from pyiso import client_factory
from unittest import TestCase
from datetime import datetime
import logging


class TestNVEnergy(TestCase):
    def setUp(self):
        self.c = client_factory('NVEnergy')
        handler = logging.StreamHandler()
        self.c.logger.addHandler(handler)
        self.c.logger.setLevel(logging.INFO)

    def test_load_latest(self):
        data = self.c.get_load(latest=True)
        self.assertEqual(len(data), 1)

    def test_load_far_past(self):
        data = self.c.get_load(start_at=datetime(2015, 6, 1), end_at=datetime(2015, 6, 3))
        self.assertEqual(data, [])
