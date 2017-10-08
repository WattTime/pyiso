from unittest import TestCase
from pyiso import client_factory
from pyiso.base import BaseClient


class YukonEnergyClient(TestCase):
    def setUp(self):
        self.c = client_factory('YUKON')

    def test_yukon_from_client_factory(self):
        self.assertIsInstance(self.c, BaseClient)
