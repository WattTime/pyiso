from unittest import TestCase
from pyiso import client_factory
from pyiso.base import BaseClient


class TestNSPower(TestCase):
    def setUp(self):
        self.c = client_factory('NSP')

    def test_nspower_from_client_factory(self):
        self.assertIsInstance(self.c, BaseClient)
