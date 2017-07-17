from unittest import TestCase

from pyiso import client_factory
from pyiso.base import BaseClient


class TestNBPowerClient(TestCase):
    def setUp(self):
        self.nbpower_client = client_factory('NBP')

    def test_nbpower_from_client_factory(self):
        self.assertIsInstance(self.nbpower_client, BaseClient)
