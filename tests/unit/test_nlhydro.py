from unittest import TestCase

from pyiso import client_factory
from pyiso.base import BaseClient


class TestNLHydroClient(TestCase):
    def setUp(self):
        self.c = client_factory('NLH')

    def test_nlhydro_from_client_factory(self):
        self.assertIsInstance(self.c, BaseClient)
