from unittest import TestCase
from pyiso import client_factory
from pyiso.base import BaseClient


class TestPEIClient(TestCase):
    def setUp(self):
        self.c = client_factory('PEI')

    def test_pei_retrievable_from_client_factory(self):
        self.assertIsInstance(self.c, BaseClient)
