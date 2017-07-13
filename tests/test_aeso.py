from unittest import TestCase

from pyiso import client_factory
from pyiso.base import BaseClient


class TestAESOClient(TestCase):
    def setUp(self):
        self.aeso_client = client_factory('AESO')

    def test_aeso_retrievable_from_client_factory(self):
        self.assertIsInstance(self.aeso_client, BaseClient)
