from apps.clients import client_factory
from django.test import TestCase


class TestFactory(TestCase):
    def test_names(self):
        for name in ['ISONE', 'MISO', 'SPP', 'BPA', 'CAISO']:
            c = client_factory(name)
            self.assertIsNotNone(c)
            
    def test_failing(self):
        self.assertRaises(ValueError, client_factory, 'failing')
        