from pyiso import client_factory
from unittest import TestCase
import inspect


class TestFactory(TestCase):
    def setUp(self):
        self.expected_names = ['ISONE', 'MISO', 'SPP', 'BPA', 'CAISO', 'ERCOT', 'PJM']

    def test_names(self):
        for name in self.expected_names:
            c = client_factory(name)
            self.assertIsNotNone(c)

    def test_failing(self):
        self.assertRaises(ValueError, client_factory, 'failing')

    def test_parent(self):
        """Test all clients are derived from BaseClient"""
        for name in self.expected_names:
            # instantiate
            c = client_factory(name)

            # get parent classes
            parent_names = [x.__name__ for x in inspect.getmro(c.__class__)]

            # check for BaseClient
            self.assertIn('BaseClient', parent_names)
