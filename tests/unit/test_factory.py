from os import environ
from pyiso import client_factory
from unittest import TestCase
import inspect


class TestFactory(TestCase):
    def setUp(self):
        self.expected_names = [
          { 
            'name': 'ISONE',
            'env_vars': {
              'ISONE_USERNAME': 'test',
              'ISONE_PASSWORD': 'test',
            },
          },
          {
            'name': 'MISO'
          },
          {
            'name': 'BPA',
          },
          {
            'name': 'CAISO',
          },
          {
            'name': 'ERCOT',
          },
          {
            'name': 'PJM',
          }
        ]

    def test_names(self):
        for name in self.expected_names:
            if 'env_vars' in name:
              for var in name['env_vars']:
                environ[var] = name['env_vars'][var];
            c = client_factory(name['name'])
            self.assertIsNotNone(c)

    def test_failing(self):
        self.assertRaises(ValueError, client_factory, 'failing')

    def test_parent(self):
        """Test all clients are derived from BaseClient"""
        for name in self.expected_names:
            # instantiate
            if 'env_vars' in name:
              for var in name['env_vars']:
                environ[var] = name['env_vars'][var];

            c = client_factory(name['name'])

            # get parent classes
            parent_names = [x.__name__ for x in inspect.getmro(c.__class__)]

            # check for BaseClient
            self.assertIn('BaseClient', parent_names)
