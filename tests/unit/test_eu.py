import os
from pyiso import client_factory
from unittest import TestCase
import mock
from mock import patch
from datetime import timedelta

fixtures_base_path = os.path.join(os.path.dirname(__file__), '../fixtures/eu')

class TestEU(TestCase):
    def setUp(self):
        os.environ['ENTSOe_SECURITY_TOKEN'] = 'test'
        self.c = client_factory('EU')

    def test_bad_control_area(self):
        self.assertRaises(ValueError, self.c.get_load, 'not-a-cta', latest=True)

    def test_parse_resolution(self):
        resolution = self.c.parse_resolution('PT15M')
        self.assertEqual(resolution, timedelta(minutes=15))

    def test_parse_load(self):
        self.c.handle_options(latest=True, control_area='DE(TenneT GER)', data='load')
        with open(os.path.join(fixtures_base_path, 'de_load.xml'), 'r') as report:
          parsed = self.c.parse_response(report.read().encode('ascii'))
          self.assertEqual(parsed[-1]['load_MW'], 13926)

    def test_parse_gen(self):
        self.c.handle_options(latest=True, control_area='DE(TenneT GER)', data='gen')
        with open(os.path.join(fixtures_base_path, 'de_gen.xml'), 'r') as report:
          parsed = self.c.parse_response(report.read().encode('ascii'))
          self.assertEqual(parsed[-1]['gen_MW'], 3816)
          self.assertEqual(parsed[-1]['fuel_name'], 'nuclear')

