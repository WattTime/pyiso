import os

import pytz
import requests_mock
from datetime import datetime
from unittest import TestCase
from pyiso import client_factory
from pyiso.base import BaseClient


def read_fixture(ba_name, filename):
    """
    :param str ba_name: The balancing authority name.
    :param str filename: The fixture file you wish to load.
    :return: The file's content.
    :rtype: str
    """
    fixtures_base_path = os.path.join(os.path.dirname(__file__), '../fixtures', ba_name.lower())
    return open(os.path.join(fixtures_base_path, filename), 'r').read()


class TestPEIClient(TestCase):
    def setUp(self):
        self.c = client_factory('PEI')

    def test_pei_retrievable_from_client_factory(self):
        self.assertIsInstance(self.c, BaseClient)

    @requests_mock.Mocker()
    def test_get_load_success(self, mock_request):
        expected_response = read_fixture(self.c.NAME, 'chart-values.json').encode('utf8')
        mock_request.get('http://www.gov.pe.ca/windenergy/chart-values.php', content=expected_response)

        load_ts = self.c.get_load(latest=True)

        self.assertEqual(len(load_ts), 1)
        self.assertEqual(load_ts[0].get('timestamp', None), datetime(year=2017, month=9, day=25, hour=10, minute=1,
                                                                     second=1, microsecond=0, tzinfo=pytz.utc))
        self.assertEqual(load_ts[0].get('load_MW', None), 150.56)

    @requests_mock.Mocker()
    def test_get_generation_success(self, mock_request):
        expected_response = read_fixture(self.c.NAME, 'chart-values.json').encode('utf8')
        mock_request.get('http://www.gov.pe.ca/windenergy/chart-values.php', content=expected_response)
        expected_timestamp = datetime(year=2017, month=9, day=25, hour=10, minute=1, second=1, microsecond=0,
                                      tzinfo=pytz.utc)

        load_ts = self.c.get_generation(latest=True)

        self.assertEqual(len(load_ts), 2)
        self.assertEqual(load_ts[0].get('timestamp', None), expected_timestamp)
        self.assertEqual(load_ts[0].get('fuel_name', None), 'wind')
        self.assertEqual(load_ts[0].get('gen_MW', None), 4.55)
        self.assertEqual(load_ts[1].get('timestamp', None), expected_timestamp)
        self.assertEqual(load_ts[1].get('fuel_name', None), 'other')
        self.assertEqual(load_ts[1].get('gen_MW', None), 146.01)
