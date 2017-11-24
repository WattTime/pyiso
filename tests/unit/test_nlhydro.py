import requests_mock
from unittest import TestCase
from pandas import Timestamp
from pyiso import client_factory
from pyiso.base import BaseClient
from tests import read_fixture


class TestNLHydroClient(TestCase):
    def setUp(self):
        self.c = client_factory('NLH')

    def test_nlhydro_from_client_factory(self):
        self.assertIsInstance(self.c, BaseClient)

    @requests_mock.Mocker()
    def test_get_load_latest(self, mocked_request):
        response_str = read_fixture(self.c.__module__, 'system-information-center.html')
        expected_response = response_str.encode('utf-8')
        mocked_request.get(self.c.SYSTEM_INFO_URL, content=expected_response)

        load_ts = self.c.get_load(latest=True)
        self.assertEqual(len(load_ts), 1)
        self.assertEqual(load_ts[0].get('timestamp', None), Timestamp('2017-10-20T01:45:00Z'))
        self.assertEqual(load_ts[0].get('load_MW', None), 773)
