import os
from datetime import datetime
from unittest import TestCase
import pytz
import requests_mock
from pyiso import client_factory
from pyiso.base import BaseClient

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), '../fixtures')


class TestSASK(TestCase):
    def setUp(self):
        self.c = client_factory('SASK')

    def test_sask_from_client_factory(self):
        self.assertIsInstance(self.c, BaseClient)

    @requests_mock.Mocker()
    def test_get_load_latest(self, mocked_request):
        expected_response = open(FIXTURES_DIR + '/sask/sysloadJSON.json').read().encode('utf8')
        mocked_request.get(self.c.SYSLOAD_URL, content=expected_response)

        load_ts = self.c.get_load(latest=True)

        self.assertEqual(len(load_ts), 1)
        self.assertEqual(load_ts[0].get('timestamp', None), datetime(year=2017, month=7, day=23, hour=2, minute=1,
                                                                     second=0, tzinfo=pytz.utc))
        self.assertEqual(load_ts[0].get('load_MW', None), 2712)
