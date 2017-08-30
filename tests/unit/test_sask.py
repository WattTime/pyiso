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
        self.sask_client = client_factory('SASK')

    def test_nbpower_from_client_factory(self):
        self.assertIsInstance(self.sask_client, BaseClient)

    @requests_mock.Mocker()
    def test_get_load_latest(self, exptected_requests):
        mocked_json = open(FIXTURES_DIR + '/sask/sysloadJSON.json').read().encode('utf8')
        exptected_requests.get(self.sask_client.SYSLOAD_URL, content=mocked_json)

        load_ts = self.sask_client.get_load(latest=True)

        self.assertEqual(len(load_ts), 1)
        self.assertEqual(load_ts[0].get('timestamp', None), datetime(year=2017, month=7, day=23, hour=2, minute=1,
                                                                     second=0, tzinfo=pytz.utc))
        self.assertEqual(load_ts[0].get('load_MW', None), 2712)