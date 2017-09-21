import random
from datetime import datetime, timedelta
from os import environ
from unittest import TestCase

import mock
import pytz

from pyiso import client_factory
from pyiso.base import BaseClient
from pyiso.eia_esod import EIAClient


class BALists:
    can_mex = ['IESO', 'BCTC', 'MHEB', 'AESO', 'HQT', 'NBSO', 'CFE', 'SPC']
    no_load_bas = ['DEAA', 'EEI', 'GRIF', 'GRMA', 'GWA', 'HGMA', 'SEPA', 'WWA', 'YAD']
    delay_bas = ['AEC', 'DOPD', 'GVL', 'HST', 'NSB', 'PGE', 'SCL', 'TAL', 'TIDC', 'TPWR']
    seed = 28127   # Seed for random BA selection
    n_samples = 2  # None returns all BAs from get_BAs

    def __init__(self):
        self.us_bas = [i for i in EIAClient.EIA_BAs if i not in self.can_mex]
        self.load_bas = [i for i in self.us_bas if i not in self.no_load_bas]
        self.no_delay_bas = [i for i in self.load_bas if i not in self.delay_bas]

    def get_BAs(self, name, call_types=None):
        ''' get random sample of BA list, and exclude problem BAs '''
        random.seed(self.seed)
        exclude_list = []
        if call_types:
            if type(call_types) != list:
                call_types = [call_types]
            for t in call_types:
                exclude_list = exclude_list + getattr(self, 'problem_bas_' + t)

        bas = list(set(getattr(self, name)) - set(exclude_list))
        if self.n_samples is None or len(bas) <= self.n_samples:
            return bas
        return random.sample(bas, self.n_samples)


class TestEIA(TestCase):
    def setUp(self):
        environ['EIA_KEY'] = 'test'

        self.c = client_factory("EIA")
        self.longMessage = True
        self.BA_CHOICES = EIAClient.EIA_BAs
        self.BALists = BALists()

    def tearDown(self):
        self.c = None

    def _run_null_response_test(self, ba_name, data_type, **kwargs):
        self.c.set_ba(ba_name)  # set BA name
        self.BALists = BALists()
        # mock request
        with mock.patch.object(self.c, 'request') as mock_request:
            mock_request.return_value = None
            # get data
            if data_type == "gen":
                data = self.c.get_generation(**kwargs)
            elif data_type == "trade":
                data = self.c.get_trade(**kwargs)
            elif data_type == "load":
                data = self.c.get_load(**kwargs)

            self.assertEqual(data, [], msg='BA is %s' % ba_name)

    def test_eiaclient_from_client_factory(self):
        self.assertIsInstance(self.c, BaseClient)


class TestEIAGenMix(TestEIA):
    def test_null_response_latest(self):
        self._run_null_response_test(self.BALists.us_bas[0], data_type="gen", latest=True)


class TestEIALoad(TestEIA):
    def test_null_response(self):
        self._run_null_response_test(self.BALists.load_bas[0], data_type="load", latest=True)

    def test_null_response_latest(self):
        self._run_null_response_test(self.BALists.load_bas[0], data_type="load", latest=True)

    def test_null_response_forecast(self):
        today = datetime.today().replace(tzinfo=pytz.utc)
        self._run_null_response_test(self.BALists.no_delay_bas[0], data_type="load",
                                     start_at=today + timedelta(hours=20),
                                     end_at=today+timedelta(days=2))


class TestEIATrade(TestEIA):
    def test_null_response(self):
        self._run_null_response_test(self.BALists.us_bas[0], data_type="trade", latest=True)
