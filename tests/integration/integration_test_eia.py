import random
from datetime import timedelta, datetime
from unittest import TestCase

import pytz
from parameterized import parameterized

from pyiso import client_factory
from pyiso.eia_esod import EIAClient


class BALists:
    can_mex = ['IESO', 'BCTC', 'MHEB', 'AESO', 'HQT', 'NBSO', 'CFE', 'SPC']
    no_load_bas = ['DEAA', 'EEI', 'GRIF', 'GRMA', 'GWA', 'HGMA', 'SEPA', 'WWA', 'YAD']
    delay_bas = ['AEC', 'DOPD', 'GVL', 'HST', 'NSB', 'PGE', 'SCL', 'TAL', 'TIDC', 'TPWR']
    problem_bas_gen = ["WWA", "SEPA", "GWA", "SRP", "PSCO", "JEA", "BPAT"]
    problem_bas_trade = ["WWA", "GWA", "SCL", "SRP", "JEA", "BPAT"]
    problem_bas_load = ["GRID", "SCL", "SRP", "JEA", "CPLE", "CPLW", "DUK"]
    problem_bas_load_forecast = ["SEC", "OVEC", "MISO", "SRP", "TEPC", "SC", "PSCO"]
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


BALISTS = BALists()
BALISTS.n_samples = 2


class IntegrationTestEIAClient(TestCase):
    """
    Integration tests for the EIA client. To use, set the your EIA key as an environment variable:

        export EIA_KEY=my-eia-api-key

    Run tests as follows:

        python setup.py test -s tests.integration.integration_test_eia.IntegrationTestEIALoad.test_latest_all
    """

    def setUp(self):
        self.c = client_factory("EIA")
        self.longMessage = True
        self.BALists = BALists()

    def tearDown(self):
        self.c = None

    def _run_test(self, ba_name, data_type, **kwargs):
        self.c.set_ba(ba_name)         # set BA name
        # get data
        if data_type == "gen":
            data = self.c.get_generation(**kwargs)
            data_key = 'gen_MW'
        elif data_type == "trade":
            data = self.c.get_trade(**kwargs)
            data_key = 'net_exp_MW'
        elif data_type == "load":
            data = self.c.get_load(**kwargs)
            data_key = 'load_MW'

        # test number
        self.assertGreaterEqual(len(data), 1, msg='BA is %s' % ba_name)

        # test contents
        for dp in data:
            if data_type == "gen":
                self.assertEqual(set([data_key, 'ba_name',
                                      'timestamp', 'freq', 'market',
                                      'fuel_name']),
                                 set(dp.keys()), msg='BA is %s' % ba_name)
                # add fuel check
                self.assertEqual(dp['fuel_name'], 'other')
            else:
                self.assertEqual(set([data_key, 'ba_name',
                                      'timestamp', 'freq', 'market']),
                                 set(dp.keys()), msg='BA is %s' % ba_name)

            # test values
            self.assertEqual(dp['timestamp'].tzinfo, pytz.utc,
                             msg='BA is %s' % ba_name)
            self.assertIn(dp['ba_name'], self.c.EIA_BAs,
                          msg='BA is %s' % ba_name)

            if data_type == "load" and self.c.options["forecast"]:
                self.assertEqual(dp['freq'], self.c.FREQUENCY_CHOICES.dam,
                                 msg='BA is %s' % ba_name)
                self.assertEqual(dp['freq'], self.c.FREQUENCY_CHOICES.dam,
                                 msg='BA is %s' % ba_name)
            else:
                self.assertEqual(dp['market'], self.c.MARKET_CHOICES.hourly,
                                 msg='BA is %s' % ba_name)
                self.assertEqual(dp['freq'], self.c.FREQUENCY_CHOICES.hourly,
                                 msg='BA is %s' % ba_name)

            # test for numeric value
            self.assertGreaterEqual(dp[data_key]+1, dp[data_key],
                                    msg='BA is %s' % ba_name)

            # test correct temporal relationship to now
            if self.c.options['forecast']:
                self.assertGreaterEqual(dp['timestamp'],
                                        pytz.utc.localize(datetime.utcnow()),
                                        msg='BA is %s' % ba_name)
            else:
                self.assertLess(dp['timestamp'],
                                pytz.utc.localize(datetime.utcnow()),
                                msg='BA is %s' % ba_name)

        timestamps = [d['timestamp'] for d in data]
        if self.c.options["latest"]:
            self.assertEqual(len(set(timestamps)), 1,
                             msg='BA is %s' % ba_name)
        elif self.c.options['forecast']:
            self.assertGreaterEqual(len(set(timestamps)), 1,
                                    msg='BA is %s' % ba_name)
        else:
            self.assertGreater(len(set(timestamps)), 1,
                               msg='BA is %s' % ba_name)

        return data


class IntegrationTestEIALoad(IntegrationTestEIAClient):
    @parameterized.expand(BALISTS.get_BAs('load_bas', 'load'))
    def test_latest_all(self, ba):
        self._test_latest(ba)

    @parameterized.expand(BALISTS.get_BAs('load_bas', 'load'))
    def test_get_load_yesterday(self, ba):
        self._run_test(ba, data_type="load", yesterday=True)

    @parameterized.expand(BALISTS.get_BAs('load_bas', 'load'))
    def test_date_range_all(self, ba):
        self._test_date_range(ba)

    @parameterized.expand(BALISTS.get_BAs('load_bas', 'load'))
    def test_date_range_strings_all(self, ba):
        # basic test
        self._run_test(ba, data_type="load", start_at='2016-05-01',
                       end_at='2016-05-03')

    @parameterized.expand(BALISTS.get_BAs('load_bas', 'load'))
    def test_date_range_farpast_all(self, ba):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        self._run_test(ba, data_type="load",
                       start_at=today-timedelta(days=20),
                       end_at=today-timedelta(days=10))

    @parameterized.expand(BALISTS.get_BAs('no_delay_bas', 'load'))
    def test_no_delay_bas_return_last_two_days(self, ba):
        today = datetime.today().replace(tzinfo=pytz.utc)
        two_days_ago = today - timedelta(days=2)
        self._run_test(ba, data_type="load", start_at=two_days_ago,
                       end_at=today)

    @parameterized.expand(BALISTS.get_BAs('delay_bas'))
    def test_delay_bas_raise_date_value_error(self, ba):
        today = datetime.today().replace(tzinfo=pytz.utc)
        two_days_ago = today - timedelta(days=2)
        with self.assertRaises(ValueError):
            self._run_test(ba, data_type="load", start_at=two_days_ago,
                           end_at=today)

    @parameterized.expand(BALISTS.get_BAs('no_load_bas'))
    def test_get_load_with_unsupported_ba_raises_valueerror(self, ba):
        with self.assertRaises(ValueError):
            self._run_test(ba, data_type="load",
                           market=self.c.MARKET_CHOICES.hourly)

    @parameterized.expand(BALISTS.get_BAs('no_delay_bas', ['load', 'load_forecast']))
    def test_forecast_all(self, ba):
        self._test_forecast(ba)

    @parameterized.expand(BALISTS.get_BAs('load_bas', 'load'))
    def test_all_us_bas(self, ba):
        self._run_test(ba, data_type="load",
                       market=self.c.MARKET_CHOICES.hourly)

    @parameterized.expand(BALISTS.get_BAs('no_load_bas'))
    def test_non_load_bas_raise_value_error(self, ba):
        with self.assertRaises(ValueError):
            self._run_test(ba, data_type="load",
                           market=self.c.MARKET_CHOICES.hourly)

    @parameterized.expand(BALISTS.get_BAs('can_mex'))
    def test_non_us_bas_raise_valueerror(self, ba):
        with self.assertRaises(ValueError):
            self._run_test(ba, data_type="load",
                           market=self.c.MARKET_CHOICES.hourly)

    def _test_forecast(self, ba):
        # Used 5 hours/1 day instead of 20/2 for one day forecast
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test(ba, data_type="load",
                              start_at=today + timedelta(hours=5),
                              end_at=today+timedelta(days=1))
        # test timestamps in range
        timestamps = [d['timestamp'] for d in data]
        self.assertGreaterEqual(min(timestamps), today+timedelta(hours=5))
        self.assertLessEqual(min(timestamps), today+timedelta(days=1))

    def _test_latest(self, ba):
        # basic test
        self._run_test(ba, data_type="load", latest=True)
        # test all timestamps are equal

    def _test_date_range(self, ba):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        self._run_test(ba, data_type="load",
                       start_at=today - timedelta(days=3),
                       end_at=today - timedelta(days=2),
                       market=self.c.MARKET_CHOICES.hourly)


class IntegrationTestEIAGenMix(IntegrationTestEIAClient):
    @parameterized.expand(BALISTS.get_BAs('delay_bas'))
    def test_yesterday_delay_raises_valueerror(self, ba):
        with self.assertRaises(ValueError):
            self._run_test(ba, data_type="gen", yesterday=True,
                           market=self.c.MARKET_CHOICES.hourly)

    @parameterized.expand(BALISTS.get_BAs('no_delay_bas', 'gen'))
    def test_yesterday_no_delay(self, ba):
        # basic test
        self._run_test(ba, data_type="gen", yesterday=True,
                       market=self.c.MARKET_CHOICES.hourly)

    @parameterized.expand(BALISTS.get_BAs('delay_bas'))
    def test_date_range_delay_raises_valueerror(self, ba):
        today = datetime.today().replace(tzinfo=pytz.utc)
        four_days_ago = today - timedelta(days=4)
        three_days_ago = today - timedelta(days=3)
        with self.assertRaises(ValueError):
            self._run_test(ba, data_type="gen", start_at=four_days_ago,
                           end_at=three_days_ago,
                           market=self.c.MARKET_CHOICES.hourly)

    @parameterized.expand(BALISTS.get_BAs('no_delay_bas', 'gen'))
    def test_date_range_no_delay(self, ba):
        self._test_date_range(ba)

    @parameterized.expand(BALISTS.get_BAs('us_bas', 'gen'))
    def test_all_us_bas(self, ba):
        self._run_test(ba, data_type="gen",
                       market=self.c.MARKET_CHOICES.hourly)

    @parameterized.expand(BALISTS.get_BAs('us_bas', 'gen'))
    def test_latest_all(self, ba):
        self._test_latest(ba)

    @parameterized.expand(BALISTS.get_BAs('can_mex'))
    def test_non_us_bas_raise_valueerror(self, ba):
        with self.assertRaises(ValueError):
            self._run_test(ba, data_type="gen",
                           market=self.c.MARKET_CHOICES.hourly)

    @parameterized.expand(BALISTS.get_BAs('us_bas'))
    def test_get_generation_with_forecast_raises_valueerror(self, ba):
        with self.assertRaises(ValueError):
            self._run_test(ba, data_type="gen", forecast=True)

    def _test_latest(self, ba):
        # basic test
        data = self._run_test(ba, data_type="gen", latest=True)
        return data

    def _test_date_range(self, ba):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        self._run_test(ba, data_type="gen",
                       start_at=today - timedelta(days=1),
                       end_at=today, market=self.c.MARKET_CHOICES.hourly)

    def _run_notimplemented_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)
        # method not implemented yet
        self.assertRaises(NotImplementedError, c.get_generation)


class IntegrationTestEIATrade(IntegrationTestEIAClient):
    @parameterized.expand(BALISTS.get_BAs('us_bas'))
    def test_latest_all(self, ba):
        # basic test
        self._run_test(ba, data_type="trade", latest=True,
                       market=self.c.MARKET_CHOICES.hourly)

    @parameterized.expand(BALISTS.get_BAs('load_bas', 'trade'))
    def test_date_range_no_delay(self, ba):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        self._run_test(ba, data_type="trade",
                       start_at=today-timedelta(days=2),
                       end_at=today-timedelta(days=1))

    @parameterized.expand(BALISTS.get_BAs('delay_bas', 'trade'))
    def test_date_range_delay(self, ba):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        self._run_test(ba, data_type="trade",
                       start_at=today-timedelta(days=4),
                       end_at=today-timedelta(days=3))

    @parameterized.expand(BALISTS.get_BAs('delay_bas', 'trade'))
    def test_forecast_raises_valueerror(self, ba):
        """ensure get trade with forecast raises an error."""
        with self.assertRaises(ValueError):
            self._run_test(ba, data_type="trade", forecast=True)

    @parameterized.expand(BALISTS.get_BAs('us_bas'))
    def test_date_range_future_raises_valueerror(self, ba):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        with self.assertRaises(ValueError):
            self._run_test(ba, data_type="trade",
                           start_at=today+timedelta(days=1),
                           end_at=today+timedelta(days=2))

    @parameterized.expand(BALISTS.get_BAs('no_delay_bas', 'trade'))
    def test_get_trade_yesterday(self, ba):
        self._run_test(ba, data_type="trade", yesterday=True)

    @parameterized.expand(BALISTS.get_BAs('us_bas'))
    def test_all_us_bas(self, ba):
        self._run_test(ba, data_type="trade",
                       market=self.c.MARKET_CHOICES.hourly)

    @parameterized.expand(BALISTS.get_BAs('can_mex'))
    def test_non_us_bas_raise_valueerror(self, ba):
        with self.assertRaises(ValueError):
            self._run_test(ba, data_type="trade",
                           market=self.c.MARKET_CHOICES.hourly)
