import unittest
from unittest import TestCase
from pyiso import client_factory
from pyiso.eia_esod import EIAClient
from datetime import datetime, timedelta
from pyiso import BALANCING_AUTHORITIES
import mock
import pytz

"""Test EIA client.
To use, set the your EIA key as an environment variable:
    export EIA_KEY=my-eia-api-key

Run tests as follows:
    python setup.py test -s tests.test_eia.TestEIA.test_get_generation
"""


class TestEIA(TestCase):
    def setUp(self):
        # bc = BaseClient()
        c = EIAClient()
        self.longMessage = True
        self.MARKET_CHOICES = c.MARKET_CHOICES
        self.FREQUENCY_CHOICES = c.FREQUENCY_CHOICES
        self.FUEL_CHOICES = c.FUEL_CHOICES

        self.BA_CHOICES = c.EIA_BAs
        self.can_mex = ['IESO', 'BCTC', 'MHEB', 'AESO', 'HQT', 'NBSO', 'CFE',
                        'SPC']
        self.us_bas = [i for i in self.BA_CHOICES if i not in self.can_mex]
        self.no_load_bas = ['DEAA', 'EEI', 'GRIF', 'GRMA', 'GWA',
                            'HGMA', 'SEPA', 'WWA', 'YAD']
        self.load_bas = [i for i in self.us_bas if i not in self.no_load_bas]
        self.delay_bas = ['AEC', 'DOPD', 'GVL', 'HST', 'NSB', 'PGE', 'SCL',
                          'TAL', 'TIDC', 'TPWR']
        self.no_delay_bas = [i for i in self.load_bas if i not in self.delay_bas]
        self.problem_bas_gen = ["WWA", "SEPA", "GWA", "SRP", "PSCO", "JEA",
                                "ISNE"]
        self.problem_bas_trade = ["WWA", "GWA", "SCL", "SRP", "JEA",
                                  "ISNE"]
        self.problem_bas_load = ["GRID", "SCL", "SRP", "JEA", "CPLE", "CPLW",
                                 "DUK"]
        self.problem_bas_load_forecast = ["SEC", "OVEC", "MISO", "SRP",
                                          "TEPC", "SC", "PSCO"]

    def tearDown(self):
        self.c = None

    def _run_test(self, ba_name, data_type, **kwargs):
        c = client_factory("EIA")
        c.set_ba(ba_name)         # set BA name
        # get data
        if data_type == "gen":
            data = c.get_generation(**kwargs)
            data_key = 'gen_MW'
        elif data_type == "trade":
            data = c.get_trade(**kwargs)
            data_key = 'net_exp_MW'
        elif data_type == "load":
            data = c.get_load(**kwargs)
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
            self.assertIn(dp['ba_name'], self.BA_CHOICES,
                          msg='BA is %s' % ba_name)

            if data_type == "load" and c.options["forecast"]:
                self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.dam)
                self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.dam)
            else:
                self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
                self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

            # test for numeric value
            self.assertGreaterEqual(dp[data_key]+1, dp[data_key],
                                    msg='BA is %s' % ba_name)

            # test correct temporal relationship to now
            if c.options['forecast']:
                self.assertGreaterEqual(dp['timestamp'],
                                        pytz.utc.localize(datetime.utcnow()),
                                        msg='BA is %s' % ba_name)
            else:
                self.assertLess(dp['timestamp'],
                                pytz.utc.localize(datetime.utcnow()),
                                msg='BA is %s' % ba_name)

        timestamps = [d['timestamp'] for d in data]
        if c.options["latest"]:
            self.assertEqual(len(set(timestamps)), 1)
        if c.options['forecast']:
            self.assertGreaterEqual(len(set(timestamps)), 1)
        else:
            self.assertGreater(len(set(timestamps)), 1)

        return data

    def _run_null_response_test(self, ba_name, data_type, **kwargs):
        c = client_factory("EIA")
        c.set_ba(ba_name)         # set BA name

        # mock request
        with mock.patch.object(c, 'request') as mock_request:
            mock_request.return_value = None
            # get data
            if data_type == "gen":
                data = c.get_generation(**kwargs)
            elif data_type == "trade":
                data = c.get_trade(**kwargs)
            elif data_type == "load":
                data = c.get_load(**kwargs)

            self.assertEqual(data, [], msg='BA is %s' % ba_name)


class TestEIAGenMix(TestEIA):

    def test_null_response_latest(self):
        self._run_null_response_test(self.us_bas[0], data_type="gen",
                                     latest=True)

    def test_yesterday_delay_raises_valueerror(self):
        for ba in self.delay_bas:
            with self.assertRaises(ValueError):
                self._run_test(ba, data_type="gen", yesterday=True,
                               market=self.MARKET_CHOICES.hourly)

    def test_yesterday_no_delay(self):
        for ba in self.no_delay_bas:
            # basic test
            if ba in self.problem_bas_gen:
                continue
            self._run_test(ba, data_type="gen", yesterday=True,
                           market=self.MARKET_CHOICES.hourly)

    def test_date_range_delay_raises_valueerror(self):
        today = datetime.today().replace(tzinfo=pytz.utc)
        four_days_ago = today - timedelta(days=4)
        three_days_ago = today - timedelta(days=3)
        for ba in self.delay_bas:
            with self.assertRaises(ValueError):
                self._run_test(ba, data_type="gen", start_at=four_days_ago,
                               end_at=three_days_ago,
                               market=self.MARKET_CHOICES.hourly)

    def test_date_range_no_delay(self):
        for ba in self.no_delay_bas:
            if ba in self.problem_bas_gen:
                continue
            self._test_date_range(ba)

    def test_all_us_bas(self):
        for ba in self.us_bas:
            if ba in self.problem_bas_gen:
                continue
            self._run_test(ba, data_type="gen",
                           market=self.MARKET_CHOICES.hourly)

    def test_all_latest(self):
        for ba in self.us_bas:
            if ba in self.problem_bas_gen:
                continue
            self._test_latest(ba)

    def test_non_us_bas_raise_valueerror(self):
        for ba in self.can_mex:
            with self.assertRaises(ValueError):
                self._run_test(ba, data_type="gen",
                               market=self.MARKET_CHOICES.hourly)

    def test_get_generation_with_forecast_raises_valueerror(self):
        for ba in self.us_bas:
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
                       end_at=today, market=self.MARKET_CHOICES.hourly)

    def _run_notimplemented_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)
        # method not implemented yet
        self.assertRaises(NotImplementedError, c.get_generation)


class TestEIALoad(TestEIA):

    def test_null_response(self):
        self._run_null_response_test(self.load_bas[0], data_type="load",
                                     latest=True)

    def test_null_response_latest(self):
        self._run_null_response_test(self.load_bas[0], data_type="load",
                                     latest=True)

    def test_null_response_forecast(self):
        today = datetime.today().replace(tzinfo=pytz.utc)
        self._run_null_response_test(self.no_delay_bas[0], data_type="load",
                                     start_at=today + timedelta(hours=20),
                                     end_at=today+timedelta(days=2))

    def test_latest_all(self):
        for ba in self.load_bas:
            if ba in self.problem_bas_load:
                continue
            self._test_latest(ba)

    def test_get_load_yesterday(self):
        for ba in self.no_delay_bas:
            if ba in self.problem_bas_load:
                continue
            self._run_test(ba, data_type="load", yesterday=True)

    def test_date_range_all(self):
        for ba in self.load_bas:
            if ba in self.problem_bas_load:
                continue
            self._test_date_range(ba)

    def test_date_range_strings_all(self):
        for ba in self.load_bas:
            # basic test
            if ba in self.problem_bas_load:
                continue
            self._run_test(ba, data_type="load", start_at='2016-05-01',
                           end_at='2016-05-03')

    def test_date_range_farpast_all(self):
        for ba in self.load_bas:
            if ba in self.problem_bas_load:
                continue
            # basic test
            today = datetime.today().replace(tzinfo=pytz.utc)
            self._run_test(ba, data_type="load",
                           start_at=today-timedelta(days=20),
                           end_at=today-timedelta(days=10))

    def test_no_delay_bas_return_last_two_days(self):
        today = datetime.today().replace(tzinfo=pytz.utc)
        two_days_ago = today - timedelta(days=2)
        for ba in self.no_delay_bas:
            if ba in self.problem_bas_load:
                continue
            self._run_test(ba, data_type="load", start_at=two_days_ago,
                           end_at=today)

    def test_delay_bas_raise_date_value_error(self):
        today = datetime.today().replace(tzinfo=pytz.utc)
        two_days_ago = today - timedelta(days=2)
        for ba in self.delay_bas:
            with self.assertRaises(ValueError):
                self._run_test(ba, data_type="load", start_at=two_days_ago,
                               end_at=today)

    def test_get_load_with_unsupported_ba_raises_valueerror(self):
        for ba in self.no_load_bas:
            with self.assertRaises(ValueError):
                self._run_test(ba, data_type="load",
                               market=self.MARKET_CHOICES.hourly)

    def test_forecast_all(self):
        for ba in self.no_delay_bas:
            if ba in self.problem_bas_load_forecast:
                continue
            if ba in self.problem_bas_load:
                continue
            self._test_forecast(ba)

    def test_all_us_bas(self):
        for ba in self.load_bas:
            if ba in self.problem_bas_load:
                continue
            self._run_test(ba, data_type="load",
                           market=self.MARKET_CHOICES.hourly)

    def test_non_load_bas_raise_value_error(self):
        for ba in self.no_load_bas:
            with self.assertRaises(ValueError):
                self._run_test(ba, data_type="load",
                               market=self.MARKET_CHOICES.hourly)

    def test_non_us_bas_raise_valueerror(self):
        for ba in self.can_mex:
            with self.assertRaises(ValueError):
                self._run_test(ba, data_type="load",
                               market=self.MARKET_CHOICES.hourly)

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
                       market=self.MARKET_CHOICES.hourly)


class TestEIATrade(TestEIA):
    def test_null_response(self):
        self._run_null_response_test(self.us_bas[0], data_type="trade",
                                     latest=True)

    def test_latest_all(self):
        for ba in self.us_bas:
            # basic test
            self._run_test(ba, data_type="trade", latest=True,
                           market=self.MARKET_CHOICES.hourly)

    def test_date_range_no_delay(self):
        for ba in self.no_delay_bas:
            # basic test
            if ba in self.problem_bas_trade:
                continue
            today = datetime.today().replace(tzinfo=pytz.utc)
            self._run_test(ba, data_type="trade",
                           start_at=today-timedelta(days=2),
                           end_at=today-timedelta(days=1))

    def test_date_range_delay(self):
        for ba in self.delay_bas:
            if ba in self.problem_bas_trade:
                continue
            # basic test
            today = datetime.today().replace(tzinfo=pytz.utc)
            self._run_test(ba, data_type="trade",
                           start_at=today-timedelta(days=4),
                           end_at=today-timedelta(days=3))

    def test_forecast_raises_valueerror(self):
        """Ensure get trade with forecast raises an error."""
        for ba in self.no_delay_bas:
            if ba in self.problem_bas_trade:
                continue
            with self.assertRaises(ValueError):
                self._run_test(ba, data_type="trade", forecast=True)

    def test_date_range_future_raises_valueerror(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        for ba in self.us_bas:
            with self.assertRaises(ValueError):
                self._run_test(ba, data_type="trade",
                               start_at=today+timedelta(days=1),
                               end_at=today+timedelta(days=2))

    def test_get_trade_yesterday(self):
        for ba in self.no_delay_bas:
            if ba in self.problem_bas_trade:
                continue
            self._run_test(ba, data_type="trade", yesterday=True)

    def test_all_us_bas(self):
        for ba in self.us_bas:
            self._run_test(ba, data_type="trade",
                           market=self.MARKET_CHOICES.hourly)

    def test_non_us_bas_raise_valueerror(self):
        for ba in self.can_mex:
            with self.assertRaises(ValueError):
                self._run_test(ba, data_type="trade",
                               market=self.MARKET_CHOICES.hourly)

if __name__ == '__main__':
    unittest.main()
