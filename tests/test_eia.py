import unittest
from unittest import TestCase
from pyiso import client_factory
from pyiso.base import BaseClient
from pyiso.eia_esod import EIACLIENT
from datetime import datetime, timedelta
from dateutil.parser import parse as dateutil_parse
from pyiso import BALANCING_AUTHORITIES
import mock
import pytz

"""Test EIA client.
To use, set the your EIA key as an environment variable:
    export EIA_KEY=my-eia-api-key
"""

# Start here:
# Then figure out how to deal with problem BAs more gracefully. Try/except?
# More error handling would be good- try/except
# PR!


class TestEIA(TestCase):
    def setUp(self):
        # bc = BaseClient()
        c = EIACLIENT()
        self.MARKET_CHOICES = c.MARKET_CHOICES
        self.FREQUENCY_CHOICES = c.FREQUENCY_CHOICES
        self.FUEL_CHOICES = c.FUEL_CHOICES

        self.BA_CHOICES = [k for k, v in BALANCING_AUTHORITIES.items() if v['module'] == 'eia_esod']
        self.can_mex = ['IESO', 'BCTC', 'MHEB', 'AESO', 'HQT', 'NBSO', 'CFE',
                        'SPC']
        self.us_bas = [i for i in self.BA_CHOICES if i not in self.can_mex]
        self.no_load_bas = ['DEAA-EIA', 'EEI', 'GRIF-EIA', 'GRMA', 'GWA',
                            'HGMA-EIA', 'SEPA', 'WWA', 'YAD']
        self.load_bas = [i for i in self.us_bas if i not in self.no_load_bas]
        self.delay_bas = ['AEC', 'DOPD', 'GVL', 'HST', 'NSB', 'PGE', 'SCL',
                          'TAL', 'TIDC', 'TPWR']
        self.no_delay_bas = [i for i in self.load_bas if i not in self.delay_bas]
        self.problem_bas_gen = ["WWA", "SEPA", "GWA", "SRP-EIA", "PSCO"]
        self.problem_bas_trade = ["WWA", "GWA", "SCL", "SRP-EIA"]
        self.problem_bas_load = ["GRID", "SCL", "SRP-EIA"]
        self.problem_bas_load_forecast = ["SEC", "OVEC", "MISO-EIA", "SRP-EIA",
                                          "TEPC-EIA", "SC", "PSCO"]

    def tearDown(self):
        self.c = None


class TestEIAGenMix(TestEIA):

    def test_null_response_latest(self):
        self._run_null_response_test(self.us_bas[0], latest=True)

    def test_yesterday_delay_raises_valueerror(self):
        for ba in self.delay_bas:
            with self.assertRaises(ValueError):
                self._run_test(ba, yesterday=True,
                               market=self.MARKET_CHOICES.hourly)

    def test_yesterday_no_delay(self):
        for ba in self.no_delay_bas:
            # basic test
            if ba in self.problem_bas_gen:
                continue
            data = self._run_test(ba, yesterday=True,
                                  market=self.MARKET_CHOICES.hourly)

            # test timestamps are different
            timestamps = [d['timestamp'] for d in data]
            self.assertGreater(len(set(timestamps)), 1)

            # test flags
            for dp in data:
                self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
                self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

            # test fuel names
            fuels = set([d['fuel_name'] for d in data])
            expected_fuels = ['other']
            # changed to other based on https://github.com/WattTime/pyiso/issues/97
            for expfuel in expected_fuels:
                self.assertIn(expfuel, fuels)

    def test_date_range_delay_raises_valueerror(self):
        today = datetime.today().replace(tzinfo=pytz.utc)
        four_days_ago = today - timedelta(days=4)
        three_days_ago = today - timedelta(days=3)
        for ba in self.delay_bas:
            with self.assertRaises(ValueError):
                self._run_test(ba, start_at=four_days_ago,
                               end_at=three_days_ago,
                               market=self.MARKET_CHOICES.hourly)

    def test_date_range_no_delay(self):
        for ba in self.no_delay_bas:
            if ba in self.problem_bas_gen:
                continue
            self._test_date_range(ba)

    def test_all_us_bas(self):
        for ba in self.us_bas:
            data = self._run_test(ba, market=self.MARKET_CHOICES.hourly)
            self.assertGreater(len(data), 1)

    def test_all_latest(self):
        for ba in self.us_bas:
            data = self._run_test(ba, latest=True)
            self.assertGreater(len(data), 1)

    def test_non_us_bas_raise_valueerror(self):
        for ba in self.can_mex:
            with self.assertRaises(ValueError):
                self._run_test(ba, market=self.MARKET_CHOICES.hourly)

    def test_get_generation_with_forecast_raises_valueerror(self):
        for ba in self.us_bas:
            with self.assertRaises(ValueError):
                self._run_test(ba, forecast=True)

    def test_get_generation_naive_start_at(self):
        for ba in self.us_bas:
            self._run_test(ba, start_at="20161212", end_at="20161222T04Z")

    def test_get_generation_naive_end_at(self):
        for ba in self.us_bas:
            self._run_test(ba, start_at="20161212T04Z", end_at="20161222")

    def _test_latest(self, ba):
        # basic test
        data = self._run_test(ba, latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

        # test fuel names
        fuels = set([d['fuel_name'] for d in data])
        expected_fuels = ['other']
        for expfuel in expected_fuels:
            self.assertIn(expfuel, fuels)

    def _test_date_range(self, ba):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test(ba, start_at=today - timedelta(days=1),
                              end_at=today, market=self.MARKET_CHOICES.hourly)
        # data only available for previous + current day

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

        # test fuel names
        fuels = set([d['fuel_name'] for d in data])
        expected_fuels = ['other']
        for expfuel in expected_fuels:
            self.assertIn(expfuel, fuels)

    def _run_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)

        # get data
        data = c.get_generation(**kwargs)

        # test number
        self.assertGreater(len(data), 1)

        # test contents
        for dp in data:
            # test key names
            self.assertEqual(set(['gen_MW', 'ba_name', 'fuel_name',
                                  'timestamp', 'freq', 'market']),
                             set(dp.keys()))

            # test values
            self.assertEqual(dp['timestamp'].tzinfo, pytz.utc)
            self.assertIn(dp['fuel_name'], self.FUEL_CHOICES)
            self.assertIn(dp['ba_name'], self.BA_CHOICES)

            # test for numeric gen
            self.assertGreaterEqual(dp['gen_MW']+1, dp['gen_MW'])

            # test earlier than now
            if c.options.get('forecast', False):
                self.assertGreater(dp['timestamp'], datetime.now(pytz.utc))
            else:
                self.assertLess(dp['timestamp'], datetime.now(pytz.utc))

            # test within date range
            start_at = c.options.get('start_at', False)
            end_at = c.options.get('end_at', False)
            if start_at and end_at:
                self.assertGreaterEqual(dp['timestamp'], start_at)
                self.assertLessEqual(dp['timestamp'], end_at)

        return data

    def _run_notimplemented_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)

        # method not implemented yet
        self.assertRaises(NotImplementedError, c.get_generation)

    def _run_null_response_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)

        # mock request
        with mock.patch.object(c, 'request') as mock_request:
            mock_request.return_value = None

            # get data
            data = c.get_generation(**kwargs)

            # test
            self.assertEqual(data, [])


class TestEIALoad(TestEIA):

    def test_null_response(self):
        self._run_null_response_test(self.load_bas[0], latest=True)

    def test_null_response_latest(self):
        self._run_null_response_test(self.load_bas[0], latest=True)

    def test_null_response_forecast(self):
        today = datetime.today().replace(tzinfo=pytz.utc)
        self._run_null_response_test(self.no_delay_bas[0],
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
            self._run_test(ba, yesterday=True)

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
            self._run_test(ba, start_at='2016-05-01', end_at='2016-05-03')

    def test_date_range_farpast_all(self):
        for ba in self.load_bas:
            if ba in self.problem_bas_load:
                continue
            # basic test
            today = datetime.today().replace(tzinfo=pytz.utc)
            data = self._run_test(ba, start_at=today-timedelta(days=20),
                                  end_at=today-timedelta(days=10))

            # test timestamps are not equal
            timestamps = [d['timestamp'] for d in data]
            self.assertGreater(len(set(timestamps)), 1)

    def test_no_delay_bas_return_last_two_days(self):
        today = datetime.today().replace(tzinfo=pytz.utc)
        two_days_ago = today - timedelta(days=2)
        for ba in self.no_delay_bas:
            if ba in self.problem_bas_load:
                continue
            data = self._run_test(ba, start_at=two_days_ago, end_at=today)
            self.assertGreater(len(data), 1)

    def test_delay_bas_raise_date_value_error(self):
        today = datetime.today().replace(tzinfo=pytz.utc)
        two_days_ago = today - timedelta(days=2)
        for ba in self.delay_bas:
            with self.assertRaises(ValueError):
                self._run_test(ba, start_at=two_days_ago, end_at=today)

    def test_get_load_naive_start_at(self):
        for ba in self.load_bas:
            if ba in self.problem_bas_load:
                continue
            self._run_test(ba, start_at="20161212", end_at="20161222T04Z")

    def test_get_load_naive_end_at(self):
        for ba in self.load_bas:
            if ba in self.problem_bas_load:
                continue
            self._run_test(ba, start_at="20161212T04Z", end_at="20161222")

    def test_get_load_with_unsupported_ba_raises_valueerror(self):
        for ba in self.no_load_bas:
            with self.assertRaises(ValueError):
                self._run_test(ba, market=self.MARKET_CHOICES.hourly)

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
            data = self._run_test(ba, market=self.MARKET_CHOICES.hourly)
            self.assertGreater(len(data), 1)

    def test_non_load_bas_raise_value_error(self):
        for ba in self.no_load_bas:
            with self.assertRaises(ValueError):
                self._run_test(ba, market=self.MARKET_CHOICES.hourly)

    def test_non_us_bas_raise_valueerror(self):
        for ba in self.can_mex:
            with self.assertRaises(ValueError):
                self._run_test(ba, market=self.MARKET_CHOICES.hourly)

    def _test_forecast(self, ba):
        # Used 5 hours/1 day instead of 20/2 for one day forecast
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test(ba, start_at=today + timedelta(hours=5),
                              end_at=today+timedelta(days=1))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreaterEqual(len(set(timestamps)), 1)

        # test timestamps in range
        self.assertGreaterEqual(min(timestamps), today+timedelta(hours=5))
        self.assertLessEqual(min(timestamps), today+timedelta(days=1))

    def _test_latest(self, ba):
        # basic test
        data = self._run_test(ba, latest=True)
        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            # hourly='RTHR'
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def _test_date_range(self, ba):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test(ba, start_at=today - timedelta(days=3),
                              end_at=today - timedelta(days=2),
                              market=self.MARKET_CHOICES.hourly)

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def _run_test(self, ba_name, expect_data=True, tol_min=0, **kwargs):
        # set up
        c = client_factory(ba_name)
        # get data
        data = c.get_load(**kwargs)

        # test number
        if expect_data:
            self.assertGreaterEqual(len(data), 1)
        else:
            self.assertEqual(data, [])

        # test contents
        for dp in data:
            # test key names
            self.assertEqual(set(['load_MW', 'ba_name',
                                  'timestamp', 'freq', 'market']),
                             set(dp.keys()))

            # test values
            self.assertEqual(dp['timestamp'].tzinfo, pytz.utc)
            self.assertIn(dp['ba_name'], self.BA_CHOICES)

            # test for numeric gen
            self.assertGreaterEqual(dp['load_MW']+1, dp['load_MW'])

            # test correct temporal relationship to now
            if c.options['forecast']:
                self.assertGreaterEqual(dp['timestamp'],
                                        pytz.utc.localize(datetime.utcnow())-timedelta(minutes=tol_min))
            else:
                self.assertLess(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))

            # test within date range
            start_at = c.options.get('start_at', False)
            end_at = c.options.get('end_at', False)
            if start_at and end_at:
                self.assertGreaterEqual(dp['timestamp'], start_at)
                self.assertLessEqual(dp['timestamp'], end_at)

        # return
        return data

    # this is repeated- super it?
    def _run_net_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)
        # get data
        data = c.get_trade(**kwargs)

        # test number
        self.assertGreaterEqual(len(data), 1)

        # test contents
        for dp in data:
            # test key names
            for key in ['ba_name', 'timestamp', 'freq', 'market']:
                self.assertIn(key, dp.keys())
            self.assertEqual(len(dp.keys()), 5)

            # test values
            self.assertEqual(dp['timestamp'].tzinfo, pytz.utc)
            self.assertIn(dp['ba_name'], self.BA_CHOICES)

            # test for numeric value
            self.assertGreaterEqual(dp['net_exp_MW']+1, dp['net_exp_MW'])

            # test correct temporal relationship to now
            if c.options['forecast']:
                self.assertGreaterEqual(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))
            else:
                self.assertLess(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))

        return data

    def _run_null_response_test(self, ba_name, **kwargs):
        c = client_factory(ba_name)

        # mock request
        with mock.patch.object(c, 'request') as mock_request:
            mock_request.return_value = None

            # get data
            data = c.get_load(**kwargs)
            self.assertEqual(data, [])


class TestEIATrade(TestEIA):

    def test_null_response(self):
        self._run_null_response_test(self.us_bas[0], latest=True)

    def test_latest_all(self):
        for ba in self.us_bas:
            # basic test
            data = self._run_net_test(ba, latest=True,
                                      market=self.MARKET_CHOICES.hourly)

            # test all timestamps are equal
            timestamps = [d['timestamp'] for d in data]
            self.assertEqual(len(set(timestamps)), 1)

            # test flags
            for dp in data:
                self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
                self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_date_range_no_delay(self):
        for ba in self.no_delay_bas:
            # basic test
            if ba in self.problem_bas_trade:
                continue
            today = datetime.today().replace(tzinfo=pytz.utc)
            data = self._run_net_test(ba, start_at=today-timedelta(days=2),
                                      end_at=today-timedelta(days=1))

            # test timestamps are not equal
            timestamps = [d['timestamp'] for d in data]
            self.assertGreater(len(set(timestamps)), 1)

            # test flags
            for dp in data:
                self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
                self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_date_range_delay(self):
        for ba in self.delay_bas:
            if ba in self.problem_bas_trade:
                continue
            # basic test
            today = datetime.today().replace(tzinfo=pytz.utc)
            data = self._run_net_test(ba, start_at=today-timedelta(days=4),
                                      end_at=today-timedelta(days=3))

            # test timestamps are not equal
            timestamps = [d['timestamp'] for d in data]
            self.assertGreater(len(set(timestamps)), 1)

            # test flags
            for dp in data:
                self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
                self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_forecast_raises_valueerror(self):
        """Ensure get trade with forecast raises an error."""
        for ba in self.no_delay_bas:
            with self.assertRaises(ValueError):
                self._run_net_test(ba, forecast=True)

    def test_date_range_future_raises_valueerror(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        for ba in self.us_bas:
            with self.assertRaises(ValueError):
                self._run_net_test(ba, start_at=today+timedelta(days=1),
                                   end_at=today+timedelta(days=2))

    def test_get_trade_naive_start_at(self):
        for ba in self.us_bas:
            if ba in self.problem_bas_trade:
                continue
            self._run_net_test(ba, start_at="20161212", end_at="20161222T04Z")

    def test_get_trade_naive_end_at(self):
        for ba in self.us_bas:
            if ba in self.problem_bas_trade:
                continue
            self._run_net_test(ba, start_at="20161212T04Z", end_at="20161222")

    def test_get_trade_yesterday(self):
        for ba in self.no_delay_bas:
            if ba in self.problem_bas_trade:
                continue
            self._run_net_test(ba, yesterday=True)

    def test_all_us_bas(self):
        for ba in self.us_bas:
            data = self._run_net_test(ba, market=self.MARKET_CHOICES.hourly)
            self.assertGreater(len(data), 1)

    def test_non_us_bas_raise_valueerror(self):
        for ba in self.can_mex:
            with self.assertRaises(ValueError):
                self._run_net_test(ba, market=self.MARKET_CHOICES.hourly)

    def _run_test(self, ba_name, expect_data=True, tol_min=0, **kwargs):
        # set up
        c = client_factory(ba_name)
        # get data
        data = c.get_trade(**kwargs)

        # test number
        if expect_data:
            self.assertGreaterEqual(len(data), 1)
        else:
            self.assertEqual(data, [])

        # test contents
        for dp in data:
            # test key names
            self.assertEqual(set(['load_MW', 'ba_name',
                                  'timestamp', 'freq', 'market']),
                             set(dp.keys()))

            # test values
            self.assertEqual(dp['timestamp'].tzinfo, pytz.utc)
            self.assertIn(dp['ba_name'], self.BA_CHOICES)

            # test for numeric gen
            self.assertGreaterEqual(dp['load_MW']+1, dp['load_MW'])

            # test correct temporal relationship to now
            if c.options['forecast']:
                self.assertGreaterEqual(dp['timestamp'],
                                        pytz.utc.localize(datetime.utcnow())-timedelta(minutes=tol_min))
            else:
                self.assertLess(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))

            # test within date range
            start_at = c.options.get('start_at', False)
            end_at = c.options.get('end_at', False)
            if start_at and end_at:
                self.assertGreaterEqual(dp['timestamp'], start_at)
                self.assertLessEqual(dp['timestamp'], end_at)

        # return
        return data

    # this is repeated- super it?
    # remove this?
    def _run_net_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)
        # get data
        data = c.get_trade(**kwargs)

        # test number
        self.assertGreaterEqual(len(data), 1)

        # test contents
        for dp in data:
            # test key names
            for key in ['ba_name', 'timestamp', 'freq', 'market']:
                self.assertIn(key, dp.keys())
            self.assertEqual(len(dp.keys()), 5)

            # test values
            self.assertEqual(dp['timestamp'].tzinfo, pytz.utc)
            self.assertIn(dp['ba_name'], self.BA_CHOICES)

            # test for numeric value
            self.assertGreaterEqual(dp['net_exp_MW']+1, dp['net_exp_MW'])

            # test correct temporal relationship to now
            if c.options['forecast']:
                self.assertGreaterEqual(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))
            else:
                self.assertLess(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))

        # return
        return data

    def _run_null_response_test(self, ba_name, **kwargs):
        c = client_factory(ba_name)

        # mock request
        with mock.patch.object(c, 'request') as mock_request:
            mock_request.return_value = None

            # get data

            data = c.get_trade(**kwargs)

            self.assertEqual(data, [])

# python setup.py test -s tests.test_eia.TestEIA.test_get_generation
# python setup.py test -s tests.test_load.TestEIALoad.test_date_range_some
# source venv/bin/activate

if __name__ == '__main__':
    unittest.main()
