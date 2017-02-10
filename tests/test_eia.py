import unittest
from unittest import TestCase
from pyiso import client_factory
from datetime import datetime, timedelta
from dateutil.parser import parse as dateutil_parse
from pyiso import BALANCING_AUTHORITIES
import random
import pytz
import time

"""Test EIA client.
To use, set the your EIA key as an environment variable:
    export EIA_KEY=my-eia-api-key
"""

# Start here- split this into classes
# ok, we may have a decent bit of work here- make sure these test cases
# fit into the test framework- i'm not sure they do now.
# Also, add mocking:
# http://docs.python-guide.org/en/latest/writing/tests/

# ok, so i need to spread these tests across the test_load, test_trade, test_genmix, etc modules. There won't be as much in here.


class TestEIA(TestCase):
    def setUp(self):
        eia_bas = [i for i in BALANCING_AUTHORITIES.keys() if BALANCING_AUTHORITIES[i]["class"] == "EIACLIENT"]
        self.ba = random.choice(eia_bas)
        self.c = client_factory(self.ba)

    def tearDown(self):
        self.c = None
        time.sleep(4) # Delay to cut down on throttling

    def test_get_trade(self):
        """Show that we get data back for get trade against EIA balancing
        authorities. """
        # Need to improve this test
        self.result = self.c.get_trade(bal_auth=self.ba,
                                       start_at="20161212",
                                       end_at="20161222T04Z")

        self.assertTrue(len(self.result) > 0)

    def test_get_trade_yesterday(self):
        """Check date for yesterday trade data"""
        self.result = self.c.get_trade(bal_auth=self.ba,
                                       yesterday=True)
        dates = [dateutil_parse(i["timestamp"]) for i in self.result]
        local_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone(self.c.TZ_NAME))
        local_day = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = (local_day - timedelta(days=1)).day
        for date in dates:
            self.assertEqual(date.day, yesterday)

    def test_get_trade_latest(self):
        self.result = self.c.get_trade(bal_auth=self.ba,
                                       latest=True)
        try:
            self.assertLess(len(self.result), 2)
        except AssertionError as e:
            print ("failed!", self.options, e)
            raise e

    def test_get_trade_naive_start_at(self):
        self.result = self.c.get_trade(bal_auth=self.ba,
                                       start_at="20161212",
                                       end_at="20161222T04Z")
        self.assertTrue(self.result[0]["timestamp"][-1] == "Z")

    def test_get_trade_naive_end_at(self):
        self.result = self.c.get_trade(bal_auth=self.ba,
                                       start_at="20161212T04Z",
                                       end_at="20161222")
        self.assertTrue(self.result[0]["timestamp"][-1] == "Z")

    def test_get_trade_two_day_bas(self):
        delay_bas = ['AEC', 'DOPD', 'GVL', 'HST', 'NSB', 'PGE', 'SCL',
                     'TAL', 'TIDC', 'TPWR']
        self.ba = random.choice(delay_bas)
        local_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone(self.c.TZ_NAME))
        local_day = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        one_days_ago = (local_day - timedelta(days=1)).strftime("%Y%m%d")
        three_days_ago = (local_day - timedelta(days=3)).strftime("%Y%m%d")
        with self.assertRaises(ValueError):
            self.c.get_trade(bal_auth=self.ba, start_at=three_days_ago,
                             end_at=one_days_ago)

    def test_get_trade_with_forecast_raises_valueerror(self):
        """Ensure get trade with forecast raises an error."""

        with self.assertRaises(ValueError):
            self.result = self.c.get_trade(bal_auth=self.ba, forecast=True)


    def test_get_load(self):
        """Test load - only on BAs that support it."""
        eia_bas = [i for i in BALANCING_AUTHORITIES.keys() if BALANCING_AUTHORITIES[i]["class"] == "EIACLIENT"]
        no_load = ['DEAA-EIA', 'EEI', 'GRIF-EIA', 'GRMA', 'GWA',
                   'HGMA-EIA', 'SEPA', 'WWA', 'YAD']
        bas_with_load = [i for i in eia_bas if i not in no_load]
        self.ba = random.choice(bas_with_load)
        self.result = self.c.get_load(bal_auth=self.ba,
                                      start_at="20161212",
                                      end_at="20161222T04Z")
        self.assertTrue(len(self.result) > 0)

    def test_get_load_yesterday(self):
        """Check date for yesterday load data"""
        eia_bas = [i for i in BALANCING_AUTHORITIES.keys() if BALANCING_AUTHORITIES[i]["class"] == "EIACLIENT"]
        no_load = ['DEAA-EIA', 'EEI', 'GRIF-EIA', 'GRMA', 'GWA',
                                  'HGMA-EIA', 'SEPA', 'WWA', 'YAD']
        delay_bas = ['AEC', 'DOPD', 'GVL', 'HST', 'NSB', 'PGE', 'SCL',
                     'TAL', 'TIDC', 'TPWR']
        bas_with_load = [i for i in eia_bas if i not in no_load and i not in delay_bas]
        self.ba = random.choice(bas_with_load)
        self.result = self.c.get_load(bal_auth=self.ba,
                                      yesterday=True)
        dates = [dateutil_parse(i["timestamp"]) for i in self.result]
        local_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone(self.c.TZ_NAME))
        local_day = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = (local_day - timedelta(days=1)).day
        for date in dates:
            self.assertEqual(date.day, yesterday)

    def test_get_load_latest(self):
        eia_bas = [i for i in BALANCING_AUTHORITIES.keys() if BALANCING_AUTHORITIES[i]["class"] == "EIACLIENT"]
        no_load = ['DEAA-EIA', 'EEI', 'GRIF-EIA', 'GRMA', 'GWA',
                                  'HGMA-EIA', 'SEPA', 'WWA', 'YAD']
        bas_with_load = [i for i in eia_bas if i not in no_load]
        self.ba = random.choice(bas_with_load)
        self.result = self.c.get_load(bal_auth=self.ba,
                                      latest=True)
        try:
            self.assertLess(len(self.result), 2)
        except AssertionError as e:
            print ("failed!", self.options, e)
            raise e

    def test_get_load_forecast(self):
        """Test load forecast - only on BAs that support it."""
        eia_bas = [i for i in BALANCING_AUTHORITIES.keys() if BALANCING_AUTHORITIES[i]["class"] == "EIACLIENT"]
        no_load = ['DEAA-EIA', 'EEI', 'GRIF-EIA', 'GRMA', 'GWA',
                                  'HGMA-EIA', 'SEPA', 'WWA', 'YAD']
        delay_bas = ['AEC', 'DOPD', 'GVL', 'HST', 'NSB', 'PGE', 'SCL',
                     'TAL', 'TIDC', 'TPWR']
        bas_with_load = [i for i in eia_bas if i not in no_load and i not in delay_bas]
        self.ba = random.choice(bas_with_load)
        self.result = self.c.get_load(bal_auth=self.ba, forecast=True)
        result_day = dateutil_parse(self.result[0]["timestamp"]).day
        today = datetime.now().day
        self.assertTrue(result_day >= today)

    def test_get_load_naive_start_at(self):
        eia_bas = [i for i in BALANCING_AUTHORITIES.keys() if BALANCING_AUTHORITIES[i]["class"] == "EIACLIENT"]
        no_load = ['DEAA-EIA', 'EEI', 'GRIF-EIA', 'GRMA', 'GWA',
                                  'HGMA-EIA', 'SEPA', 'WWA', 'YAD']
        bas_with_load = [i for i in eia_bas if i not in no_load]
        self.ba = random.choice(bas_with_load)
        self.result = self.c.get_load(bal_auth=self.ba,
                                      start_at="20161212",
                                      end_at="20161222T04Z")
        self.assertTrue(self.result[0]["timestamp"][-1] == "Z")

    def test_get_load_naive_end_at(self):
        eia_bas = [i for i in BALANCING_AUTHORITIES.keys() if BALANCING_AUTHORITIES[i]["class"] == "EIACLIENT"]
        no_load = ['DEAA-EIA', 'EEI', 'GRIF-EIA', 'GRMA', 'GWA',
                                  'HGMA-EIA', 'SEPA', 'WWA', 'YAD']
        bas_with_load = [i for i in eia_bas if i not in no_load]
        self.ba = random.choice(bas_with_load)
        self.result = self.c.get_load(bal_auth=self.ba,
                                      start_at="20161212T04Z",
                                      end_at="20161222")
        self.assertTrue(self.result[0]["timestamp"][-1] == "Z")

    def test_get_load_two_day_bas(self):
        delay_bas = ['AEC', 'DOPD', 'GVL', 'HST', 'NSB', 'PGE', 'SCL',
                     'TAL', 'TIDC', 'TPWR']
        self.ba = random.choice(delay_bas)
        local_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone(self.c.TZ_NAME))
        local_day = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        one_days_ago = (local_day - timedelta(days=1)).strftime("%Y%m%d")
        three_days_ago = (local_day - timedelta(days=3)).strftime("%Y%m%d")
        with self.assertRaises(ValueError):
            self.c.get_load(bal_auth=self.ba, start_at=three_days_ago,
                            end_at=one_days_ago)

    def test_get_load_with_unsupported_ba_raises_valueerror(self):
        """
        Confirm that requesting load data for a BA that doesn't support load
        raises a value error.
        """
        load_not_supported_bas = ['DEAA', 'EEI', 'GRIF', 'GRMA', 'GWA',
                                  'HGMA', 'SEPA', 'WWA', 'YAD']
        for ba in load_not_supported_bas:
            with self.assertRaises(ValueError):
                self.result = self.c.get_load(bal_auth=ba, latest=True)

    def test_get_generation(self):
        self.result = self.c.get_generation(bal_auth=self.ba,
                                            start_at="20161212",
                                            end_at="20161222T04Z")
        self.assertTrue(len(self.result) > 0)

    def test_get_generation_yesterday(self):
        """Check date for yesterday trade data"""

        eia_bas = [i for i in BALANCING_AUTHORITIES.keys() if BALANCING_AUTHORITIES[i]["class"] == "EIACLIENT"]
        delay_bas = ['AEC', 'DOPD', 'GVL', 'HST', 'NSB', 'PGE', 'SCL',
                     'TAL', 'TIDC', 'TPWR']
        bas_without_delay = [i for i in eia_bas if i not in delay_bas]
        self.ba = random.choice(bas_without_delay)
        self.result = self.c.get_generation(bal_auth=self.ba,
                                            yesterday=True)
        dates = [dateutil_parse(i["timestamp"]) for i in self.result]
        local_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone(self.c.TZ_NAME))
        local_day = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = (local_day - timedelta(days=1)).day
        for date in dates:
            self.assertEqual(date.day, yesterday)

    def test_get_generation_two_day_bas(self):
        delay_bas = ['AEC', 'DOPD', 'GVL', 'HST', 'NSB', 'PGE', 'SCL',
                     'TAL', 'TIDC', 'TPWR']
        self.ba = random.choice(delay_bas)
        local_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone(self.c.TZ_NAME))
        local_day = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        one_days_ago = (local_day - timedelta(days=1)).strftime("%Y%m%d")
        three_days_ago = (local_day - timedelta(days=3)).strftime("%Y%m%d")
        with self.assertRaises(ValueError):
            self.c.get_generation(bal_auth=self.ba, start_at=three_days_ago,
                                  end_at=one_days_ago)

    def test_get_generation_naive_start_at(self):
        self.result = self.c.get_generation(bal_auth=self.ba,
                                            start_at="20161212",
                                            end_at="20161222T04Z")
        self.assertTrue(self.result[0]["timestamp"][-1] == "Z")

    def test_get_generation_naive_end_at(self):
        self.result = self.c.get_generation(bal_auth=self.ba,
                                            start_at="20161212T04Z",
                                            end_at="20161222")
        self.assertTrue(self.result[0]["timestamp"][-1] == "Z")

    def test_get_generation_with_forecast_raises_valueerror(self):
        """Ensure get generation with forecast raises an error."""

        with self.assertRaises(ValueError):
            self.result = self.c.get_generation(bal_auth=self.ba, forecast=True)

    def test_get_generation_latest(self):
        eia_bas = [i for i in BALANCING_AUTHORITIES.keys() if BALANCING_AUTHORITIES[i]["class"] == "EIACLIENT"]
        no_load = ['DEAA-EIA', 'EEI', 'GRIF-EIA', 'GRMA', 'GWA',
                                  'HGMA-EIA', 'SEPA', 'WWA', 'YAD']
        bas_with_load = [i for i in eia_bas if i not in no_load]
        self.ba = random.choice(bas_with_load)
        self.result = self.c.get_generation(bal_auth=self.ba,
                                            latest=True)
        try:
            self.assertLess(len(self.result), 2)
        except AssertionError as e:
            print ("failed!", self.options, e)
            raise e


# python setup.py test -s tests.test_eia.TestEIA.test_get_generation
# python setup.py test -s tests.test_load.TestEIALoad.test_date_range_some
# source venv/bin/activate

if __name__ == '__main__':
    unittest.main()
