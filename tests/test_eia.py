import unittest
from unittest import TestCase
from pyiso import client_factory
import json
import datetime
from dateutil.parser import parse as dateutil_parse
from pyiso import BALANCING_AUTHORITIES
import random
import mock

"""Test EIA client.
To use, set the your EIA key as an environment variable:
    os.environ["EIA_KEY"] = my-eia-api-key
"""


class TestEIA(TestCase):
    def setUp(self):
        eia_bas = [i for i in BALANCING_AUTHORITIES.keys() if BALANCING_AUTHORITIES[i]["class"] == "EIACLIENT"]
        self.ba = random.choice(eia_bas)
        self.c = client_factory(self.ba)

    def tearDown(self):
        self.c = None

    def test_get_trade_all(self):
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
        # print "yesterday: ", self.result
        days = [dateutil_parse(i["timestamp"]) for i in self.result]
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).day
        for _ in days:
            self.assertEqual(_.day, yesterday)

    def test_get_trade_latest(self):
        self.result = self.c.get_trade(bal_auth=self.ba,
                                       latest=True)
        self.assertLess(len(self.result), 2)

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

    def test_get_load(self):
        # Need to improve this test
        self.result = self.c.get_load(bal_auth=self.ba,
                                      start_at="20161212",
                                      end_at="20161222T04Z")
        self.assertTrue(len(self.result) > 0)

    def test_get_load_forecast(self):

        # this one is failing intermittently with a series key error

        self.result = self.c.get_load(bal_auth=self.ba, forecast=True)
        print(self.result[0])
        result_day = dateutil_parse(self.result[0]["timestamp"]).day
        today = datetime.datetime.now().day
        self.assertTrue(result_day >= today)
        # self.assertTrue(len(self.result) > 0)

    def test_get_generation(self):
        self.result = self.c.get_generation(bal_auth=self.ba,
                                            start_at="20161212",
                                            end_at="20161222T04Z")

        # Need to improve this test
        self.assertTrue(len(self.result) > 0)


if __name__ == '__main__':
    unittest.main()
