import os

from pyiso import client_factory
from unittest import TestCase
import pandas as pd
from datetime import datetime, timedelta
import pytz

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), '../fixtures')


class TestPJM(TestCase):
    def setUp(self):
        self.edata_inst_load = open(FIXTURES_DIR + '/pjm/InstantaneousLoad.html').read().encode('utf8')
        self.edata_forecast_load = open(FIXTURES_DIR + '/pjm/ForecastedLoadHistory.html').read().encode('utf8')
        self.c = client_factory('PJM')

    def test_utcify_pjmlike_edt(self):
        ts_str = '04/13/14 21:45 EDT'
        ts = self.c.utcify(ts_str)
        self.assertEqual(ts.year, 2014)
        self.assertEqual(ts.month, 4)
        self.assertEqual(ts.day, 13+1)
        self.assertEqual(ts.hour, 21-20)
        self.assertEqual(ts.minute, 45)
        self.assertEqual(ts.tzinfo, pytz.utc)

    def test_utcify_pjmlike_est(self):
        ts_str = '11/13/14 21:45 EST'
        ts = self.c.utcify(ts_str)
        self.assertEqual(ts.year, 2014)
        self.assertEqual(ts.month, 11)
        self.assertEqual(ts.day, 13+1)
        self.assertEqual(ts.hour, 21-19)
        self.assertEqual(ts.minute, 45)
        self.assertEqual(ts.tzinfo, pytz.utc)

    def test_time_as_of(self):
        ts = self.c.time_as_of(self.edata_inst_load)
        self.assertEqual(ts, datetime(2015, 12, 11, 17, 23, tzinfo=pytz.utc) + timedelta(hours=5))

    def test_parse_inst_load(self):
        dfs = pd.read_html(self.edata_inst_load, header=0, index_col=0)
        df = dfs[0]
        self.assertEqual(df.columns, 'MW')
        self.assertEqual(df.loc['PJM RTO Total']['MW'], 91419)

    def test_missing_time_is_none(self):
        ts = self.c.time_as_of('')
        self.assertIsNone(ts)

    def test_bad_url(self):
        ts, val = self.c.fetch_edata_point('badtype', 'badkey', 'badheader')
        self.assertIsNone(ts)
        self.assertIsNone(val)
