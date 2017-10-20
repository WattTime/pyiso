from datetime import datetime, timedelta
from unittest import TestCase

import pandas as pd
import pytz

from pyiso import client_factory


class IntegrationTestPJMClient(TestCase):
    def setUp(self):
        self.c = client_factory('PJM')

    def test_fetch_edata_series_timezone(self):
        data = self.c.fetch_edata_series('ForecastedLoadHistory', {'name': 'PJM RTO Total'})

        # check that latest forecast is within 1 hour, 1 minute of now
        td = data.index[0] - pytz.utc.localize(datetime.utcnow())
        self.assertLessEqual(td, timedelta(hours=1, minutes=1))

    def test_edata_point_rounds_to_5min(self):
        ts, val = self.c.fetch_edata_point('InstantaneousLoad', 'PJM RTO Total', 'MW')
        self.assertEqual(ts.minute % 5, 0)

    def test_fetch_historical_load(self):
        df = self.c.fetch_historical_load(2015)
        self.assertEqual(df['load_MW'][0], 94001.713000000003)
        self.assertEqual(df['load_MW'][-1], 79160.809999999998)
        self.assertEqual(len(df), 365*24 - 2)

        est = pytz.timezone(self.c.TZ_NAME)
        start_at = est.localize(datetime(2015, 1, 1))
        end_at = est.localize(datetime(2015, 12, 31, 23, 0))

        self.assertEqual(df.index[0], start_at)
        self.assertEqual(df.index[-1], end_at)

        # Manually checked form xls file
        # These tests should fail if timezones are improperly handled
        tz_func = lambda x: pd.Timestamp(pytz.utc.normalize(est.localize(x)))
        self.assertEqual(df.ix[tz_func(datetime(2015, 2, 2, 14))]['load_MW'], 105714.638)
        self.assertEqual(df.ix[tz_func(datetime(2015, 6, 4, 2))]['load_MW'], 64705.985)
        self.assertEqual(df.ix[tz_func(datetime(2015, 12, 15, 23))]['load_MW'], 79345.672)

    def test_parse_date_from_markets_operations(self):
        soup = self.c.fetch_markets_operations_soup()
        ts = self.c.parse_date_from_markets_operations(soup)
        td = self.c.local_now() - ts

        # Test that the timestamp is within the last 24 hours
        self.assertLess(td.total_seconds(), 60*60*24)

    def test_parse_realtime_genmix(self):
        soup = self.c.fetch_markets_operations_soup()
        data = self.c.parse_realtime_genmix(soup)

        # expect 10 fuels
        self.assertEqual(len(data), 10)
