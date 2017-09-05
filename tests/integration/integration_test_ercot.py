from pyiso import client_factory
from unittest import TestCase
import pytz
from datetime import datetime, timedelta
import pandas as pd


class IntegrationTestERCOT(TestCase):
    def setUp(self):
        self.c = client_factory('ERCOT')
        self.node_counts = range(612, 633)  # 632 LMP nodes as of 2017-09-04. TODO Why is this a range?

    def test_request_report_gen_hrly(self):
        # get data as list of dicts
        df = self.c._request_report('gen_hrly')

        # test for expected data
        self.assertEqual(len(df), 1)
        for key in ['SE_EXE_TIME_DST', 'SE_EXE_TIME', 'SE_MW']:
            self.assertIn(key, df.columns)

    def test_request_report_wind_hrly(self):
        # get data as list of dicts
        df = self.c._request_report('wind_hrly')

        # test for expected data
        self.assertLessEqual(len(df), 96)
        for key in ['DSTFlag', 'ACTUAL_SYSTEM_WIDE', 'HOUR_ENDING']:
            self.assertIn(key, df.columns)

    def test_request_report_load_7day(self):
        # get data as list of dicts
        df = self.c._request_report('load_7day')

        # test for expected data
        # subtract 1 hour for DST
        self.assertGreaterEqual(len(df), 8 * 24 - 1)
        for key in ['SystemTotal', 'HourEnding', 'DSTFlag', 'DeliveryDate']:
            self.assertIn(key, df.columns)

    def test_request_report_dam_hrl_lmp(self):
        now = datetime.now(pytz.timezone(self.c.TZ_NAME))
        df = self.c._request_report('dam_hrly_lmp', now)
        s = pd.to_datetime(df['DeliveryDate'], utc=True)

        self.assertEqual(s.min().date(), now.date())
        self.assertEqual(s.max().date(), now.date())
        self.assertIn(len(df), [n * 24 for n in self.node_counts])  # nodes * 24 hrs/day

    def test_request_report_rt5m_lmp(self):
        now = datetime.now(pytz.utc) - timedelta(minutes=5)
        df = self.c._request_report('rt5m_lmp', now)
        df.index = df['SCEDTimestamp']
        s = pd.to_datetime(df.index).tz_localize('Etc/GMT+5').tz_convert('utc')

        self.assertLess((s.min() - now).total_seconds(), 8 * 60)
        self.assertLess((s.max() - now).total_seconds(), 8 * 60)
        self.assertIn(len(df), self.node_counts)
