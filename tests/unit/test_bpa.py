import os
from datetime import datetime
from unittest import TestCase

import pandas as pd
import pytz

from pyiso import client_factory

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), '../fixtures')


class TestBPABase(TestCase):
    def setUp(self):
        self.wind_tsv = open(FIXTURES_DIR + '/bpa/wind_tsv.csv').read().encode('utf8')

    def test_parse_to_df(self):
        c = client_factory('BPA')
        df = c.parse_to_df(self.wind_tsv, skiprows=6, header=0, delimiter='\t',
                           index_col=0, parse_dates=True)

        # Date/Time set as index
        self.assertEqual(list(df.columns), ['Load', 'Wind', 'Hydro', 'Thermal'])

        # rows with NaN dropped
        self.assertEqual(len(df), 12)

    def test_utcify(self):
        ts_str = '04/15/2014 10:10'
        c = client_factory('BPA')
        ts = c.utcify(ts_str)
        self.assertEqual(ts, datetime(2014, 4, 15, 10+7, 10, tzinfo=pytz.utc))

    def test_utcify_index(self):
        c = client_factory('BPA')
        df = c.parse_to_df(self.wind_tsv, skiprows=6, header=0, delimiter='\t',
                           index_col=0, parse_dates=True)
        utc_index = c.utcify_index(df.index)
        self.assertEqual(utc_index[0].to_pydatetime(), datetime(2014, 4, 15, 10+7, 10, tzinfo=pytz.utc))
        self.assertEqual(len(df), len(utc_index))

    def test_slice_latest(self):
        c = client_factory('BPA')
        df = c.parse_to_df(self.wind_tsv, skiprows=6, header=0, delimiter='\t',
                           index_col=0, parse_dates=True)
        df.index = c.utcify_index(df.index)
        sliced = c.slice_times(df, {'latest': True})

        # values in last non-empty row
        self.assertEqual(list(sliced.values[0]), [6464, 3688, 10662, 1601])

    def test_slice_startend(self):
        c = client_factory('BPA')
        df = c.parse_to_df(self.wind_tsv, skiprows=6, header=0, delimiter='\t',
                           index_col=0, parse_dates=True)
        df.index = c.utcify_index(df.index)
        sliced = c.slice_times(df, {'start_at': datetime(2014, 4, 15, 10+7, 25, tzinfo=pytz.utc),
                                    'end_at': datetime(2014, 4, 15, 11+7, 30, tzinfo=pytz.utc)})

        self.assertEqual(len(sliced), 9)
        self.assertEqual(list(sliced.iloc[0].values), [6537, 3684, 11281, 1601])

    def test_serialize_gen(self):
        c = client_factory('BPA')
        df = c.parse_to_df(self.wind_tsv, skiprows=6, header=0, delimiter='\t',
                           index_col=0, parse_dates=True, usecols=[0, 2, 3, 4])
        df.index = c.utcify_index(df.index)
        renamed = df.rename(columns=c.fuels, inplace=False)
        pivoted = c.unpivot(renamed)

        data = c.serialize(pivoted, header=['timestamp', 'fuel_name', 'gen_MW'])
        self.assertEqual(len(data), len(pivoted))
        self.assertEqual(data[0], {'timestamp': datetime(2014, 4, 15, 17, 10, tzinfo=pytz.utc),
                                   'gen_MW': 3732.0,
                                   'fuel_name': 'wind'})

    def test_serialize_load(self):
        c = client_factory('BPA')
        df = c.parse_to_df(self.wind_tsv, skiprows=6, header=0, delimiter='\t',
                           index_col=0, parse_dates=True, usecols=[0, 1])
        df.index = c.utcify_index(df.index)

        data = c.serialize(df, header=['timestamp', 'load_MW'])
        self.assertEqual(len(data), len(df))
        self.assertEqual(data[0], {'timestamp': datetime(2014, 4, 15, 17, 10, tzinfo=pytz.utc), 'load_MW': 6553.0})

    def test_parse_xls(self):
        c = client_factory('BPA')
        xd = pd.ExcelFile(FIXTURES_DIR + '/bpa/WindGenTotalLoadYTD_2014_short.xls')

        # parse xls
        df = c.parse_to_df(xd, mode='xls', sheet_names=xd.sheet_names, skiprows=18,
                           index_col=0, parse_dates=True,
                           parse_cols=[0, 2, 4, 5], header_names=['Wind', 'Hydro', 'Thermal']
                           )
        self.assertEqual(list(df.columns), ['Wind', 'Hydro', 'Thermal'])
        self.assertGreater(len(df), 0)
        self.assertEqual(df.iloc[0].name, datetime(2014, 1, 1))
