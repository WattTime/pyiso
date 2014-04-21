from pyiso import client_factory
from unittest import TestCase
import pytz
from datetime import datetime
import logging
import StringIO


class TestBPABase(TestCase):
    def setUp(self):
        self.wind_tsv = StringIO.StringIO("BPA Balancing Authority Load & Total Wind Generation\n\
at 5-minute intervals, last 7 days\n\
Dates: 09Apr2014 - 16Apr2014 (last updated 15Apr2014 11:07:06) Pacific Time\n\
Based on 5-min MW readings from the BPA SCADA system for points 45583, 79687, 79682, 79685\n\
BPA/Technical Operations (TOT-OpInfo@bpa.gov)\n\
\n\
Date/Time       \tLoad\tWind\tHydro\tThermal\n\
04/15/2014 10:10\t6553\t3732\t11225\t1599\n\
04/15/2014 10:15\t6580\t3686\t11230\t1603\n\
04/15/2014 10:20\t6560\t3700\t11254\t1602\n\
04/15/2014 10:25\t6537\t3684\t11281\t1601\n\
04/15/2014 10:30\t6562\t3680\t11260\t1607\n\
04/15/2014 10:35\t6525\t3675\t11212\t1608\n\
04/15/2014 10:40\t6496\t3706\t11240\t1605\n\
04/15/2014 10:45\t6514\t3700\t11261\t1607\n\
04/15/2014 10:50\t6501\t3727\t11172\t1607\n\
04/15/2014 10:55\t6451\t3700\t11066\t1596\n\
04/15/2014 11:00\t6449\t3696\t10816\t1601\n\
04/15/2014 11:05\t6464\t3688\t10662\t1601\n\
04/15/2014 11:10\t\t\t\t\n\
04/15/2014 11:15\t\t\t\t\n\
04/15/2014 11:20\t\t\t\t\n\
04/15/2014 11:25\t\t\t\t\n\
04/15/2014 11:30\t\n\
")

    def create_client(self, ba_name):
        # set up client with logging
        c = client_factory(ba_name)
        handler = logging.StreamHandler()
        c.logger.addHandler(handler)
        c.logger.setLevel(logging.DEBUG)
        return c

    def test_request_latest(self):
        c = self.create_client('BPA')
        response = c.request('http://transmission.bpa.gov/business/operations/wind/baltwg.txt')
        self.assertIn('BPA Balancing Authority Load & Total Wind Generation', response.text)

    def test_parse_to_df(self):
        c = self.create_client('BPA')
        df = c.parse_to_df(self.wind_tsv, skiprows=6, header=0, delimiter='\t',
                           index_col=0, parse_dates=True)

        # Date/Time set as index
        self.assertEqual(list(df.columns), ['Load', 'Wind', 'Hydro', 'Thermal'])

        # rows with NaN dropped
        self.assertEqual(len(df), 12)

    def test_utcify(self):
        ts_str = '04/15/2014 10:10'
        c = self.create_client('BPA')
        ts = c.utcify(ts_str)
        self.assertEqual(ts, datetime(2014, 4, 15, 10+7, 10, tzinfo=pytz.utc))

    def test_utcify_index(self):
        c = self.create_client('BPA')
        df = c.parse_to_df(self.wind_tsv, skiprows=6, header=0, delimiter='\t',
                            index_col=0, parse_dates=True)
        utc_index = c.utcify_index(df.index)
        self.assertEqual(utc_index[0].to_pydatetime(), datetime(2014, 4, 15, 10+7, 10, tzinfo=pytz.utc))
        self.assertEqual(len(df), len(utc_index))

    def test_slice_latest(self):
        c = self.create_client('BPA')
        df = c.parse_to_df(self.wind_tsv, skiprows=6, header=0, delimiter='\t',
                            index_col=0, parse_dates=True)
        df.index = c.utcify_index(df.index)
        sliced = c.slice_times(df, latest=True)

        # values in last non-empty row
        self.assertEqual(list(sliced.values[0]), [6464, 3688, 10662, 1601])

    def test_slice_startend(self):
        c = self.create_client('BPA')
        df = c.parse_to_df(self.wind_tsv, skiprows=6, header=0, delimiter='\t',
                            index_col=0, parse_dates=True)
        df.index = c.utcify_index(df.index)
        sliced = c.slice_times(df, start_at=datetime(2014, 4, 15, 10+7, 25, tzinfo=pytz.utc),
                         end_at=datetime(2014, 4, 15, 11+7, 30, tzinfo=pytz.utc))

        self.assertEqual(len(sliced), 9)
        self.assertEqual(list(sliced.iloc[0].values), [6537, 3684, 11281, 1601])

    def test_serialize(self):
        c = self.create_client('BPA')
        df = c.parse_to_df(self.wind_tsv, skiprows=6, header=0, delimiter='\t',
                            index_col=0, parse_dates=True, usecols=[0, 2, 3, 4])
        df.index = c.utcify_index(df.index)
        renamed = df.rename(columns=c.fuels, inplace=False)
        pivoted = c.unpivot(renamed)

        data = c.serialize(pivoted, header=['timestamp', 'fuel_name', 'gen_MW'])
        self.assertEqual(len(data), len(pivoted))
        self.assertEqual(data[0], {'timestamp': datetime(2014, 4, 15, 17, 10, tzinfo=pytz.utc), 'gen_MW': 3732.0, 'fuel_name': 'wind'})
