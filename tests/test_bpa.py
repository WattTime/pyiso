from pyiso import client_factory
from unittest import TestCase
import pytz
from datetime import datetime
from io import StringIO


class TestBPABase(TestCase):
    def setUp(self):
        self.wind_tsv = StringIO(u"BPA Balancing Authority Load & Total Wind Generation\n\
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

        self.wind_xls = StringIO(u"TOTAL LOAD & WIND GENERATION IN THE BPA CONTROL AREA (Balancing Authority Area)                      \n\
Beginning 1/1/2014, at 5-min increments, updated daily                      \n\
This data pertains to the Total Load & Total Wind, Hydro, and Thermal Generation in the BPA Balancing Authority area (control area).                        \n\
It is based upon 5-minute interpolated snapshots.                       \n\
                        \n\
The Balancing Authority load reflects the load in our electrical control area, within which our AGC systems balance generation, interchange, and load.                      \n\
This is the most common building block unit for load analysis within NWPP, WECC, and NERC.                      \n\
For wind plants with output split between multiple control areas (e.g., Nine Canyon) only the portion of the output\n\ in the BPA BA is included.                      \n\
This file includes the Total Wind Generation \"Basepoint\", which is the sum of all operator-supplied wind gen forecasts (schedules).                     \n\
                        \n\
Source: 5-min data via SCADA/PI point AGC_ONLN.CAREA..MW.103349 (Total Wind Basepoint)                      \n\
Source: 5-min data via SCADA/PI point AGCDISTB.AGCRPT..MW.79687 (Total Wind Generation)                     \n\
Source: 5-min data via SCADA/PI point BPA.CAREA..MW.45583 (Net Load)                        \n\
Source: 5-min data via SCADA/PI point AGCDISTB.AGCRPT..MW.79682 (Total Hydro Generation)                       \n\
Source: 5-min data via SCADA/PI point AGCDISTB.AGCRPT..MW.79685 (Total Thermal Generation)                      \n\
Source: 5-min data via SCADA/PI point AGCDISTB.AGCRPT..MW.45581 (Net Interchange)                       \n\
BPA Transmission Technical Operations/TOT/21Apr14                       \n\
                        \n\
Date/Time   TOTAL WIND GENERATION  BASEPOINT (FORECAST) IN BPA CONTROL AREA (MW; SCADA 103349)\tTOTAL WIND GENERATION  IN BPA CONTROL AREA (MW; SCADA 79687)\tTOTAL BPA CONTROL AREA LOAD (MW; SCADA 45583)\tTOTAL HYDRO GENERATION (MW; SCADA 79682)\tTOTAL THERMAL GENERATION (MW; SCADA 79685)\tNET INTERCHANGE (MW; SCADA 45581)\n\
01/01/14 00:00\t227\t127\t5965\t5500\t3637\t3299\n\
01/01/14 00:05\t190\t121\t5977\t5336\t3639\t3119\n\
01/01/14 00:10\t162\t113\t5936\t5157\t3644\t2977\n\
01/01/14 00:15\t162\t113\t5907\t5168\t3639\t3011\n\
01/01/14 00:20\t162\t104\t5908\t5092\t3640\t2929\n\
01/01/14 00:25\t162\t98\t5918\t5078\t3640\t2898\n\
01/01/14 00:30\t162\t85\t5879\t5124\t3642\t2973\n\
01/01/14 00:35\t163\t78\t5858\t5125\t3642\t2988\n\
01/01/14 00:40\t163\t66\t5900\t5118\t3653\t2937\n\
01/01/14 00:45\t163\t59\t5886\t5112\t3657\t2943\n\
01/01/14 00:50\t163\t54\t5878\t5110\t3648\t2935\n\
01/01/14 00:55\t141\t45\t5880\t5041\t3652\t2811\n\
01/01/14 01:00\t123\t39\t5865\t5015\t3652\t2745\n\
01/01/14 01:05\t105\t40\t5870\t5018\t3644\t2685\n\
01/01/14 01:10\t93\t44\t5868\t5044\t3656\t2678\n\
01/01/14 01:15\t93\t44\t5853\t5085\t3668\t2745\n\
01/01/14 01:20\t93\t35\t5854\t5077\t3651\t2710\n\
01/01/14 01:25\t93\t27\t5852\t5042\t3658\t2676\n\
")

    def test_request_latest(self):
        c = client_factory('BPA')
        response = c.request('http://transmission.bpa.gov/business/operations/wind/baltwg.txt')
        self.assertIn('BPA Balancing Authority Load & Total Wind Generation', response.text)

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
        self.assertEqual(data[0], {'timestamp': datetime(2014, 4, 15, 17, 10, tzinfo=pytz.utc), 'gen_MW': 3732.0, 'fuel_name': 'wind'})

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
        xd = c.fetch_xls(c.base_url + 'wind/WindGenTotalLoadYTD_2014.xls')

        # parse xls
        df = c.parse_to_df(xd, mode='xls', sheet_names=xd.sheet_names, skiprows=18,
                           index_col=0, parse_dates=True,
                           parse_cols=[0, 2, 4, 5], header_names=['Wind', 'Hydro', 'Thermal']
                           )
        self.assertEqual(list(df.columns), ['Wind', 'Hydro', 'Thermal'])
        self.assertGreater(len(df), 0)
        self.assertEqual(df.iloc[0].name, datetime(2014, 1, 1))
