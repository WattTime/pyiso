from pyiso import client_factory, LOG_LEVEL
from unittest import TestCase
import logging
from io import StringIO
from datetime import date, datetime
import pytz


class TestNYISOBase(TestCase):
    def setUp(self):
        self.load_csv = StringIO(u'"Time Stamp","Time Zone","Name","PTID","Load"\n\
"09/10/2014 00:00:00","EDT","CAPITL",61757,1173.2\n\
"09/10/2014 00:00:00","EDT","CENTRL",61754,1591.2\n\
"09/10/2014 00:00:00","EDT","DUNWOD",61760,609.4\n\
"09/10/2014 00:00:00","EDT","GENESE",61753,1003.3\n\
"09/10/2014 00:00:00","EDT","HUD VL",61758,965.4\n\
"09/10/2014 00:00:00","EDT","LONGIL",61762,2099.7\n\
"09/10/2014 00:00:00","EDT","MHK VL",61756,714.7\n\
"09/10/2014 00:00:00","EDT","MILLWD",61759,235.5\n\
"09/10/2014 00:00:00","EDT","N.Y.C.",61761,5546.5\n\
"09/10/2014 00:00:00","EDT","NORTH",61755,436.2\n\
"09/10/2014 00:00:00","EDT","WEST",61752,1707.7\n\
"09/10/2014 00:05:00","EDT","CAPITL",61757,1180\n\
"09/10/2014 00:05:00","EDT","CENTRL",61754,1577.1\n\
"09/10/2014 00:05:00","EDT","DUNWOD",61760,596.2\n\
"09/10/2014 00:05:00","EDT","GENESE",61753,994.1\n\
"09/10/2014 00:05:00","EDT","HUD VL",61758,943.9\n\
"09/10/2014 00:05:00","EDT","LONGIL",61762,2083.9\n\
"09/10/2014 00:05:00","EDT","MHK VL",61756,723\n\
"09/10/2014 00:05:00","EDT","MILLWD",61759,240.1\n\
"09/10/2014 00:05:00","EDT","N.Y.C.",61761,5473.5\n\
"09/10/2014 00:05:00","EDT","NORTH",61755,421.1\n\
"09/10/2014 00:05:00","EDT","WEST",61752,1689.6\n\
"09/10/2014 00:10:00","EDT","CAPITL",61757,1172.8\n\
"09/10/2014 00:10:00","EDT","CENTRL",61754,1569.1\n\
"09/10/2014 00:10:00","EDT","DUNWOD",61760,601.2\n\
"09/10/2014 00:10:00","EDT","GENESE",61753,992.6\n\
"09/10/2014 00:10:00","EDT","HUD VL",61758,935.1\n\
"09/10/2014 00:10:00","EDT","LONGIL",61762,2067.5\n\
"09/10/2014 00:10:00","EDT","MHK VL",61756,712.9\n\
"09/10/2014 00:10:00","EDT","MILLWD",61759,247.2\n\
"09/10/2014 00:10:00","EDT","N.Y.C.",61761,5428.1\n\
"09/10/2014 00:10:00","EDT","NORTH",61755,431.4\n\
"09/10/2014 00:10:00","EDT","WEST",61752,1676.3\n\
"09/10/2014 00:15:00","EDT","CAPITL",61757,1172.8\n\
"09/10/2014 00:15:00","EDT","CENTRL",61754,1570.5\n\
"09/10/2014 00:15:00","EDT","DUNWOD",61760,595.8\n\
"09/10/2014 00:15:00","EDT","GENESE",61753,978.9\n\
"09/10/2014 00:15:00","EDT","HUD VL",61758,934.7\n\
"09/10/2014 00:15:00","EDT","LONGIL",61762,2040.8\n\
"09/10/2014 00:15:00","EDT","MHK VL",61756,724.5\n\
"09/10/2014 00:15:00","EDT","MILLWD",61759,235.8\n\
"09/10/2014 00:15:00","EDT","N.Y.C.",61761,5399.5\n\
"09/10/2014 00:15:00","EDT","NORTH",61755,430.3\n\
"09/10/2014 00:15:00","EDT","WEST",61752,1661.1\n\
"09/10/2014 19:35:00","EDT","CAPITL",61757,\n\
"09/10/2014 19:35:00","EDT","CENTRL",61754,\n\
"09/10/2014 19:35:00","EDT","DUNWOD",61760,\n\
"09/10/2014 19:35:00","EDT","GENESE",61753,\n\
"09/10/2014 19:35:00","EDT","HUD VL",61758,\n\
"09/10/2014 19:35:00","EDT","LONGIL",61762,\n\
"09/10/2014 19:35:00","EDT","MHK VL",61756,\n\
"09/10/2014 19:35:00","EDT","MILLWD",61759,\n\
"09/10/2014 19:35:00","EDT","N.Y.C.",61761,\n\
"09/10/2014 19:35:00","EDT","NORTH",61755,\n\
"09/10/2014 19:35:00","EDT","WEST",61752,\n\
')

        self.trade_csv = StringIO(u'Timestamp,Interface Name,Point ID,Flow (MWH),Positive Limit (MWH),Negative Limit (MWH)\n\
09/10/2014 00:00,WEST CENTRAL,23312,-106.15,2250,-9999\n\
09/10/2014 00:00,SCH - PJM_HTP,325905,0,660,-660\n\
09/10/2014 00:00,UPNY CONED,23315,1102.21,4850,-9999\n\
09/10/2014 00:00,SCH - PJ - NY,23316,-163.13,2450,-1300\n\
09/10/2014 00:00,SCH - OH - NY,23317,1011.6,1900,-1900\n\
09/10/2014 00:00,SCH - NE - NY,23318,-66.48,1400,-1600\n\
09/10/2014 00:00,MOSES SOUTH,23319,1452.05,2150,-1600\n\
09/10/2014 00:00,SPR/DUN-SOUTH,23320,2032.45,4350,-9999\n\
09/10/2014 00:00,SCH - HQ - NY,23324,1057,1320,-500\n\
09/10/2014 00:00,DYSINGER EAST,23326,303.25,2850,-9999\n\
09/10/2014 00:00,CENTRAL EAST - VC,23330,1738.13,2235,-9999\n\
09/10/2014 00:00,SCH - NPX_CSC,325154,330,330,-330\n\
09/10/2014 00:00,SCH - HQ_CEDARS,325274,7,190,-40\n\
09/10/2014 00:00,SCH - NPX_1385,325277,57,200,-200\n\
09/10/2014 00:00,SCH - PJM_NEPTUNE,325305,495,660,-660\n\
09/10/2014 00:00,SCH - HQ_IMPORT_EXPORT,325376,1001,1310,-9999\n\
09/10/2014 00:00,SCH - PJM_VFT,325658,-14,315,-315\n\
09/10/2014 00:00,TOTAL EAST,23314,2274.4,6500,-9999\n\
09/10/2014 00:05,SCH - HQ_CEDARS,325274,0,190,-40\n\
09/10/2014 00:05,SCH - NPX_CSC,325154,330,330,-330\n\
09/10/2014 00:05,SCH - PJM_NEPTUNE,325305,330,660,-660\n\
09/10/2014 00:05,SCH - HQ_IMPORT_EXPORT,325376,1059,1310,-9999\n\
09/10/2014 00:05,SCH - PJM_VFT,325658,-28,315,-315\n\
09/10/2014 00:05,SCH - PJM_HTP,325905,0,660,-660\n\
09/10/2014 00:05,WEST CENTRAL,23312,-127.47,2250,-9999\n\
09/10/2014 00:05,TOTAL EAST,23314,2492.19,6500,-9999\n\
09/10/2014 00:05,UPNY CONED,23315,1201.06,4850,-9999\n\
09/10/2014 00:05,SCH - PJ - NY,23316,-56.74,2450,-1300\n\
09/10/2014 00:05,SCH - OH - NY,23317,961.71,1900,-1900\n\
09/10/2014 00:05,SCH - NE - NY,23318,-93.37,1400,-1600\n\
09/10/2014 00:05,MOSES SOUTH,23319,1496.56,2150,-1600\n\
09/10/2014 00:05,SPR/DUN-SOUTH,23320,1993.25,4350,-9999\n\
09/10/2014 00:05,SCH - HQ - NY,23324,1109,1320,-500\n\
09/10/2014 00:05,DYSINGER EAST,23326,274.64,2850,-9999\n\
09/10/2014 00:05,CENTRAL EAST - VC,23330,1794.95,2235,-9999\n\
09/10/2014 00:05,SCH - NPX_1385,325277,65,200,-200\n\
09/10/2014 00:10,WEST CENTRAL,23312,-81.81,2250,-9999\n\
09/10/2014 00:10,SCH - PJM_HTP,325905,0,660,-660\n\
09/10/2014 00:10,UPNY CONED,23315,1217.03,4850,-9999\n\
09/10/2014 00:10,SCH - PJ - NY,23316,-60.19,2450,-1300\n\
09/10/2014 00:10,SCH - OH - NY,23317,961.12,1900,-1900\n\
09/10/2014 00:10,SCH - NE - NY,23318,-92.84,1400,-1600\n\
09/10/2014 00:10,MOSES SOUTH,23319,1484.79,2150,-1600\n\
09/10/2014 00:10,SPR/DUN-SOUTH,23320,2036.56,4350,-9999\n\
09/10/2014 00:10,SCH - HQ - NY,23324,1109,1320,-500\n\
09/10/2014 00:10,DYSINGER EAST,23326,314.15,2850,-9999\n\
09/10/2014 00:10,CENTRAL EAST - VC,23330,1802.89,2235,-9999\n\
09/10/2014 00:10,SCH - NPX_CSC,325154,330,330,-330\n\
09/10/2014 19:55,MOSES SOUTH,23319,1896.72,2150,-1600\n\
09/10/2014 19:55,SCH - NE - NY,23318,-302.78,1400,-1600\n\
09/10/2014 19:55,SCH - HQ - NY,23324,1274,1320,-500\n\
09/10/2014 19:55,DYSINGER EAST,23326,413.3,2850,-9999\n\
09/10/2014 19:55,CENTRAL EAST - VC,23330,2072.8,2380,-9999\n\
09/10/2014 19:55,SCH - NPX_CSC,325154,330,330,-330\n\
09/10/2014 19:55,SCH - HQ_CEDARS,325274,83,190,-40\n\
09/10/2014 19:55,SCH - NPX_1385,325277,90,200,-200\n\
09/10/2014 19:55,SCH - PJM_NEPTUNE,325305,580,660,-660\n\
09/10/2014 19:55,SCH - HQ_IMPORT_EXPORT,325376,1014,1310,-9999\n\
09/10/2014 19:55,SCH - PJM_VFT,325658,69,315,-315\n\
09/10/2014 19:55,SCH - PJM_HTP,325905,0,660,-660\n\
09/10/2014 19:55,WEST CENTRAL,23312,-55.11,2250,-9999\n\
09/10/2014 19:55,TOTAL EAST,23314,2841.45,6500,-9999\n\
09/10/2014 19:55,UPNY CONED,23315,1569.15,4850,-9999\n\
09/10/2014 19:55,SCH - PJ - NY,23316,-320.58,2450,-1300\n\
09/10/2014 19:55,SCH - OH - NY,23317,1259.54,1900,-1900\n\
09/10/2014 19:55,SPR/DUN-SOUTH,23320,2380.44,4350,-9999\n\
')

    def create_client(self, ba_name):
        # set up client with logging
        c = client_factory(ba_name)
        handler = logging.StreamHandler()
        c.logger.addHandler(handler)
        c.logger.setLevel(LOG_LEVEL)
        return c

    def test_parse_load(self):
        c = self.create_client('NYISO')
        data = c.parse_load(self.load_csv)
        expected_keys = ['timestamp', 'ba_name', 'load_MW', 'freq', 'market']
        for dp in data:
            self.assertEqual(sorted(dp.keys()), sorted(expected_keys))
            self.assertEqual(dp['timestamp'].date(), date(2014, 9, 10))
            self.assertGreater(dp['load_MW'], 15700)
            self.assertLess(dp['load_MW'], 16100)

        # should have 4 dps, even though file has 5 (last one has no data)
        self.assertEqual(len(data), 4)

    def test_parse_trade(self):
        c = self.create_client('NYISO')
        data = c.parse_trade(self.trade_csv)
        expected_keys = ['timestamp', 'ba_name', 'freq', 'market', 'net_exp_MW']
        for dp in data:
            self.assertEqual(dp['timestamp'].date(), date(2014, 9, 10))
            self.assertEqual(sorted(dp.keys()), sorted(expected_keys))

            self.assertLess(dp['net_exp_MW'], -1400)
            self.assertGreater(dp['net_exp_MW'], -6300)

        # should have 4 timestamps
        self.assertEqual(len(data), 4)

    def test_fetch_csv_load(self):
        c = self.create_client('NYISO')
        now = pytz.utc.localize(datetime.utcnow())
        today = now.astimezone(pytz.timezone(c.TZ_NAME)).date()
        content = c.fetch_csv(today, 'pal')
        self.assertEqual(content.split('\r\n')[0], '"Time Stamp","Time Zone","Name","PTID","Load"')

    def test_fetch_csv_trade(self):
        c = self.create_client('NYISO')
        now = pytz.utc.localize(datetime.utcnow())
        today = now.astimezone(pytz.timezone(c.TZ_NAME)).date()
        content = c.fetch_csv(today, 'ExternalLimitsFlows')
        self.assertEqual(content.split('\r\n')[0], 'Timestamp,Interface Name,Point ID,Flow (MWH),Positive Limit (MWH),Negative Limit (MWH)')
