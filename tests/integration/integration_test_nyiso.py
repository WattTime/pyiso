from datetime import datetime
from unittest import TestCase

import pytz

from pyiso import client_factory


class IntegrationTestNYISOClient(TestCase):
    def setUp(self):
        self.c = client_factory('NYISO')

    def test_fetch_csv_load(self):
        self.c.options = {'data': 'dummy'}
        now = pytz.utc.localize(datetime.utcnow())
        today = now.astimezone(pytz.timezone(self.c.TZ_NAME)).date()
        content_list = self.c.fetch_csvs(today, 'pal')
        self.assertEqual(len(content_list), 1)
        self.assertEqual(content_list[0].split('\r\n')[0],
                         '"Time Stamp","Time Zone","Name","PTID","Load"')

    def test_fetch_csv_load_forecast(self):
        self.c.options = {'data': 'dummy'}
        now = pytz.utc.localize(datetime.utcnow())
        today = now.astimezone(pytz.timezone(self.c.TZ_NAME)).date()
        content_list = self.c.fetch_csvs(today, 'isolf')
        self.assertEqual(len(content_list), 1)
        self.assertEqual(content_list[0].split('\n')[0],
                         '"Time Stamp","Capitl","Centrl","Dunwod","Genese","Hud Vl","Longil","Mhk Vl","Millwd","N.Y.C.","North","West","NYISO"')

    def test_fetch_csv_trade(self):
        self.c.options = {'data': 'dummy'}
        now = pytz.utc.localize(datetime.utcnow())
        today = now.astimezone(pytz.timezone(self.c.TZ_NAME)).date()
        content_list = self.c.fetch_csvs(today, 'ExternalLimitsFlows')
        self.assertEqual(len(content_list), 1)
        self.assertEqual(content_list[0].split('\r\n')[0],
                         'Timestamp,Interface Name,Point ID,Flow (MWH),Positive Limit (MWH),Negative Limit (MWH)')

    def test_fetch_csv_genmix(self):
        self.c.options = {'data': 'dummy'}
        now = pytz.utc.localize(datetime.utcnow())
        today = now.astimezone(pytz.timezone(self.c.TZ_NAME)).date()
        content_list = self.c.fetch_csvs(today, 'rtfuelmix')
        self.assertEqual(len(content_list), 1)
        self.assertEqual(content_list[0].split('\r\n')[0],
                         'Time Stamp,Time Zone,Fuel Category,Gen MWh')

    def test_fetch_csv_lmp(self):
        self.c.options = {'data': 'lmp'}
        now = pytz.utc.localize(datetime.utcnow())
        today = now.astimezone(pytz.timezone(self.c.TZ_NAME)).date()
        content_list = self.c.fetch_csvs(today, 'realtime')
        self.assertEqual(len(content_list), 1)
        self.assertEqual(content_list[0].split('\r\n')[0],
                         '"Time Stamp","Name","PTID","LBMP ($/MWHr)","Marginal Cost Losses ($/MWHr)","Marginal Cost Congestion ($/MWHr)"')
