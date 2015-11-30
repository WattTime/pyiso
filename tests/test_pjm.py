from pyiso import client_factory
from unittest import TestCase
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import pytz


class TestPJM(TestCase):
    def setUp(self):
        self.edata_main = "<html><w_page><head><title>PJM Wireless Data</title><meta content=\"60\" http-equiv=\"refresh\"/><meta content=\"width=320,user-scalable=false\" name=\"viewport\"/><link href=\"./style/style.css\" type=\"text/css\" rel=\"stylesheet\"/></head><body alink=\"#0000FF\" vlink=\"#0000FF\" link=\"#0000FF\" text=\"#000000\" bgcolor=\"#ffffff\"><table class=\"title\"><tr><td width=\"100%\" nowrap=\"true\" class=\"title\">Main</td><td nowrap=\"true\" align=\"right\"><a href=\"javascript: window.location.reload()\" class=\"main\">Refresh</a>\n\
                             | <a href=\"./SessionManager?a=contactPJM\" class=\"main\">Contact PJM</a></td></tr></table><br/>\n\
        Telemetry time: <span class=\"ts\">05/02/14 18:35 EDT</span><br/><br/><table><tr><td>PJM RTO Total</td><td class=\"value_num\">81,410 MW</td></tr><tr><td>Mid-Atlantic Region</td><td class=\"value_num\">28,396 MW</td></tr><tr><td>Southern Region</td><td class=\"value_num\">9,616 MW</td></tr><tr><td>Western Region</td><td class=\"value_num\">43,399 MW</td></tr></table><br/><p align=\"\"><a href=\"./SessionManager?a=instLoad\" class=\"\">Instantaneous Load</a><br/><a href=\"./SessionManager?a=forecastedLoad\" class=\"\">Forecasted Load</a><br/><a href=\"./SessionManager?a=tieFlow\" class=\"\">Tie Flows</a><br/><a href=\"./SessionManager?a=ace\" class=\"\">Area Control Error (ACE)</a><br/><a href=\"./SessionManager?a=drate\" class=\"\">Dispatch Rates</a><br/><a href=\"./SessionManager?a=constraint\" class=\"\">Constraints</a><br/><a href=\"./SessionManager?a=rti\" class=\"\">Reactive Transfer Interfaces</a><br/><a href=\"./SessionManager?a=reserve\" class=\"\">Reserve Quantities</a><br/><a href=\"./SessionManager?a=weatherCity\" class=\"\">Weather Data</a><br/><a href=\"./SessionManager?a=weatherImages\" class=\"\">Weather Images</a><br/><a href=\"./SessionManager?a=wind\" class=\"\">Instantaneous Wind Data</a><br/><a href=\"./SessionManager?a=forecastedWind\" class=\"\">Forecasted Wind Data</a><br/><a href=\"./SessionManager?a=zonalAggLmp\" class=\"\">Zonal Aggregrate LMPs</a><br/><a href=\"./SessionManager?a=busSearchInput\" class=\"\">[Search for Bus or Aggregate]</a><br/></p><form action=\"./SessionManager\">PNode Id Search: <input value=\"pnode\" name=\"a\" type=\"hidden\"/><input value=\"\" name=\"id\" size=\"9\" maxLength=\"9\"/><input value=\"Search\" type=\"submit\"/></form><body_footer/></body><page_footer/></w_page></html>"

        self.edata_wind = "<html><w_page><head><title>PJM Wireless (Wind)</title><meta content=\"60\" http-equiv=\"refresh\"/><meta content=\"width=320,user-scalable=false\" name=\"viewport\"/><link href=\"./style/style.css\" type=\"text/css\" rel=\"stylesheet\"/></head><body alink=\"#0000FF\" vlink=\"#0000FF\" link=\"#0000FF\" text=\"#000000\" bgcolor=\"#ffffff\"><table class=\"title\"><tr><td width=\"100%\" nowrap=\"true\" class=\"title\">RTO Wind Power</td><td nowrap=\"true\" align=\"right\"><a href=\"javascript: window.location.reload()\" class=\"main\">Refresh</a>\n\
                             | <a href=\"./SessionManager?a=main\" class=\"main\">Main</a></td></tr></table><br/>\n\
        Telemetry time: <span class=\"ts\">05/02/14 18:37:57 EDT</span><br/><br/><table><tr><th align=\"center\"> Wind </th><th align=\"center\"> MW </th></tr><tr><td><a href=\"./SessionManager?a=windHistory&amp;id=22023\" class=\"\">RTO Wind Power</a></td><td class=\"value_num\">2,379</td></tr></table><body_footer/></body><page_footer/></w_page></html>"

        self.edata_load = "<html><w_page><head><title>PJM Wireless (Inst. Load)</title><meta content=\"60\" http-equiv=\"refresh\"/><meta content=\"width=320,user-scalable=false\" name=\"viewport\"/><link href=\"./style/style.css\" type=\"text/css\" rel=\"stylesheet\"/></head><body alink=\"#0000FF\" vlink=\"#0000FF\" link=\"#0000FF\" text=\"#000000\" bgcolor=\"#ffffff\"><table class=\"title\"><tr><td width=\"100%\" nowrap=\"true\" class=\"title\">Instantaneous Load</td><td nowrap=\"true\" align=\"right\"><a href=\"javascript: window.location.reload()\" class=\"main\">Refresh</a>\n\
                             | <a href=\"./SessionManager?a=main\" class=\"main\">Main</a></td></tr></table><br/>\n\
        Telemetry time: <span class=\"ts\">05/02/14 18:35 EDT</span><br/><br/><table><tr><th align=\"center\">Load</th><th align=\"center\">MW</th></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=19994\" class=\"\">PJM RTO Total</a></td><td class=\"value_num\">81,483</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=2603\" class=\"\">Mid-Atlantic Region</a></td><td class=\"value_num\">28,364</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999023\" class=\"\">Southern Region</a></td><td class=\"value_num\">9,579</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=9277\" class=\"\">Western Region</a></td><td class=\"value_num\">43,539</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999007\" class=\"\">AE Zone</a></td><td class=\"value_num\">1,029</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999019\" class=\"\">AEP Zone</a></td><td class=\"value_num\">12,881</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999013\" class=\"\">APS Zone</a></td><td class=\"value_num\">4,849</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999207\" class=\"\">ATSI Zone</a></td><td class=\"value_num\">7,571</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999005\" class=\"\">BC Zone</a></td><td class=\"value_num\">3,133</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999015\" class=\"\">COMED Zone</a></td><td class=\"value_num\">10,567</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999020\" class=\"\">DAYTON Zone</a></td><td class=\"value_num\">1,753</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999211\" class=\"\">DEOK Zone</a></td><td class=\"value_num\">2,715</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999022\" class=\"\">DOM Zone</a></td><td class=\"value_num\">9,563</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999008\" class=\"\">DPL Zone</a></td><td class=\"value_num\">1,776</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999025\" class=\"\">DUQ Zone</a></td><td class=\"value_num\">1,500</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999213\" class=\"\">EKPC Zone</a></td><td class=\"value_num\">1,234</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999009\" class=\"\">JC Zone</a></td><td class=\"value_num\">2,403</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999010\" class=\"\">ME Zone</a></td><td class=\"value_num\">1,517</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999003\" class=\"\">PE Zone</a></td><td class=\"value_num\">4,198</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999006\" class=\"\">PEP Zone</a></td><td class=\"value_num\">3,054</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999004\" class=\"\">PL Zone</a></td><td class=\"value_num\">4,195</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999011\" class=\"\">PN Zone</a></td><td class=\"value_num\">2,018</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999002\" class=\"\">PS Zone</a></td><td class=\"value_num\">4,706</td></tr><tr><td><a href=\"./SessionManager?a=instLoadHistory&amp;id=999012\" class=\"\">RECO Zone</a></td><td class=\"value_num\">169</td></tr></table><body_footer/></body><page_footer/></w_page></html>"

        self.edata_forecast_load = """<html><w_page><head><title>PJM Wireless (For. Load Hist.)</title><meta content="60" http-equiv="refresh"/><meta content="width=320,user-scalable=false" name="viewport"/><link href="./style/style.css" type="text/css" rel="stylesheet"/></head><body alink="#0000FF" vlink="#0000FF" link="#0000FF" text="#000000" bgcolor="#ffffff"><table class="title"><tr><td width="100%" nowrap="true" class="title">Forecasted Load History</td><td nowrap="true" align="right"><a href="javascript: window.location.reload()" class="main">Refresh</a>
                             | <a href="./SessionManager?a=main" class="main">Main</a></td></tr></table><br/><span class="data_name">Load: </span><span class="data_value">PJM RTO Total</span><br/><br/>
        Forecast time: <span class="ts">11/21/15 12:30 EST</span><br/><br/><table><tr><th align="center">Timestamp</th><th align="center">MW</th></tr><tr><td>11/21/15 12:30 EST</td><td class="value_num">81,690</td></tr><tr><td>11/21/15 13:30 EST</td><td class="value_num">79,944</td></tr><tr><td>11/21/15 14:30 EST</td><td class="value_num">78,739</td></tr><tr><td>11/21/15 15:30 EST</td><td class="value_num">78,364</td></tr><tr><td>11/21/15 16:30 EST</td><td class="value_num">80,127</td></tr><tr><td>11/21/15 17:30 EST</td><td class="value_num">85,378</td></tr><tr><td>11/21/15 18:30 EST</td><td class="value_num">87,475</td></tr><tr><td>11/21/15 19:30 EST</td><td class="value_num">87,030</td></tr><tr><td>11/21/15 20:30 EST</td><td class="value_num">86,104</td></tr><tr><td>11/21/15 21:30 EST</td><td class="value_num">83,997</td></tr><tr><td>11/21/15 22:30 EST</td><td class="value_num">80,746</td></tr><tr><td>11/21/15 23:30 EST</td><td class="value_num">77,062</td></tr><tr><td>11/22/15 00:00 EST</td><td class="value_num">42,748</td></tr><tr><td>11/22/15 00:30 EST</td><td class="value_num">76,358</td></tr><tr><td>11/22/15 01:30 EST</td><td class="value_num">74,158</td></tr><tr><td>11/22/15 02:30 EST</td><td class="value_num">73,253</td></tr><tr><td>11/22/15 03:30 EST</td><td class="value_num">72,976</td></tr><tr><td>11/22/15 04:30 EST</td><td class="value_num">73,739</td></tr><tr><td>11/22/15 05:30 EST</td><td class="value_num">75,497</td></tr><tr><td>11/22/15 06:30 EST</td><td class="value_num">78,540</td></tr><tr><td>11/22/15 07:30 EST</td><td class="value_num">81,420</td></tr><tr><td>11/22/15 08:30 EST</td><td class="value_num">84,104</td></tr><tr><td>11/22/15 09:30 EST</td><td class="value_num">85,902</td></tr><tr><td>11/22/15 10:30 EST</td><td class="value_num">86,260</td></tr></table><body_footer/></body><page_footer/></w_page></html>"
        """

        self.edata_flow = "<html><w_page><head><title>PJM Wireless (Tie Flows)</title><meta content=\"60\" http-equiv=\"refresh\"/><meta content=\"width=320,user-scalable=false\" name=\"viewport\"/><link href=\"./style/style.css\" type=\"text/css\" rel=\"stylesheet\"/></head><body alink=\"#0000FF\" vlink=\"#0000FF\" link=\"#0000FF\" text=\"#000000\" bgcolor=\"#ffffff\"><table class=\"title\"><tr><td width=\"100%\" nowrap=\"true\" class=\"title\">Actual Tie Flows</td><td nowrap=\"true\" align=\"right\"><a href=\"javascript: window.location.reload()\" class=\"main\">Refresh</a>\n\
                             | <a href=\"./SessionManager?a=main\" class=\"main\">Main</a></td></tr></table><br/>\n\
        Telemetry time: <span class=\"ts\">05/02/14 19:15 EDT</span><br/><br/><table><tr><th align=\"center\"> Tie Flow </th><th align=\"center\"> MW </th></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99068\" class=\"\">PJM RTO</a></td><td class=\"value_num\">2,140</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99033\" class=\"\">ALTE</a></td><td class=\"value_num\">-790</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99031\" class=\"\">ALTW</a></td><td class=\"value_num\">166</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99073\" class=\"\">AMIL</a></td><td class=\"value_num\">636</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99048\" class=\"\">CIN</a></td><td class=\"value_num\">-246</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99052\" class=\"\">CPLE</a></td><td class=\"value_num\">1,076</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99054\" class=\"\">CPLW</a></td><td class=\"value_num\">-83</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99070\" class=\"\">CWLP</a></td><td class=\"value_num\">15</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99056\" class=\"\">DUKE</a></td><td class=\"value_num\">333</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99082\" class=\"\">HTP</a></td><td class=\"value_num\">0</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99058\" class=\"\">IPL</a></td><td class=\"value_num\">414</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99066\" class=\"\">LGEE</a></td><td class=\"value_num\">292</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99077\" class=\"\">LINDEN</a></td><td class=\"value_num\">-2</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99029\" class=\"\">MEC</a></td><td class=\"value_num\">295</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99060\" class=\"\">MECS</a></td><td class=\"value_num\">-1,970</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99075\" class=\"\">SAYR</a></td><td class=\"value_num\">0</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99021\" class=\"\">NIPS</a></td><td class=\"value_num\">-660</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=2556\" class=\"\">NYIS</a></td><td class=\"value_num\">-560</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99062\" class=\"\">OVEC</a></td><td class=\"value_num\">326</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99064\" class=\"\">TVA</a></td><td class=\"value_num\">1,377</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99035\" class=\"\">WEC</a></td><td class=\"value_num\">973</td></tr><tr><td><a href=\"./SessionManager?a=tieFlowHistory&amp;id=99087\" class=\"\">PJM MISO</a></td><td class=\"value_num\">550</td></tr></table><body_footer/></body><page_footer/></w_page></html>"

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

    def test_time_from_load(self):
        soup = BeautifulSoup(self.edata_load)
        ts = self.c.time_from_soup(soup)
        self.assertEqual(ts, datetime(2014, 5, 2, 18, 35, tzinfo=pytz.utc) + timedelta(hours=4))

    def test_val_from_load(self):
        soup = BeautifulSoup(self.edata_load)
        val = self.c.val_from_soup(soup, 'PJM RTO Total')
        self.assertEqual(val, 81483)

    def test_time_from_wind(self):
        soup = BeautifulSoup(self.edata_wind)
        ts = self.c.time_from_soup(soup)
        self.assertEqual(ts, datetime(2014, 5, 2, 18, 37, 57, tzinfo=pytz.utc) + timedelta(hours=4))

    def test_val_from_wind(self):
        soup = BeautifulSoup(self.edata_wind)
        val = self.c.val_from_soup(soup, 'RTO Wind Power')
        self.assertEqual(val, 2379)

    def test_time_from_flow(self):
        soup = BeautifulSoup(self.edata_flow)
        ts = self.c.time_from_soup(soup)
        self.assertEqual(ts, datetime(2014, 5, 2, 19, 15, tzinfo=pytz.utc) + timedelta(hours=4))

    def test_val_from_flow(self):
        soup = BeautifulSoup(self.edata_flow)
        val = self.c.val_from_soup(soup, 'PJM RTO')
        self.assertEqual(val, 2140)

    def test_parse_forecast_load(self):
        dfs = pd.read_html(self.edata_forecast_load, header=0, index_col=0, parse_dates=True)
        df = self.c.utcify_index(dfs[1])
        self.assertEqual(df.columns, 'MW')
        self.assertEqual(df.shape, (24, 1))
        self.assertEqual(df.index[0], pytz.utc.localize(datetime(2015, 11, 21, 17, 30)))
        self.assertEqual(df.index[-1], pytz.utc.localize(datetime(2015, 11, 22, 15, 30)))

    def test_missing_time_is_none(self):
        soup = BeautifulSoup('')
        ts = self.c.time_from_soup(soup)
        self.assertIsNone(ts)

    def test_missing_val_is_none(self):
        soup = BeautifulSoup('')
        val = self.c.val_from_soup(soup, 'key')
        self.assertIsNone(val)

    def test_bad_url(self):
        ts, val = self.c.fetch_edata_point('badtype', 'badkey')
        self.assertIsNone(ts)
        self.assertIsNone(val)
