from pyiso import client_factory
from unittest import TestCase, expectedFailure
from io import StringIO
import pandas as pd
import pytz
from datetime import date, datetime, timedelta
from bs4 import BeautifulSoup
import numpy
import mock
import requests


class TestCAISOBase(TestCase):
    def setUp(self):
        self.ren_report_tsv = StringIO(u"03/12/14\t\t\tHourly Breakdown of Renewable Resources (MW)\t\t\t\t\t\t\t\t\t\t\t\t\n\
\tHour\t\tGEOTHERMAL\tBIOMASS\t\tBIOGAS\t\tSMALL HYDRO\tWIND TOTAL\tSOLAR PV\tSOLAR THERMAL\t\t\t\t\n\
\t1\t\t900\t\t313\t\t190\t\t170\t\t1596\t\t0\t\t0\n\
\t2\t\t900\t\t314\t\t189\t\t169\t\t1814\t\t0\t\t0\n\
\t3\t\t900\t\t328\t\t190\t\t170\t\t2076\t\t0\t\t0\n\
\t4\t\t900\t\t334\t\t190\t\t167\t\t2086\t\t0\t\t0\n\
\t5\t\t900\t\t344\t\t190\t\t167\t\t1893\t\t0\t\t0\n\
\t6\t\t900\t\t344\t\t190\t\t166\t\t1650\t\t0\t\t0\n\
\t7\t\t899\t\t339\t\t189\t\t177\t\t1459\t\t0\t\t0\n\
\t8\t\t897\t\t341\t\t188\t\t179\t\t1487\t\t245\t\t0\n\
\t9\t\t898\t\t341\t\t187\t\t171\t\t1502\t\t1455\t\t5\n\
\t10\t\t898\t\t341\t\t186\t\t169\t\t1745\t\t2613\t\t251\n\
\t11\t\t898\t\t342\t\t183\t\t168\t\t2204\t\t3318\t\t336\n\
\t12\t\t897\t\t358\t\t185\t\t168\t\t2241\t\t3558\t\t341\n\
\t13\t\t897\t\t367\t\t189\t\t169\t\t1850\t\t3585\t\t326\n\
\t14\t\t897\t\t367\t\t191\t\t169\t\t1632\t\t3598\t\t316\n\
\t15\t\t892\t\t364\t\t193\t\t171\t\t1317\t\t3541\t\t299\n\
\t16\t\t890\t\t368\t\t192\t\t174\t\t1156\t\t3359\t\t338\n\
\t17\t\t891\t\t367\t\t189\t\t187\t\t1082\t\t2697\t\t400\n\
\t18\t\t892\t\t362\t\t192\t\t202\t\t1047\t\t1625\t\t325\n\
\t19\t\t892\t\t361\t\t196\t\t205\t\t861\t\t306\t\t32\n\
\t20\t\t892\t\t359\t\t197\t\t214\t\t725\t\t0\t\t0\n\
\t21\t\t895\t\t354\t\t192\t\t208\t\t722\t\t0\t\t0\n\
\t22\t\t900\t\t355\t\t182\t\t209\t\t689\t\t0\t\t0\n\
\t23\t\t901\t\t355\t\t182\t\t198\t\t482\t\t0\t\t0\n\
\t24\t\t902\t\t357\t\t183\t\t170\t\t507\t\t0\t\t0\n\
\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\n\
\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\n\
\t\t\tHourly Breakdown of Total Production by Resource Type (MW)\t\t\t\t\t\t\t\t\t\t\t\t\n\
\tHour\t\tRENEWABLES\tNUCLEAR\t\tTHERMAL\t\tIMPORTS\t\tHYDRO\t\t\t\t\t\n\
\t1\t\t3170\t\t1092\t\t6862\t\t9392\t\t871\t\t\t\t\n\
\t2\t\t3386\t\t1092\t\t6181\t\t9144\t\t709\t\t\t\t\n\
\t3\t\t3663\t\t1092\t\t6016\t\t8993\t\t675\t\t\t\t\n\
\t4\t\t3678\t\t1091\t\t5900\t\t9107\t\t669\t\t\t\t\n\
\t5\t\t3494\t\t1092\t\t6041\t\t9080\t\t840\t\t\t\t\n\
\t6\t\t3251\t\t1091\t\t7115\t\t8848\t\t919\t\t\t\t\n\
\t7\t\t3064\t\t1091\t\t9917\t\t8464\t\t1269\t\t\t\t\n\
\t8\t\t3337\t\t1091\t\t10955\t\t8694\t\t1387\t\t\t\t\n\
\t9\t\t4559\t\t1091\t\t10291\t\t8667\t\t1120\t\t\t\t\n\
\t10\t\t6204\t\t1091\t\t8967\t\t8783\t\t829\t\t\t\t\n\
\t11\t\t7449\t\t1091\t\t8095\t\t8776\t\t656\t\t\t\t\n\
\t12\t\t7747\t\t1089\t\t8068\t\t8565\t\t703\t\t\t\t\n\
\t13\t\t7382\t\t1087\t\t8157\t\t8736\t\t862\t\t\t\t\n\
\t14\t\t7171\t\t1085\t\t8564\t\t8633\t\t973\t\t\t\t\n\
\t15\t\t6776\t\t1079\t\t8588\t\t8832\t\t1004\t\t\t\t\n\
\t16\t\t6477\t\t1079\t\t8890\t\t8857\t\t980\t\t\t\t\n\
\t17\t\t5812\t\t1080\t\t8937\t\t9168\t\t1210\t\t\t\t\n\
\t18\t\t4645\t\t1081\t\t9620\t\t9481\t\t1443\t\t\t\t\n\
\t19\t\t2853\t\t1081\t\t11258\t\t9653\t\t1763\t\t\t\t\n\
\t20\t\t2388\t\t1081\t\t12971\t\t10022\t\t1979\t\t\t\t\n\
\t21\t\t2370\t\t1081\t\t12644\t\t10324\t\t2130\t\t\t\t\n\
\t22\t\t2335\t\t1082\t\t11820\t\t10069\t\t1774\t\t\t\t\n\
\t23\t\t2119\t\t1085\t\t10712\t\t9871\t\t1250\t\t\t\t\n\
\t24\t\t2118\t\t1082\t\t9800\t\t8904\t\t935\t\t\t\t\n\
")

        self.sld_fcst_xml = StringIO(u"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<OASISReport xmlns=\"http://www.caiso.com/soa/OASISReport_v1.xsd\">\n\
<MessageHeader>\n\
<TimeDate>2014-05-09T17:50:06-00:00</TimeDate>\n\
<Source>OASIS</Source>\n\
<Version>v20140401</Version>\n\
</MessageHeader>\n\
<MessagePayload>\n\
<RTO>\n\
<name>CAISO</name>\n\
<REPORT_ITEM>\n\
<REPORT_HEADER>\n\
<SYSTEM>OASIS</SYSTEM>\n\
<TZ>PPT</TZ>\n\
<REPORT>SLD_FCST</REPORT>\n\
<MKT_TYPE>RTM</MKT_TYPE>\n\
<EXECUTION_TYPE>RTPD</EXECUTION_TYPE>\n\
<UOM>MW</UOM>\n\
<INTERVAL>ENDING</INTERVAL>\n\
<SEC_PER_INTERVAL>900</SEC_PER_INTERVAL>\n\
</REPORT_HEADER>\n\
<REPORT_DATA>\n\
<DATA_ITEM>SYS_FCST_15MIN_MW</DATA_ITEM>\n\
<RESOURCE_NAME>CA ISO-TAC</RESOURCE_NAME>\n\
<OPR_DATE>2014-05-08</OPR_DATE>\n\
<INTERVAL_NUM>50</INTERVAL_NUM>\n\
<INTERVAL_START_GMT>2014-05-08T19:15:00-00:00</INTERVAL_START_GMT>\n\
<INTERVAL_END_GMT>2014-05-08T19:30:00-00:00</INTERVAL_END_GMT>\n\
<VALUE>26723</VALUE>\n\
</REPORT_DATA>\n\
<REPORT_DATA>\n\
<DATA_ITEM>SYS_FCST_5MIN_MW</DATA_ITEM>\n\
<RESOURCE_NAME>CA ISO-TAC</RESOURCE_NAME>\n\
<OPR_DATE>2014-05-08</OPR_DATE>\n\
<INTERVAL_NUM>144</INTERVAL_NUM>\n\
<INTERVAL_START_GMT>2014-05-08T18:55:00-00:00</INTERVAL_START_GMT>\n\
<INTERVAL_END_GMT>2014-05-08T19:00:00-00:00</INTERVAL_END_GMT>\n\
<VALUE>26755</VALUE>\n\
</REPORT_DATA>\n\
<REPORT_DATA>\n\
<DATA_ITEM>SYS_FCST_5MIN_MW</DATA_ITEM>\n\
<RESOURCE_NAME>PGE-TAC</RESOURCE_NAME>\n\
<OPR_DATE>2014-05-08</OPR_DATE>\n\
<INTERVAL_NUM>146</INTERVAL_NUM>\n\
<INTERVAL_START_GMT>2014-05-08T19:05:00-00:00</INTERVAL_START_GMT>\n\
<INTERVAL_END_GMT>2014-05-08T19:10:00-00:00</INTERVAL_END_GMT>\n\
<VALUE>11530</VALUE>\n\
</REPORT_DATA>\n\
</REPORT_ITEM>\n\
<DISCLAIMER_ITEM>\n\
<DISCLAIMER>The contents of these pages are subject to change without notice.  Decisions based on information contained within the California ISO's web site are the visitor's sole responsibility.</DISCLAIMER>\n\
</DISCLAIMER_ITEM>\n\
</RTO>\n\
</MessagePayload>\n\
</OASISReport>")

        self.ene_slrs_xml = StringIO(u"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<OASISReport xmlns=\"http://www.caiso.com/soa/OASISReport_v1.xsd\">\n\
<MessageHeader>\n\
<TimeDate>2014-02-24T20:25:40-00:00</TimeDate>\n\
<Source>OASIS</Source>\n\
<Version>v20131201</Version>\n\
</MessageHeader>\n\
<MessagePayload>\n\
<RTO>\n\
<name>CAISO</name>\n\
<REPORT_ITEM>\n\
<REPORT_HEADER>\n\
<SYSTEM>OASIS</SYSTEM>\n\
<TZ>PPT</TZ>\n\
<REPORT>ENE_SLRS</REPORT>\n\
<MKT_TYPE>DAM</MKT_TYPE>\n\
<UOM>MW</UOM>\n\
<INTERVAL>ENDING</INTERVAL>\n\
<SEC_PER_INTERVAL>3600</SEC_PER_INTERVAL>\n\
</REPORT_HEADER>\n\
<REPORT_DATA>\n\
<DATA_ITEM>ISO_TOT_EXP_MW</DATA_ITEM>\n\
<RESOURCE_NAME>Caiso_Totals</RESOURCE_NAME>\n\
<OPR_DATE>2013-09-19</OPR_DATE>\n\
<INTERVAL_NUM>9</INTERVAL_NUM>\n\
<INTERVAL_START_GMT>2013-09-19T15:00:00-00:00</INTERVAL_START_GMT>\n\
<INTERVAL_END_GMT>2013-09-19T16:00:00-00:00</INTERVAL_END_GMT>\n\
<VALUE>704</VALUE>\n\
</REPORT_DATA>\n\
<REPORT_DATA>\n\
<DATA_ITEM>ISO_TOT_EXP_MW</DATA_ITEM>\n\
<RESOURCE_NAME>Caiso_Totals</RESOURCE_NAME>\n\
<OPR_DATE>2013-09-19</OPR_DATE>\n\
<INTERVAL_NUM>14</INTERVAL_NUM>\n\
<INTERVAL_START_GMT>2013-09-19T20:00:00-00:00</INTERVAL_START_GMT>\n\
<INTERVAL_END_GMT>2013-09-19T21:00:00-00:00</INTERVAL_END_GMT>\n\
<VALUE>884</VALUE>\n\
</REPORT_DATA>\n\
<REPORT_DATA>\n\
<DATA_ITEM>ISO_TOT_GEN_MW</DATA_ITEM>\n\
<RESOURCE_NAME>Caiso_Totals</RESOURCE_NAME>\n\
<OPR_DATE>2013-09-19</OPR_DATE>\n\
<INTERVAL_NUM>18</INTERVAL_NUM>\n\
<INTERVAL_START_GMT>2013-09-20T00:00:00-00:00</INTERVAL_START_GMT>\n\
<INTERVAL_END_GMT>2013-09-20T01:00:00-00:00</INTERVAL_END_GMT>\n\
<VALUE>27521.96</VALUE>\n\
</REPORT_DATA>\n\
<REPORT_DATA>\n\
<DATA_ITEM>ISO_TOT_GEN_MW</DATA_ITEM>\n\
<RESOURCE_NAME>Caiso_Totals</RESOURCE_NAME>\n\
<OPR_DATE>2013-09-19</OPR_DATE>\n\
<INTERVAL_NUM>11</INTERVAL_NUM>\n\
<INTERVAL_START_GMT>2013-09-19T17:00:00-00:00</INTERVAL_START_GMT>\n\
<INTERVAL_END_GMT>2013-09-19T18:00:00-00:00</INTERVAL_END_GMT>\n\
<VALUE>23900.79</VALUE>\n\
</REPORT_DATA>\n\
<REPORT_DATA>\n\
<DATA_ITEM>ISO_TOT_IMP_MW</DATA_ITEM>\n\
<RESOURCE_NAME>Caiso_Totals</RESOURCE_NAME>\n\
<OPR_DATE>2013-09-19</OPR_DATE>\n\
<INTERVAL_NUM>14</INTERVAL_NUM>\n\
<INTERVAL_START_GMT>2013-09-19T20:00:00-00:00</INTERVAL_START_GMT>\n\
<INTERVAL_END_GMT>2013-09-19T21:00:00-00:00</INTERVAL_END_GMT>\n\
<VALUE>7248</VALUE>\n\
</REPORT_DATA>\n\
<REPORT_DATA>\n\
<DATA_ITEM>ISO_TOT_IMP_MW</DATA_ITEM>\n\
<RESOURCE_NAME>Caiso_Totals</RESOURCE_NAME>\n\
<OPR_DATE>2013-09-19</OPR_DATE>\n\
<INTERVAL_NUM>1</INTERVAL_NUM>\n\
<INTERVAL_START_GMT>2013-09-19T07:00:00-00:00</INTERVAL_START_GMT>\n\
<INTERVAL_END_GMT>2013-09-19T08:00:00-00:00</INTERVAL_END_GMT>\n\
<VALUE>5014</VALUE>\n\
</REPORT_DATA>\n\
<DISCLAIMER_ITEM>\n\
<DISCLAIMER>The contents of these pages are subject to change without notice.  Decisions based on information contained within the California ISO's web site are the visitor's sole responsibility.</DISCLAIMER>\n\
</DISCLAIMER_ITEM>\n\
</RTO>\n\
</MessagePayload>\n\
</OASISReport>\n\
")

        self.sld_ren_fcst_xml = StringIO(u"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<OASISReport xmlns=\"http://www.caiso.com/soa/OASISReport_v1.xsd\">\n\
<MessageHeader>\n\
<TimeDate>2014-02-24T18:51:45-00:00</TimeDate>\n\
<Source>OASIS</Source>\n\
<Version>v20131201</Version>\n\
</MessageHeader>\n\
<MessagePayload>\n\
<RTO>\n\
<name>CAISO</name>\n\
<REPORT_ITEM>\n\
<REPORT_HEADER>\n\
<SYSTEM>OASIS</SYSTEM>\n\
<TZ>PPT</TZ>\n\
<REPORT>SLD_REN_FCST</REPORT>\n\
<MKT_TYPE>DAM</MKT_TYPE>\n\
<UOM>MW</UOM>\n\
<INTERVAL>ENDING</INTERVAL>\n\
<SEC_PER_INTERVAL>3600</SEC_PER_INTERVAL>\n\
</REPORT_HEADER>\n\
<REPORT_DATA>\n\
<DATA_ITEM>RENEW_FCST_DA_MW</DATA_ITEM>\n\
<OPR_DATE>2013-09-19</OPR_DATE>\n\
<INTERVAL_NUM>1</INTERVAL_NUM>\n\
<INTERVAL_START_GMT>2013-09-19T07:00:00-00:00</INTERVAL_START_GMT>\n\
<INTERVAL_END_GMT>2013-09-19T08:00:00-00:00</INTERVAL_END_GMT>\n\
<VALUE>0.01</VALUE>\n\
<TRADING_HUB>NP15</TRADING_HUB>\n\
<RENEWABLE_TYPE>Solar</RENEWABLE_TYPE>\n\
</REPORT_DATA>\n\
<REPORT_DATA>\n\
<DATA_ITEM>RENEW_FCST_DA_MW</DATA_ITEM>\n\
<OPR_DATE>2013-09-19</OPR_DATE>\n\
<INTERVAL_NUM>2</INTERVAL_NUM>\n\
<INTERVAL_START_GMT>2013-09-19T08:00:00-00:00</INTERVAL_START_GMT>\n\
<INTERVAL_END_GMT>2013-09-19T09:00:00-00:00</INTERVAL_END_GMT>\n\
<VALUE>0</VALUE>\n\
<TRADING_HUB>NP15</TRADING_HUB>\n\
<RENEWABLE_TYPE>Solar</RENEWABLE_TYPE>\n\
</REPORT_DATA>\n\
</REPORT_ITEM>\n\
<REPORT_ITEM>\n\
<REPORT_HEADER>\n\
<SYSTEM>OASIS</SYSTEM>\n\
<TZ>PPT</TZ>\n\
<REPORT>SLD_REN_FCST</REPORT>\n\
<MKT_TYPE>DAM</MKT_TYPE>\n\
<UOM>MW</UOM>\n\
<INTERVAL>ENDING</INTERVAL>\n\
<SEC_PER_INTERVAL>3600</SEC_PER_INTERVAL>\n\
</REPORT_HEADER>\n\
<REPORT_DATA>\n\
<DATA_ITEM>RENEW_FCST_DA_MW</DATA_ITEM>\n\
<OPR_DATE>2013-09-19</OPR_DATE>\n\
<INTERVAL_NUM>1</INTERVAL_NUM>\n\
<INTERVAL_START_GMT>2013-09-19T07:00:00-00:00</INTERVAL_START_GMT>\n\
<INTERVAL_END_GMT>2013-09-19T08:00:00-00:00</INTERVAL_END_GMT>\n\
<VALUE>478.86</VALUE>\n\
<TRADING_HUB>NP15</TRADING_HUB>\n\
<RENEWABLE_TYPE>Wind</RENEWABLE_TYPE>\n\
</REPORT_DATA>\n\
<REPORT_DATA>\n\
<DATA_ITEM>RENEW_FCST_DA_MW</DATA_ITEM>\n\
<OPR_DATE>2013-09-19</OPR_DATE>\n\
<INTERVAL_NUM>24</INTERVAL_NUM>\n\
<INTERVAL_START_GMT>2013-09-20T06:00:00-00:00</INTERVAL_START_GMT>\n\
<INTERVAL_END_GMT>2013-09-20T07:00:00-00:00</INTERVAL_END_GMT>\n\
<VALUE>580.83</VALUE>\n\
<TRADING_HUB>NP15</TRADING_HUB>\n\
<RENEWABLE_TYPE>Wind</RENEWABLE_TYPE>\n\
</REPORT_DATA>\n\
</REPORT_ITEM>\n\
<DISCLAIMER_ITEM>\n\
<DISCLAIMER>The contents of these pages are subject to change without notice.  Decisions based on information contained within the California ISO's web site are the visitor's sole responsibility.</DISCLAIMER>\n\
</DISCLAIMER_ITEM>\n\
</RTO>\n\
</MessagePayload>\n\
</OASISReport>\n\
")

        self.todays_outlook_renewables = StringIO(u"<!doctype html public \"-//W3C//DTD HTML 3.2 Final//EN\">\n\
\n\
<HTML>\n\
<HEAD>\n\
<meta http-equiv=\"refresh\" content=\"600\">\n\
<TITLE>System Conditions - The California ISO</TITLE>\n\
<LINK REL=\"stylesheet\" TYPE=\"text/css\" HREF=\"/styles01.css\">\n\
<LINK REL=\"stylesheet\" TYPE=\"text/css\" HREF=\"http://www.caiso.com/Style%20Library/caiso/css/outlook.css\">\n\
<meta http-equiv=\"refresh\" content=\"300\">\n\
</HEAD>\n\
\n\
<BODY BGCOLOR=\"#ffffff\" TEXT=\"#000000\" LINK=\"#d96100\" VLINK=\"#666666\" TOPMARGIN=\"0\">\n\
<table width=\"100%\" border=\"0\" cellspacing=\"0\" cellpadding=\"0\">\n\
<tr>\n\
  <td valign=\"top\"><p><span class=\"to_callout1\">Current Renewables</span><br />\n\
    <span class=\"to_readings\" id=\"totalrenewables\">\n\
      6086 \n\
      MW</span><br />\n\
  </p>\n\
    <p><br />\n\
      <a href=\"http://www.caiso.com/market/Pages/ReportsBulletins/DailyRenewablesWatch.aspx\" target=\"_top\"><img src=\"http://www.caiso.com/PublishingImages/Today's%20Outlook%20Images/RenewablesWatchLogo.jpg\" width=\"150\" height=\"57\" alt=\"Renewables Watch\" border=\"0\"></a><br />\n\
      <span class=\"to_callout2\">The Renewables Watch provides actual renewable energy production within the ISO Grid.</span><br />\n\
    <a href=\"/green/renewrpt/DailyRenewablesWatch.pdf\" target=\"_blank\"><span class=\"to_about\">Click here to view yesterday's output.</span></a></p></td>\n\
  <td class=\"docdate\" valign=\"top\" align=\"right\"><img src=\"/outlook/SP/ems_renewables.gif\"><br /> <br /><img src=\"http://www.caiso.com/PublishingImages/RenewablesGraphKey.gif\"></td>\n\
</tr>  </table>\n\
</BODY>\n\
</HTML>\n\
\n\
")

    def test_request_renewable_report(self):
        c = client_factory('CAISO')
        response = c.request('http://content.caiso.com/green/renewrpt/20140312_DailyRenewablesWatch.txt')
        self.assertIn('Hourly Breakdown of Renewable Resources (MW)', response.text)

    def test_parse_ren_report_both(self):
        c = client_factory('CAISO')

        # top half
        top_df = c.parse_to_df(self.ren_report_tsv,
                               skiprows=1, nrows=24, header=0,
                               delimiter='\t+', engine='python')
        self.assertEqual(list(top_df.columns), ['Hour', 'GEOTHERMAL', 'BIOMASS', 'BIOGAS', 'SMALL HYDRO', 'WIND TOTAL', 'SOLAR PV', 'SOLAR THERMAL'])
        self.assertEqual(len(top_df), 24)

        # bottom half
        bot_df = c.parse_to_df(self.ren_report_tsv,
                               skiprows=3, nrows=24, header=0,
                               delimiter='\t+', engine='python')
        self.assertEqual(list(bot_df.columns), ['Hour', 'RENEWABLES', 'NUCLEAR', 'THERMAL', 'IMPORTS', 'HYDRO'])
        self.assertEqual(len(bot_df), 24)

    def test_parse_ren_report_bot(self):
        c = client_factory('CAISO')

        # bottom half
        bot_df = c.parse_to_df(self.ren_report_tsv,
                               skiprows=29, nrows=24, header=0,
                               delimiter='\t+', engine='python')
        self.assertEqual(list(bot_df.columns), ['Hour', 'RENEWABLES', 'NUCLEAR', 'THERMAL', 'IMPORTS', 'HYDRO'])
        self.assertEqual(len(bot_df), 24)

    def test_dt_index(self):
        c = client_factory('CAISO')
        df = c.parse_to_df(self.ren_report_tsv,
                           skiprows=1, nrows=24, header=0,
                           delimiter='\t+', engine='python')
        indexed = c.set_dt_index(df, date(2014, 3, 12), df['Hour'])
        self.assertEqual(type(indexed.index), pd.tseries.index.DatetimeIndex)
        self.assertEqual(indexed.index[0].hour, 7)

    def test_pivot(self):
        c = client_factory('CAISO')
        df = c.parse_to_df(self.ren_report_tsv,
                           skiprows=1, nrows=24, header=0,
                           delimiter='\t+', engine='python')
        indexed = c.set_dt_index(df, date(2014, 3, 12), df['Hour'])
        indexed.pop('Hour')
        pivoted = c.unpivot(indexed)

        # no rows with 'Hour'
        hour_rows = pivoted[pivoted['level_1'] == 'Hour']
        self.assertEqual(len(hour_rows), 0)

        # number of rows is from number of columns
        self.assertEqual(len(pivoted), 24*len(indexed.columns))

    def test_oasis_payload(self):
        c = client_factory('CAISO')
        c.handle_options(start_at='2014-01-01', end_at='2014-02-01',
                         market=c.MARKET_CHOICES.fivemin)
        constructed = c.construct_oasis_payload('SLD_FCST')
        expected = {'queryname': 'SLD_FCST',
                    'market_run_id': 'RTM',
                    'startdatetime': (datetime(2014, 1, 1, 8)).strftime(c.oasis_request_time_format),
                    'enddatetime': (datetime(2014, 2, 1, 8)).strftime(c.oasis_request_time_format),
                    'version': 1,
                    }
        self.assertEqual(constructed, expected)

    def test_fetch_oasis_demand_rtm(self):
        c = client_factory('CAISO')
        ts = c.utcify('2014-05-08 12:00')
        payload = {'queryname': 'SLD_FCST',
                   'market_run_id': 'RTM',
                   'startdatetime': (ts-timedelta(minutes=20)).strftime(c.oasis_request_time_format),
                   'enddatetime': (ts+timedelta(minutes=20)).strftime(c.oasis_request_time_format),
                   }
        payload.update(c.base_payload)
        data = c.fetch_oasis(payload=payload)
        self.assertEqual(len(data), 55)
        self.assertEqual(str(data[0]), '<report_data>\n\
<data_item>SYS_FCST_15MIN_MW</data_item>\n\
<resource_name>CA ISO-TAC</resource_name>\n\
<opr_date>2014-05-08</opr_date>\n\
<interval_num>50</interval_num>\n\
<interval_start_gmt>2014-05-08T19:15:00-00:00</interval_start_gmt>\n\
<interval_end_gmt>2014-05-08T19:30:00-00:00</interval_end_gmt>\n\
<value>26723</value>\n\
</report_data>')

    def test_fetch_oasis_csv(self):
        c = client_factory('CAISO')
        ts = c.utcify('2014-05-08 12:00')
        payload = {'queryname': 'SLD_FCST',
                   'market_run_id': 'RTM',
                   'startdatetime': (ts-timedelta(minutes=20)).strftime(c.oasis_request_time_format),
                   'enddatetime': (ts+timedelta(minutes=20)).strftime(c.oasis_request_time_format),
                   'resultformat': 6,
                   }
        payload.update(c.base_payload)
        data = c.fetch_oasis(payload=payload)
        self.assertEqual(len(data), 7828)
        self.assertIn(b'INTERVALSTARTTIME_GMT', data)

    def test_parse_oasis_demand_rtm(self):
        # set up list of data
        c = client_factory('CAISO')
        soup = BeautifulSoup(self.sld_fcst_xml)
        data = soup.find_all('report_data')

        # parse
        c.handle_options(market=c.MARKET_CHOICES.fivemin, freq=c.FREQUENCY_CHOICES.fivemin)
        parsed_data = c.parse_oasis_demand_forecast(data)

        # test
        self.assertEqual(len(parsed_data), 1)
        expected = {'ba_name': 'CAISO',
                    'timestamp': datetime(2014, 5, 8, 18, 55, tzinfo=pytz.utc),
                    'freq': '5m', 'market': 'RT5M',
                    'load_MW': 26755.0}
        self.assertEqual(expected, parsed_data[0])

    def test_parse_todays_outlook_renwables(self):
        # set up soup and ts
        c = client_factory('CAISO')
        soup = BeautifulSoup(self.todays_outlook_renewables)
        ts = c.utcify('2014-05-08 12:00')

        # set up options
        c.handle_options()

        # parse
        parsed_data = c.parse_todays_outlook_renewables(soup, ts)

        # test
        expected = [{'ba_name': 'CAISO',
                     'freq': '10m',
                     'fuel_name': 'renewable',
                     'gen_MW': 6086.0,
                     'market': 'RT5M',
                     'timestamp': datetime(2014, 5, 8, 19, 0, tzinfo=pytz.utc)}]
        self.assertEqual(parsed_data, expected)

    def test_fetch_oasis_demand_dam(self):
        c = client_factory('CAISO')
        ts = c.utcify('2014-05-08 12:00')
        payload = {'queryname': 'SLD_FCST',
                   'market_run_id': 'DAM',
                   'startdatetime': (ts-timedelta(minutes=20)).strftime(c.oasis_request_time_format),
                   'enddatetime': (ts+timedelta(minutes=40)).strftime(c.oasis_request_time_format),
                   }
        payload.update(c.base_payload)
        data = c.fetch_oasis(payload=payload)
        self.assertEqual(len(data), 5)
        self.assertEqual(str(data[0]), '<report_data>\n\
<data_item>SYS_FCST_DA_MW</data_item>\n\
<resource_name>CA ISO-TAC</resource_name>\n\
<opr_date>2014-05-08</opr_date>\n\
<interval_num>13</interval_num>\n\
<interval_start_gmt>2014-05-08T19:00:00-00:00</interval_start_gmt>\n\
<interval_end_gmt>2014-05-08T20:00:00-00:00</interval_end_gmt>\n\
<value>26559.38</value>\n\
</report_data>')

    def test_fetch_oasis_slrs_dam(self):
        c = client_factory('CAISO')
        ts = c.utcify('2014-05-08 12:00')
        payload = {'queryname': 'ENE_SLRS',
                   'market_run_id': 'DAM',
                   'startdatetime': (ts-timedelta(minutes=20)).strftime(c.oasis_request_time_format),
                   'enddatetime': (ts+timedelta(minutes=40)).strftime(c.oasis_request_time_format),
                   }
        payload.update(c.base_payload)
        data = c.fetch_oasis(payload=payload)
        self.assertEqual(len(data), 17)
        self.assertEqual(str(data[0]), '<report_data>\n\
<data_item>ISO_TOT_EXP_MW</data_item>\n\
<resource_name>Caiso_Totals</resource_name>\n\
<opr_date>2014-05-08</opr_date>\n\
<interval_num>13</interval_num>\n\
<interval_start_gmt>2014-05-08T19:00:00-00:00</interval_start_gmt>\n\
<interval_end_gmt>2014-05-08T20:00:00-00:00</interval_end_gmt>\n\
<value>1044</value>\n\
</report_data>')

    def test_fetch_oasis_ren_dam(self):
        c = client_factory('CAISO')
        ts = c.utcify('2014-05-08 12:00')
        payload = {'queryname': 'SLD_REN_FCST',
                   'market_run_id': 'DAM',
                   'startdatetime': (ts-timedelta(minutes=20)).strftime(c.oasis_request_time_format),
                   'enddatetime': (ts+timedelta(minutes=40)).strftime(c.oasis_request_time_format),
                   }
        payload.update(c.base_payload)
        data = c.fetch_oasis(payload=payload)
        self.assertEqual(len(data), 4)
        self.assertEqual(str(data[0]), '<report_data>\n\
<data_item>RENEW_FCST_DA_MW</data_item>\n\
<opr_date>2014-05-08</opr_date>\n\
<interval_num>13</interval_num>\n\
<interval_start_gmt>2014-05-08T19:00:00-00:00</interval_start_gmt>\n\
<interval_end_gmt>2014-05-08T20:00:00-00:00</interval_end_gmt>\n\
<value>813.7</value>\n\
<trading_hub>NP15</trading_hub>\n\
<renewable_type>Solar</renewable_type>\n\
</report_data>')

    def test_parse_oasis_slrs_gen_rtm(self):
        # set up list of data
        c = client_factory('CAISO')
        soup = BeautifulSoup(self.ene_slrs_xml)
        data = soup.find_all('report_data')

        # parse
        c.handle_options(data='gen', market=c.MARKET_CHOICES.fivemin, freq=c.FREQUENCY_CHOICES.fivemin)
        parsed_data = c.parse_oasis_slrs(data)

        # test
        self.assertEqual(len(parsed_data), 2)
        expected = {'ba_name': 'CAISO',
                    'timestamp': datetime(2013, 9, 19, 17, 0, tzinfo=pytz.utc),
                    'freq': '5m', 'market': 'RT5M', 'fuel_name': 'other',
                    'gen_MW': 23900.79}
        self.assertEqual(expected, parsed_data[0])

    def test_parse_oasis_slrs_trade_dam(self):
        # set up list of data
        c = client_factory('CAISO')
        soup = BeautifulSoup(self.ene_slrs_xml)
        data = soup.find_all('report_data')

        # parse
        c.handle_options(data='trade', market=c.MARKET_CHOICES.dam, freq=c.FREQUENCY_CHOICES.dam)
        parsed_data = c.parse_oasis_slrs(data)

        # test
        self.assertEqual(len(parsed_data), 3)
        expected = {'ba_name': 'CAISO',
                    'timestamp': datetime(2013, 9, 19, 7, 0, tzinfo=pytz.utc),
                    'freq': '1hr', 'market': 'DAHR',
                    'net_exp_MW': -5014.0}
        self.assertEqual(expected, parsed_data[0])

    def test_parse_oasis_renewables_dam(self):
        # set up list of data
        c = client_factory('CAISO')
        soup = BeautifulSoup(self.sld_ren_fcst_xml)
        data = soup.find_all('report_data')

        # parse
        c.handle_options(data='gen', market=c.MARKET_CHOICES.dam, freq=c.FREQUENCY_CHOICES.dam)
        parsed_data = c.parse_oasis_renewable(data)

        # test
        self.assertEqual(len(parsed_data), 6)
        expected = {'ba_name': 'CAISO',
                    'timestamp': datetime(2013, 9, 20, 6, 0, tzinfo=pytz.utc),
                    'freq': '1hr', 'market': 'DAHR', 'fuel_name': 'wind',
                    'gen_MW': 580.83}
        self.assertEqual(expected, parsed_data[0])

    def test_get_lmp_dataframe_latest(self):
        c = client_factory('CAISO')
        ts = pytz.utc.localize(datetime.utcnow())
        lmp = c.get_lmp_as_dataframe('SLAP_PGP2-APND')
        self.assertEqual(len(lmp), 1)

        self.assertGreaterEqual(lmp.iloc[0]['LMP_PRC'], -300)
        self.assertLessEqual(lmp.iloc[0]['LMP_PRC'], 1500)

        # lmp is a dataframe, lmp.iloc[0] is a Series, Series.name is the index of that entry
        self.assertGreater(lmp.iloc[0].name, ts - timedelta(minutes=5))
        self.assertLess(lmp.iloc[0].name, ts + timedelta(minutes=5))

    def test_get_lmp_dataframe_hist(self):
        c = client_factory('CAISO')
        ts = pytz.utc.localize(datetime(2015, 3, 1, 12))
        start = ts - timedelta(hours=2)
        lmps = c.get_lmp_as_dataframe('SLAP_PGP2-APND', latest=False, start_at=start, end_at=ts)
        self.assertEqual(len(lmps), 24)

        self.assertGreaterEqual(lmps['LMP_PRC'].max(), 0)
        self.assertLess(lmps['LMP_PRC'].max(), 1500)
        self.assertGreaterEqual(lmps['LMP_PRC'].min(), -300)

        self.assertGreaterEqual(lmps.index.to_pydatetime().min(), start)
        self.assertLessEqual(lmps.index.to_pydatetime().max(), ts)

    def test_get_lmp_dataframe_badnode(self):
        c = client_factory('CAISO')
        df = c.get_lmp_as_dataframe('badnode')
        self.assertTrue(df.empty)

    def test_get_AS_dataframe(self):
        c = client_factory('CAISO')
        ts = datetime(2015, 3, 1, 11, 0, 0, tzinfo=pytz.utc)
        start = ts - timedelta(days=2)

        as_prc = c.get_AS_dataframe(node_id='AS_CAISO_EXP', start_at=start, end_at=ts,
                                    market_run_id='DAM')

        self.assertEqual(len(as_prc), 288)
        self.assertAlmostEqual(as_prc['MW'].mean(), 1.528506944444443)

        grouped = as_prc.groupby('XML_DATA_ITEM')
        self.assertEqual(len(grouped), 6)
        means = {
            'SP_CLR_PRC': 1.685417,
            'RU_CLR_PRC': 3.074583,
            'RMU_CLR_PRC': 5.729167e-02,
            'RMD_CLR_PRC': 3.620833e-01,
            'RD_CLR_PRC': 3.901667,
            'NS_CLR_PRC': 9.000000e-02,
        }

        for group in means:
            self.assertAlmostEqual(grouped.get_group(group)['MW'].mean(), means[group], places=6)
            self.assertEqual(len(grouped.get_group(group)), 48)

    def test_get_AS_dataframe_empty(self):
        c = client_factory('CAISO')
        st = pytz.utc.localize(datetime.now() + timedelta(days=2))
        et = st + timedelta(days=1)
        as_prc = c.get_AS_dataframe('AS_CAISO_EXP', start_at=st, end_at=et,
                                    market_run_id='DAM', anc_type='RU')
        self.assertTrue(as_prc.empty)

    def test_get_AS_dataframe_latest(self):
        c = client_factory('CAISO')
        as_prc = c.get_AS_dataframe('AS_CAISO_EXP')

        # Could be 1 or 2 prices in last 61 minutes
        self.assertLessEqual(len(as_prc), 12)
        self.assertGreaterEqual(len(as_prc), 6)

    def test_get_ancillary_services(self):
        c = client_factory('CAISO')
        ts = datetime(2015, 3, 1, 11, 0, 0, tzinfo=pytz.utc)
        start = ts - timedelta(days=2)

        as_prc = c.get_ancillary_services('AS_CAISO_EXP', start_at=start, end_at=ts,
                                          market_run_id='DAM')

        self.assertEqual(len(as_prc), 48)
        self.assertGreaterEqual(min([i['timestamp'] for i in as_prc]),
                                start - timedelta(minutes=5))
        self.assertLessEqual(max([i['timestamp'] for i in as_prc]),
                             ts + timedelta(minutes=5))

        means = {
            'SR': 1.685417,
            'RU': 3.074583,
            'RMU': 5.729167e-02,
            'RMD': 3.620833e-01,
            'RD': 3.901667,
            'NR': 9.000000e-02,
        }

        for anc_type in means:
            dp = [i[anc_type] for i in as_prc]
            self.assertAlmostEqual(numpy.mean(dp), means[anc_type], places=6)

    def test_get_ancillary_services_RU(self):
        c = client_factory('CAISO')
        ts = datetime(2015, 3, 1, 11, 0, 0, tzinfo=pytz.utc)
        start = ts - timedelta(days=2)

        as_prc = c.get_ancillary_services('AS_CAISO_EXP', start_at=start, end_at=ts,
                                          market_run_id='DAM', anc_type='RU')

        self.assertEqual(len(as_prc), 48)
        self.assertGreaterEqual(min([i['timestamp'] for i in as_prc]),
                                start - timedelta(minutes=5))
        self.assertLessEqual(max([i['timestamp'] for i in as_prc]),
                             ts + timedelta(minutes=5))

        self.assertAlmostEqual(numpy.mean([i['RU'] for i in as_prc]), 3.074583, places=6)

    def test_get_AS_empty(self):
        """No AS data available 2 days in future"""
        c = client_factory('CAISO')
        st = pytz.utc.localize(datetime.now() + timedelta(days=2))
        et = st + timedelta(days=1)
        as_prc = c.get_ancillary_services(node_id='AS_CAISO_EXP', start_at=st, end_at=et,
                                          market_run_id='DAM', anc_type='RU')
        self.assertEqual(as_prc, {})

    @expectedFailure
    def test_lmp_loc(self):
        c = client_factory('CAISO')
        loc_data = c.get_lmp_loc()

        # one entry for each node
        self.assertGreaterEqual(len(loc_data), 4228)

        # check keys
        self.assertItemsEqual(loc_data[0].keys(),
                              ['node_id', 'latitude', 'longitude', 'area'])

    @mock.patch('pyiso.caiso.CAISOClient.request')
    def test_bad_data(self, mock_request):
        mock_request.return_value = requests.get('https://httpbin.org/')

        c = client_factory('CAISO')
        ts = pytz.utc.localize(datetime(2015, 3, 1, 12))
        start = ts - timedelta(hours=2)
        df = c.get_lmp_as_dataframe('CAISO_AS', latest=False, start_at=start, end_at=ts)

        self.assertIsInstance(df, pd.DataFrame)

    @mock.patch('pyiso.caiso.CAISOClient.request')
    def test_bad_data_lmp_only(self, mock_request):
        mock_request.return_value = requests.get('https://httpbin.org/')

        c = client_factory('CAISO')
        ts = pytz.utc.localize(datetime(2015, 3, 1, 12))
        start = ts - timedelta(hours=2)
        df = c.get_lmp_as_dataframe('CAISO_AS', latest=False, start_at=start, end_at=ts,
                                    lmp_only=False)
        self.assertIsInstance(df, pd.DataFrame)
