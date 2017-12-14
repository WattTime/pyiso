from datetime import timedelta, datetime
from unittest import TestCase

import numpy
import pytz

from pyiso import client_factory


class IntegrationTestCAISOClient(TestCase):
    def setUp(self):
        self.c = client_factory('CAISO')

    def test_request_renewable_report(self):
        c = client_factory('CAISO')
        response = self.c.request('http://content.caiso.com/green/renewrpt/20140312_DailyRenewablesWatch.txt')
        self.assertIn('Hourly Breakdown of Renewable Resources (MW)', response.text)

    def test_fetch_oasis_csv(self):
        ts = self.c.utcify('2014-05-08 12:00')
        payload = {'queryname': 'SLD_FCST',
                   'market_run_id': 'RTM',
                   'startdatetime': (ts-timedelta(minutes=20)).strftime(self.c.oasis_request_time_format),
                   'enddatetime': (ts+timedelta(minutes=20)).strftime(self.c.oasis_request_time_format),
                   'resultformat': 6,
                   }
        payload.update(self.c.base_payload)
        data = self.c.fetch_oasis(payload=payload)
        self.assertEqual(len(data), 7828)
        self.assertIn(b'INTERVALSTARTTIME_GMT', data)

    def test_fetch_oasis_demand_dam(self):
        ts = self.c.utcify('2014-05-08 12:00')
        payload = {'queryname': 'SLD_FCST',
                   'market_run_id': 'DAM',
                   'startdatetime': (ts-timedelta(minutes=20)).strftime(self.c.oasis_request_time_format),
                   'enddatetime': (ts+timedelta(minutes=40)).strftime(self.c.oasis_request_time_format),
                   }
        payload.update(self.c.base_payload)
        data = self.c.fetch_oasis(payload=payload)
        self.assertEqual(len(data), 5)
        self.assertEqual(str(data[0]).lower(), '<report_data>\n\
<data_item>SYS_FCST_DA_MW</data_item>\n\
<resource_name>CA ISO-TAC</resource_name>\n\
<opr_date>2014-05-08</opr_date>\n\
<interval_num>13</interval_num>\n\
<interval_start_gmt>2014-05-08T19:00:00-00:00</interval_start_gmt>\n\
<interval_end_gmt>2014-05-08T20:00:00-00:00</interval_end_gmt>\n\
<value>26559.38</value>\n\
</report_data>'.lower())

    def test_fetch_oasis_demand_rtm(self):
        ts = self.c.utcify('2014-05-08 12:00')
        payload = {'queryname': 'SLD_FCST',
                   'market_run_id': 'RTM',
                   'startdatetime': (ts-timedelta(minutes=20)).strftime(self.c.oasis_request_time_format),
                   'enddatetime': (ts+timedelta(minutes=20)).strftime(self.c.oasis_request_time_format),
                   }
        payload.update(self.c.base_payload)
        data = self.c.fetch_oasis(payload=payload)
        self.assertEqual(len(data), 55)
        self.assertEqual(str(data[0]).lower(), '<report_data>\n\
<data_item>sys_fcst_15min_mw</data_item>\n\
<resource_name>ca iso-tac</resource_name>\n\
<opr_date>2014-05-08</opr_date>\n\
<interval_num>49</interval_num>\n\
<interval_start_gmt>2014-05-08t19:00:00-00:00</interval_start_gmt>\n\
<interval_end_gmt>2014-05-08t19:15:00-00:00</interval_end_gmt>\n\
<value>26731</value>\n\
</report_data>'.lower())

    def test_fetch_oasis_ren_dam(self):
        ts = self.c.utcify('2014-05-08 12:00')
        payload = {'queryname': 'SLD_REN_FCST',
                   'market_run_id': 'DAM',
                   'startdatetime': (ts-timedelta(minutes=20)).strftime(self.c.oasis_request_time_format),
                   'enddatetime': (ts+timedelta(minutes=40)).strftime(self.c.oasis_request_time_format),
                   }
        payload.update(self.c.base_payload)
        data = self.c.fetch_oasis(payload=payload)
        self.assertEqual(len(data), 4)
        self.assertEqual(str(data[0]).lower(), '<report_data>\n\
<data_item>RENEW_FCST_DA_MW</data_item>\n\
<opr_date>2014-05-08</opr_date>\n\
<interval_num>13</interval_num>\n\
<interval_start_gmt>2014-05-08T19:00:00-00:00</interval_start_gmt>\n\
<interval_end_gmt>2014-05-08T20:00:00-00:00</interval_end_gmt>\n\
<value>813.7</value>\n\
<trading_hub>NP15</trading_hub>\n\
<renewable_type>Solar</renewable_type>\n\
</report_data>'.lower())

    def test_fetch_oasis_slrs_dam(self):
        ts = self.c.utcify('2014-05-08 12:00')
        payload = {'queryname': 'ENE_SLRS',
                   'market_run_id': 'DAM',
                   'startdatetime': (ts-timedelta(minutes=20)).strftime(self.c.oasis_request_time_format),
                   'enddatetime': (ts+timedelta(minutes=40)).strftime(self.c.oasis_request_time_format),
                   }
        payload.update(self.c.base_payload)
        data = self.c.fetch_oasis(payload=payload)
        self.assertEqual(len(data), 17)
        self.assertEqual(str(data[0]).lower(), '<report_data>\n\
<data_item>ISO_TOT_EXP_MW</data_item>\n\
<resource_name>Caiso_Totals</resource_name>\n\
<opr_date>2014-05-08</opr_date>\n\
<interval_num>13</interval_num>\n\
<interval_start_gmt>2014-05-08T19:00:00-00:00</interval_start_gmt>\n\
<interval_end_gmt>2014-05-08T20:00:00-00:00</interval_end_gmt>\n\
<value>1044</value>\n\
</report_data>'.lower())
