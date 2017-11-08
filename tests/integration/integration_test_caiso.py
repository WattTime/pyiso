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

    def test_get_AS_dataframe(self):
        ts = datetime(2015, 3, 1, 11, 0, 0, tzinfo=pytz.utc)
        start = ts - timedelta(days=2)

        as_prc = self.c.get_AS_dataframe(node_id='AS_CAISO_EXP', start_at=start, end_at=ts,
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
        st = pytz.utc.localize(datetime.now() + timedelta(days=2))
        et = st + timedelta(days=1)
        as_prc = self.c.get_AS_dataframe('AS_CAISO_EXP', start_at=st, end_at=et,
                                         market_run_id='DAM', anc_type='RU')
        self.assertTrue(as_prc.empty)

    def test_get_AS_dataframe_latest(self):
        as_prc = self.c.get_AS_dataframe('AS_CAISO_EXP')

        # Could be 1 or 2 prices in last 61 minutes
        self.assertLessEqual(len(as_prc), 12)
        self.assertGreaterEqual(len(as_prc), 6)

    def test_get_AS_empty(self):
        """No AS data available 2 days in future"""
        st = pytz.utc.localize(datetime.now() + timedelta(days=2))
        et = st + timedelta(days=1)
        as_prc = self.c.get_ancillary_services(node_id='AS_CAISO_EXP', start_at=st, end_at=et,
                                               market_run_id='DAM', anc_type='RU')
        self.assertEqual(as_prc, {})

    def test_get_ancillary_services(self):
        ts = datetime(2015, 3, 1, 11, 0, 0, tzinfo=pytz.utc)
        start = ts - timedelta(days=2)

        as_prc = self.c.get_ancillary_services('AS_CAISO_EXP', start_at=start, end_at=ts,
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
        ts = datetime(2015, 3, 1, 11, 0, 0, tzinfo=pytz.utc)
        start = ts - timedelta(days=2)

        as_prc = self.c.get_ancillary_services('AS_CAISO_EXP', start_at=start, end_at=ts,
                                               market_run_id='DAM', anc_type='RU')

        self.assertEqual(len(as_prc), 48)
        self.assertGreaterEqual(min([i['timestamp'] for i in as_prc]),
                                start - timedelta(minutes=5))
        self.assertLessEqual(max([i['timestamp'] for i in as_prc]),
                             ts + timedelta(minutes=5))

        self.assertAlmostEqual(numpy.mean([i['RU'] for i in as_prc]), 3.074583, places=6)

    def test_get_lmp_dataframe_latest(self):
        ts = pytz.utc.localize(datetime.utcnow())
        lmp = self.c.get_lmp_as_dataframe('SLAP_PGP2-APND')
        lmp = self.c._standardize_lmp_dataframe(lmp)
        self.assertEqual(len(lmp), 1)

        self.assertGreaterEqual(lmp.iloc[0]['lmp'], -300)
        self.assertLessEqual(lmp.iloc[0]['lmp'], 1500)

        # lmp is a dataframe, lmp.iloc[0] is a Series, Series.name is the index of that entry
        self.assertGreater(lmp.iloc[0].name, ts - timedelta(minutes=5))
        self.assertLess(lmp.iloc[0].name, ts + timedelta(minutes=5))

    def test_get_lmp_dataframe_hist(self):
        ts = pytz.utc.localize(datetime(2015, 3, 1, 12))
        start = ts - timedelta(hours=2)
        lmps = self.c.get_lmp_as_dataframe('SLAP_PGP2-APND', latest=False, start_at=start, end_at=ts)
        lmps = self.c._standardize_lmp_dataframe(lmps)
        self.assertEqual(len(lmps), 24)

        self.assertGreaterEqual(lmps['lmp'].max(), 0)
        self.assertLess(lmps['lmp'].max(), 1500)
        self.assertGreaterEqual(lmps['lmp'].min(), -300)

        self.assertGreaterEqual(lmps.index.to_pydatetime().min(), start)
        self.assertLessEqual(lmps.index.to_pydatetime().max(), ts)

    def test_get_lmp_dataframe_fifteen(self):
        ts = pytz.utc.localize(datetime(2016, 10, 1, 12))
        start = ts - timedelta(hours=2)
        lmps = self.c.get_lmp_as_dataframe('SLAP_PGP2-APND', market='RTPD', market_run_id='RTPD', latest=False,
                                           start_at=start, end_at=ts)
        lmps = self.c._standardize_lmp_dataframe(lmps)

        self.assertEqual(len(lmps), 8)
        self.assertGreaterEqual(lmps['lmp'].max(), 0)
        self.assertLess(lmps['lmp'].max(), 1500)
        self.assertGreaterEqual(lmps['lmp'].min(), -300)

        self.assertGreaterEqual(lmps.index.to_pydatetime().min(), start)
        self.assertLessEqual(lmps.index.to_pydatetime().max(), ts)

    def test_get_lmp_dataframe_badnode(self):
        c = client_factory('CAISO')
        df = c.get_lmp_as_dataframe('badnode')
        self.assertTrue(df.empty)
