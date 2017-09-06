import os
from pyiso import client_factory
from unittest import TestCase
from datetime import datetime, timedelta
try:
    from urllib2 import HTTPError
except ImportError:
    from urllib.error import HTTPError
import pytz
import mock

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), '../fixtures/nvenergy')


class TestNVEnergy(TestCase):
    def setUp(self):
        self.c = client_factory('NEVP')

        self.one_day_response = open(FIXTURES_DIR + '/native_system_load_and_ties_for_08_02_2015_.html').read().encode('utf8')
        self.tomorrow_response = open(FIXTURES_DIR + '/tomorrow.htm').read().encode('utf8')
        self.one_month_response = open(FIXTURES_DIR + '/Monthly_Ties_and_Loads_L_from_07_01_2015_to_07_31_2015_.html').read().encode('utf8')

        self.today = datetime(2015, 8, 2, 12, 34)
        self.tomorrow = datetime(2015, 8, 3, 12, 34)
        self.last_month = datetime(2015, 7, 2, 12, 34)
        self.now = pytz.utc.localize(datetime.utcnow())

    def test_idx2ts(self):
        # hour 01 is midnight local
        expected_local_01 = pytz.timezone('US/Pacific').localize(datetime(2015, 8, 2, 0))
        self.assertEqual(self.c.idx2ts(self.today, '01'),
                         expected_local_01.astimezone(pytz.utc))

        # hour 24 is 11pm local
        expected_local_24 = pytz.timezone('US/Pacific').localize(datetime(2015, 8, 2, 23))
        self.assertEqual(self.c.idx2ts(self.today, '24'),
                         expected_local_24.astimezone(pytz.utc))

        # bad hour errors
        self.assertRaises(ValueError, self.c.idx2ts, self.today, 'nothour')

    def test_data_url_future(self):
        # no data after tomorrow
        self.assertRaises(ValueError, self.c.data_url, self.now+timedelta(days=2))

    def test_data_url_tomorrow(self):
        url, mode = self.c.data_url(self.now+timedelta(days=1))
        self.assertEqual(url, self.c.BASE_URL+'tomorrow.htm')
        self.assertEqual(mode, 'tomorrow')

    def test_data_url_today(self):
        url, mode = self.c.data_url(self.now)
        self.assertEqual(url, self.c.BASE_URL+'native_system_load_and_ties_for_%02d_%02d_%04d_.html' % (self.now.month, self.now.day, self.now.year))
        self.assertEqual(mode, 'recent')

    def test_data_url_historical(self):
        last_month_date = self.now-timedelta(days=35)
        url, mode = self.c.data_url(last_month_date)
        self.assertIn(self.c.BASE_URL, url)
        self.assertIn('Monthly_Ties_and_Loads_L_from', url)
        self.assertIn(last_month_date.strftime('%m_01_%Y'), url)
        self.assertEqual(mode, 'historical')

    def test_fetch_df_today(self):
        with mock.patch.object(self.c, 'request') as mocker:
            mocker.return_value = mock.Mock(status_code=200, content=self.one_day_response)

            df, mode = self.c.fetch_df(self.today, url='http://mockurl', mode='recent')

            # test index and columns
            self.assertEqual(list(df.index),
                             ['Actual Native Load', 'Actual System Load',
                              'Forecast Native Load', 'Forecast System Load',
                              'Tie Line', 'Tie Line', 'Tie Line', 'Tie Line',
                              'Tie Line', 'Tie Line', 'Tie Line'])
            self.assertEqual(list(df.columns),
                             ['Counterparty'] + list(range(1, 25)) + ['Total'])

    def test_fetch_df_bad(self):
        # no data in year 2020
        data, mode = self.c.fetch_df(self.today, self.c.BASE_URL + 'native_system_load_and_ties_for_01_01_2020_.html')
        self.assertEqual(len(data), 0)
        self.assertEqual(mode, 'error')

    def test_parse_load_today(self):
        with mock.patch.object(self.c, 'request') as mocker:
            mocker.return_value = mock.Mock(status_code=200, content=self.one_day_response)
            df, mode = self.c.fetch_df(self.today, url='http://mockurl')

            # set up options
            self.c.handle_options(latest=True)
            data = self.c.parse_load(df, self.today)

            # test
            self.assertEqual(len(data), 18)
        for idp, dp in enumerate(data):
            self.assertEqual(dp['market'], 'RTHR')
            self.assertEqual(dp['freq'], '1hr')
            self.assertEqual(dp['ba_name'], 'NEVP')
            self.assertEqual(dp['load_MW'], df.ix['Actual System Load', idp+1])

    def test_parse_load_tomorrow(self):
        with mock.patch.object(self.c, 'request') as mocker:
            mocker.return_value = mock.Mock(status_code=200, content=self.tomorrow_response)
            df, mode = self.c.fetch_df(self.tomorrow, 'http://mockurl', 'tomorrow')

            # set up options
            self.c.handle_options(start_at=self.today, end_at=self.tomorrow+timedelta(days=1))
            data = self.c.parse_load(df, self.tomorrow, 'tomorrow')

            # test
            self.assertEqual(len(data), 24)
            for idp, dp in enumerate(data):
                self.assertEqual(dp['market'], 'RTHR')
                self.assertEqual(dp['freq'], '1hr')
                self.assertEqual(dp['ba_name'], 'NEVP')
                self.assertEqual(dp['load_MW'], df.ix['Forecast System Load', idp+1])

    def test_parse_load_last_month(self):
        with mock.patch.object(self.c, 'request') as mocker:
            mocker.return_value = mock.Mock(status_code=200, content=self.one_month_response)
            df, mode = self.c.fetch_df(self.last_month, 'http://mockurl', 'historical')

            # set up options
            self.c.handle_options(start_at=self.last_month,
                                  end_at=self.last_month+timedelta(days=2))
            data = self.c.parse_load(df, self.last_month)

            # test
            self.assertEqual(len(data), 18)
            for idp, dp in enumerate(data):
                self.assertEqual(dp['market'], 'RTHR')
                self.assertEqual(dp['freq'], '1hr')
                self.assertEqual(dp['ba_name'], 'NEVP')
                self.assertEqual(dp['load_MW'], df.ix['Actual System Load', idp+1])

    def test_parse_trade_today(self):
        with mock.patch.object(self.c, 'request') as mocker:
            mocker.return_value = mock.Mock(status_code=200, content=self.one_day_response)
            df, mode = self.c.fetch_df(self.today, url='http://mockurl')

            # set up options
            self.c.handle_options(latest=True)
            data = self.c.parse_trade(df, self.today)

            # test
            self.assertEqual(len(data), 18*len(self.c.TRADE_BAS))
            for idp, dp in enumerate(data):
                self.assertEqual(dp['market'], 'RTHR')
                self.assertEqual(dp['freq'], '1hr')
                self.assertIn(dp['dest_ba_name'], self.c.TRADE_BAS.values())

                dest = [k for k, v in self.c.TRADE_BAS.items() if v == dp['dest_ba_name']][0]
                idx = idp % 18 + 1
                self.assertEqual(dp['export_MW'], df.ix[dest, idx])

    def test_parse_trade_tomorrow(self):
        with mock.patch.object(self.c, 'request') as mocker:
            mocker.return_value = mock.Mock(status_code=200, content=self.tomorrow_response)
            df, mode = self.c.fetch_df(self.tomorrow, 'http://mockurl', 'tomorrow')

            # set up options
            self.c.handle_options(start_at=self.today, end_at=self.tomorrow+timedelta(days=1))

            # no trade data tomorrow
            self.assertRaises(KeyError, self.c.parse_trade, df, self.tomorrow, 'tomorrow')

    def test_parse_trade_last_month(self):
        with mock.patch.object(self.c, 'request') as mocker:
            mocker.return_value = mock.Mock(status_code=200, content=self.one_month_response)
            df, mode = self.c.fetch_df(self.last_month, 'http://mockurl', 'historical')

            # set up options
            self.c.handle_options(start_at=self.last_month, end_at=self.last_month+timedelta(days=2))
            data = self.c.parse_trade(df, self.last_month)

            # test
            self.assertEqual(len(data), 18*len(self.c.TRADE_BAS))
            for idp, dp in enumerate(data):
                self.assertEqual(dp['market'], 'RTHR')
                self.assertEqual(dp['freq'], '1hr')
                self.assertIn(dp['dest_ba_name'], self.c.TRADE_BAS.values())

                dest = [k for k, v in self.c.TRADE_BAS.items() if v == dp['dest_ba_name']][0]
                idx = idp % 18 + 1
                self.assertEqual(dp['export_MW'], df.ix[dest, idx])

    def test_time_subset_latest(self):
        """Subset should return all elements with latest ts"""
        self.c.handle_options(latest=True)
        data = [
            {'timestamp': datetime(2015, 8, 13), 'value': 1},
            {'timestamp': datetime(2015, 8, 12), 'value': 2},
            {'timestamp': datetime(2015, 8, 13), 'value': 3},
        ]
        subs = self.c.time_subset(data)
        self.assertEqual(len(subs), 2)
        self.assertIn({'timestamp': datetime(2015, 8, 13), 'value': 1}, subs)
        self.assertIn({'timestamp': datetime(2015, 8, 13), 'value': 3}, subs)

    def test_time_subset_range(self):
        """Subset should return all elements with ts in range, inclusive"""
        self.c.handle_options(start_at=pytz.utc.localize(datetime(2015, 8, 11)),
                              end_at=pytz.utc.localize(datetime(2015, 8, 13)))
        data = [
            {'timestamp': pytz.utc.localize(datetime(2015, 8, 13)), 'value': 1},
            {'timestamp': pytz.utc.localize(datetime(2015, 8, 10)), 'value': 2},
            {'timestamp': pytz.utc.localize(datetime(2015, 8, 15)), 'value': 3},
            {'timestamp': pytz.utc.localize(datetime(2015, 8, 11)), 'value': 4},
            {'timestamp': pytz.utc.localize(datetime(2015, 8, 12)), 'value': 5},
        ]
        subs = self.c.time_subset(data)
        self.assertEqual(len(subs), 3)
        self.assertIn({'timestamp': pytz.utc.localize(datetime(2015, 8, 13)), 'value': 1}, subs)
        self.assertIn({'timestamp': pytz.utc.localize(datetime(2015, 8, 11)), 'value': 4}, subs)
        self.assertIn({'timestamp': pytz.utc.localize(datetime(2015, 8, 12)), 'value': 5}, subs)

    def test_time_subset_accepts_no_latest_data(self):
        self.c.handle_options(latest=True)
        self.assertEqual(self.c.time_subset([]), [])
