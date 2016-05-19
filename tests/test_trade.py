from pyiso import client_factory
from pyiso.base import BaseClient
from unittest import TestCase
import pytz
from datetime import datetime, timedelta
import requests_mock
from responses import test_trade_responses as responses

class TestBaseTrade(TestCase):
    def setUp(self):
        # set up expected values from base client
        bc = BaseClient()
        self.MARKET_CHOICES = bc.MARKET_CHOICES
        self.FREQUENCY_CHOICES = bc.FREQUENCY_CHOICES

        # set up other expected values
        self.BA_CHOICES = ['ISONE', 'MISO', 'SPP',
                           'BPA', 'CAISO', 'ERCOT',
                           'PJM', 'NYISO', 'NEVP', 'SPPC']

    def _run_net_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)

        # get data
        data = c.get_trade(**kwargs)

        # test number
        self.assertGreaterEqual(len(data), 1)

        # test contents
        for dp in data:
            # test key names
            for key in ['ba_name', 'timestamp', 'freq', 'market']:
                self.assertIn(key, dp.keys())
            self.assertEqual(len(dp.keys()), 5)

            # test values
            self.assertEqual(dp['timestamp'].tzinfo, pytz.utc)
            self.assertIn(dp['ba_name'], self.BA_CHOICES)

            # test for numeric value
            self.assertGreaterEqual(dp['net_exp_MW']+1, dp['net_exp_MW'])

            # test correct temporal relationship to now
            if c.options['forecast']:
                self.assertGreaterEqual(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))
            else:
                self.assertLess(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))

        # return
        return data

    def _run_pairwise_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)

        # get data
        data = c.get_trade(**kwargs)

        # test number
        self.assertGreaterEqual(len(data), 1)

        # test contents
        for dp in data:
            # test key names
            for key in ['source_ba_name', 'dest_ba_name', 'timestamp', 'freq', 'market']:
                self.assertIn(key, dp.keys())
            self.assertEqual(len(dp.keys()), 6)

            # test values
            self.assertEqual(dp['timestamp'].tzinfo, pytz.utc)
            self.assertIn(dp['source_ba_name'], self.BA_CHOICES)

            # test for numeric value
            self.assertGreaterEqual(dp['export_MW']+1, dp['export_MW'])

            # test correct temporal relationship to now
            if c.options['forecast']:
                self.assertGreaterEqual(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))
            else:
                self.assertLess(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))

        # return
        return data

    def _run_notimplemented_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)

        # method not implemented yet
        self.assertRaises(NotImplementedError, c.get_trade)

    def _run_failing_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)

        # method not implemented yet
        self.assertRaises(ValueError, c.get_trade)


class TestBPATrade(TestBaseTrade):
    def test_failing(self):
        self._run_notimplemented_test('BPA')


class TestCAISOTrade(TestBaseTrade):
    def test_latest(self):
        # basic test
        data = self._run_net_test('CAISO', latest=True, market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_date_range(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_net_test('CAISO', start_at=today-timedelta(days=2),
                                  end_at=today-timedelta(days=1))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_forecast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_net_test('CAISO', start_at=today+timedelta(hours=10),
                                  end_at=today+timedelta(days=2))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.dam)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)


class TestNYISOTrade(TestBaseTrade):
    def test_latest(self):
        # basic test
        data = self._run_net_test('NYISO', latest=True, market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    @requests_mock.mock()
    def test_date_range(self, mocker):
        url = ('http://mis.nyiso.com/public/csv/ExternalLimitsFlows'
               '/20160513ExternalLimitsFlows.csv')
        mocker.get(url, text=responses.test_date_range_short[1])

        mocker.get(url.replace('13', '11'),
                   text=responses.test_date_range_short[1].replace('05/13', '05/11'))

        mocker.get(url.replace('13', '12'),
                   text=responses.test_date_range_short[1].replace('05/13', '05/12'))

        # basic test
        now = responses.test_date_range_short[0]
        # basic test
        data = self._run_net_test('NYISO', start_at=now-timedelta(days=2),
                                  end_at=now-timedelta(days=1))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    @requests_mock.mock()
    def test_date_range_short(self, mocker):
        url = ('http://mis.nyiso.com/public/csv/ExternalLimitsFlows'
               '/20160513ExternalLimitsFlows.csv')
        mocker.get(url, text=responses.test_date_range_short[1])
        # basic test
        now = responses.test_date_range_short[0]
        data = self._run_net_test('NYISO', start_at=now-timedelta(minutes=10),
                                  end_at=now-timedelta(minutes=5))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_date_range_future(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        self._run_failing_test('NYISO', start_at=today+timedelta(days=1),
                               end_at=today+timedelta(days=2))


class TestERCOTTrade(TestBaseTrade):
    def test_failing(self):
        self._run_notimplemented_test('ERCOT')


class TestISONETrade(TestBaseTrade):
    def test_failing(self):
        self._run_notimplemented_test('ISONE')


class TestMISOTrade(TestBaseTrade):
    def test_forecast(self):
        # basic test
        today = pytz.utc.localize(datetime.utcnow())
        data = self._run_net_test('MISO', start_at=today+timedelta(hours=2),
                                  end_at=today+timedelta(days=2))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.dam)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)


class TestNEVPTrade(TestBaseTrade):
    def test_latest(self):
        # basic test
        data = self._run_pairwise_test('NEVP', latest=True, market=self.MARKET_CHOICES.hourly)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_date_range(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_pairwise_test('NEVP', start_at=today-timedelta(days=2),
                                       end_at=today-timedelta(days=1))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_forecast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        self._run_failing_test('NEVP', start_at=today+timedelta(hours=10),
                               end_at=today+timedelta(days=2))


class TestPJMTrade(TestBaseTrade):
    def test_latest(self):
        # basic test
        data = self._run_net_test('PJM', latest=True, market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)


class TestSPPTrade(TestBaseTrade):
    def test_failing(self):
        self._run_notimplemented_test('SPP')


class TestSPPCTrade(TestBaseTrade):
    def test_latest(self):
        # basic test
        data = self._run_pairwise_test('SPPC', latest=True, market=self.MARKET_CHOICES.hourly)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_date_range(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_pairwise_test('SPPC', start_at=today-timedelta(days=2),
                                       end_at=today-timedelta(days=1))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_forecast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        self._run_failing_test('SPPC', start_at=today+timedelta(hours=10),
                               end_at=today+timedelta(days=2))
