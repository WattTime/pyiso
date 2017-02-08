from pyiso import client_factory, BALANCING_AUTHORITIES
from pyiso.base import BaseClient
from unittest import TestCase
import pytz
from datetime import datetime, timedelta
import mock
from responses import test_trade_responses as responses
import random
import time


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

    def _run_null_repsonse_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)

        # mock request
        with mock.patch.object(c, 'request') as mock_request:
            mock_request.return_value = None

            # get data
            data = c.get_trade(**kwargs)

            # test
            self.assertEqual(data, [])

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

    @mock.patch.object(BaseClient, 'request')
    def test_date_range(self, mocker):
        def mreq(url):
            day = url[58:60]
            text = responses.test_date_range_short[1].replace('05/13', '05/' + day)
            return mock.Mock(status_code=200, text=text)

#         url = ('http://mis.nyiso.com/publ/ExternalLimitsFlows'
#                '/20160513ExternalLimitsFlows.csv')
        mocker.side_effect = mreq
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

    @mock.patch.object(BaseClient, 'request')
    def test_date_range_short(self, mocker):
        mocker.return_value = mock.Mock(status_code=200,
                                        text=responses.test_date_range_short[1])
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

    # start here:
    # would be nice to get rid of the random sub samples- but for that need
    # to seriously throttle things to get through all the BAs!
    # also, need to get to the bottom of throttling- read the EIA docs more.
    # maybe pull one set of data in the setup and then run all the tests on
    # that.

    # start here
    # see if removing non-US BAs solves the throttling issues so we can pull
    # the random and time stuff stuff

    # Need to fix retry/throttling issues.
    # builtin retrying would be better than random + timeouts- replace!

    # Then pull/merge redundant tests in test_eia. Just keep the corner cases?


class TestEIATrade(TestBaseTrade):

    def setUp(self):
        super(TestEIATrade, self).setUp()
        self.BA_CHOICES = [i for i in BALANCING_AUTHORITIES.keys() if BALANCING_AUTHORITIES[i]["class"] == "EIACLIENT"]
        self.delay_bas = ['AEC', 'DOPD', 'GVL', 'HST', 'NSB', 'PGE',
                          'SCL', 'TAL', 'TIDC', 'TPWR']
        self.no_delay_bas = [i for i in self.BA_CHOICES if i not in self.delay_bas]

        self.random_delay_ba = random.sample(self.delay_bas, 1)[0]
        self.random_no_delay_ba = random.sample(self.no_delay_bas, 1)[0]
        #
        # self.delay_mock = self._run_net_test(self.random_delay_ba,
        #                                      market=self.MARKET_CHOICES.hourly)
        # self.no_delay_mock = self._run_net_test(self.random_no_delay_ba,
        #                                         market=self.MARKET_CHOICES.hourly)
        self.can_mex = ['IESO', 'BCTC', 'MHEB', 'AESO', 'HQT', 'NBSO', 'CFE',
                        'SPC']
        self.us_bas = [i for i in self.BA_CHOICES if i not in self.can_mex]
        # print(self.delay_mock)
        # print(self.no_delay_mock)

        # Mock(return_value="mocked stuff")

    def test_null_response(self):
        self._run_null_repsonse_test(self.BA_CHOICES[0], latest=True)

    def test_latest(self):
        for ba in self.BA_CHOICES:
            # basic test
            data = self._run_net_test(ba, latest=True,
                                      market=self.MARKET_CHOICES.hourly)

            # test all timestamps are equal
            timestamps = [d['timestamp'] for d in data]
            self.assertEqual(len(set(timestamps)), 1)

            # test flags
            for dp in data:
                self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
                self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)
            time.sleep(15)  # Delay to cut down on throttling

    def test_latest_some(self):
        for ba in random.sample(self.BA_CHOICES, 5):
            # basic test
            data = self._run_net_test(ba, latest=True,
                                      market=self.MARKET_CHOICES.hourly)

            # test all timestamps are equal
            timestamps = [d['timestamp'] for d in data]
            self.assertEqual(len(set(timestamps)), 1)

            # test flags
            for dp in data:
                self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
                self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)
            time.sleep(5)  # Delay to cut down on throttling

    def test_date_range_some(self):
        for ba in random.sample(self.no_delay_bas, 5):
            # basic test
            today = datetime.today().replace(tzinfo=pytz.utc)
            data = self._run_net_test(ba, start_at=today-timedelta(days=2),
                                      end_at=today-timedelta(days=1))

            # test timestamps are not equal
            timestamps = [d['timestamp'] for d in data]
            self.assertGreater(len(set(timestamps)), 1)

            # test flags
            for dp in data:
                self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
                self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_forecast_raises_valueerror(self):
        """Ensure get trade with forecast raises an error."""
        ba = random.sample(self.no_delay_bas, 1)[0]
        with self.assertRaises(ValueError):
            self._run_net_test(ba, forecast=True)

    def test_all_us_bas(self):
        for ba in self.us_bas:
            data = self._run_net_test(ba, market=self.MARKET_CHOICES.hourly)
            self.assertGreater(len(data), 1)
    # start here- this is failing unexpectedly


    def test_non_us_bas_raise_valueerror(self):
        for ba in self.can_mex:
            with self.assertRaises(ValueError):
                self._run_net_test(ba, market=self.MARKET_CHOICES.hourly)
