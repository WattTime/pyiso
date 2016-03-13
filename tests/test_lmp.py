from pyiso import client_factory, BALANCING_AUTHORITIES
from pyiso.base import BaseClient
from unittest import TestCase
import pytz
from datetime import datetime, timedelta


class TestBaseLMP(TestCase):
    def setUp(self):
        # set up expected values from base client
        bc = BaseClient()
        self.MARKET_CHOICES = bc.MARKET_CHOICES
        self.FREQUENCY_CHOICES = bc.FREQUENCY_CHOICES

        # set up other expected values
        self.BA_CHOICES = BALANCING_AUTHORITIES.keys()

    def _run_test(self, ba_name, expect_data=True, **kwargs):
        # set up
        c = client_factory(ba_name)

        # get data
        data = c.get_lmp(**kwargs)

        # test number
        if expect_data:
            self.assertGreaterEqual(len(data), 1)
        else:
            self.assertEqual(data, [])

        # test contents
        for dp in data:
            # test key names
            self.assertEqual(set(['lmp', 'ba_name',
                                  'timestamp', 'market', 'node_id', 'lmp_type', 'freq']),
                             set(dp.keys()))

            # test values
            self.assertEqual(dp['timestamp'].tzinfo, pytz.utc)
            self.assertIn(dp['ba_name'], self.BA_CHOICES)

            # test for numeric price
            self.assertGreaterEqual(dp['lmp']+1, dp['lmp'])

            # test correct temporal relationship to now
            now = pytz.utc.localize(datetime.utcnow())
            if c.options['forecast']:
                self.assertGreaterEqual(dp['timestamp'], now)
            elif c.options['latest']:
                # within 5 min
                delta = now - dp['timestamp']
                self.assertLess(abs(delta.total_seconds()), 5.5*60)
            else:
                self.assertLess(dp['timestamp'], now)

        # return
        return data

    def _run_notimplemented_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)

        # method not implemented yet
        self.assertRaises(NotImplementedError, c.get_lmp)


class TestCAISOLMP(TestBaseLMP):
    def test_latest(self):
        # basic test
        data = self._run_test('CAISO', node_id='SLAP_PGP2-APND',
                              latest=True, market=self.MARKET_CHOICES.fivemin)

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
        data = self._run_test('CAISO', node_id='SLAP_PGP2-APND',
                              start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_forecast(self):
        # basic test
        now = pytz.utc.localize(datetime.utcnow())
        data = self._run_test('CAISO', node_id='SLAP_PGP2-APND',
                              start_at=now+timedelta(hours=20),
                              end_at=now+timedelta(days=2),
                              market=self.MARKET_CHOICES.dam)

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_bad_node(self):
        self._run_test('CAISO', node_id='badnode', expect_data=False, latest=True)


class TestISONELMP(TestBaseLMP):
    def test_latest(self):
        # basic test
        data = self._run_test('ISONE', node_id='NEMASSBost',
                              latest=True, market=self.MARKET_CHOICES.fivemin)

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
        data = self._run_test('ISONE', node_id='NEMASSBost',
                              start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)


class TestNYISOLMP(TestBaseLMP):
    def test_latest(self):
        # basic test
        data = self._run_test('NYISO', node_id='LONGIL',
                              latest=True, market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_forecast(self):
        # basic test
        now = pytz.utc.localize(datetime.utcnow())
        data = self._run_test('NYISO', node_id='LONGIL',
                              start_at=now, end_at=now+timedelta(days=1))

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.dam)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_date_range(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('NYISO', node_id='LONGIL',
                              start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)
