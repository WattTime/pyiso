from pyiso import client_factory, BALANCING_AUTHORITIES
from pyiso.base import BaseClient
from unittest import TestCase
import pytz
from datetime import datetime, timedelta
from nose_parameterized import parameterized


class TestBaseLMP(TestCase):
    def setUp(self):
        # set up expected values from base client
        bc = BaseClient()
        self.MARKET_CHOICES = bc.MARKET_CHOICES
        self.FREQUENCY_CHOICES = bc.FREQUENCY_CHOICES

        # set up other expected values
        self.BA_CHOICES = BALANCING_AUTHORITIES.keys()

    def _run_test(self, ba_name, expect_data=True, tol_min=8, **kwargs):
        # set up
        c = client_factory(ba_name)

        # get data
        data = c.get_lmp(**kwargs)

        # test number
        if expect_data:
            self.assertGreaterEqual(len(data), 1)
        else:
            self.assertEqual(data, [])
            return data

        # test contents
        for dp in data:
            # test key names
            self.assertEqual(set(['lmp', 'ba_name',
                                  'timestamp', 'market', 'node_id', 'lmp_type', 'freq']),
                             set(dp.keys()))

            # test values
            self.assertEqual(dp['timestamp'].tzinfo, pytz.utc)
            self.assertIn(dp['ba_name'], self.BA_CHOICES)
            self.assertIn(dp['lmp_type'],
                          ['LMP', 'prc', 'energy', 'TotalLMP'])

            # test for numeric price
            self.assertGreaterEqual(dp['lmp']+1, dp['lmp'])

        # test correct temporal relationship to now
        timestamps = [t['timestamp'] for t in data]
        now = pytz.utc.localize(datetime.utcnow())
        if c.options['forecast']:
            self.assertGreaterEqual(max(timestamps), now - timedelta(hours=1))
        elif c.options['latest']:
            tset = list(set(timestamps))
            self.assertEqual(len(tset), 1)
            # within 8 min
            delta = now - tset[0]
            self.assertLess(abs(delta.total_seconds()), tol_min*60)
        else:
            if 'end_at' in kwargs and kwargs['end_at']:
                self.assertLess(max(timestamps), kwargs['end_at'] + timedelta(hours=1))
            else:
                self.assertLess(max(timestamps), now)

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
                              latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def date_range(self, market):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('CAISO', node_id='SLAP_PGP2-APND',
                              start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1),
                              market=market)

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)
        self.assertEqual(list(set([d['market'] for d in data])),
                         [market])
        return data

    def test_date_range_rtm(self):
        data = self.date_range(self.MARKET_CHOICES.fivemin)
        self.assertGreaterEqual(len(data), 12*23)
        self.assertLessEqual(len(data), 12*24)

    def test_date_range_dam(self):
        data = self.date_range(self.MARKET_CHOICES.dam)
        self.assertEqual(len(data), 24)

    def test_date_range_hourly(self):
        data = self.date_range(self.MARKET_CHOICES.hourly)
        self.assertGreaterEqual(len(data), 92)
        self.assertLessEqual(len(data), 96)

    def test_date_range_rtpd(self):
        data = self.date_range(self.MARKET_CHOICES.fifteenmin)
        self.assertEqual(len(data), 24*4)

    def test_forecast(self):
        # basic test
        now = pytz.utc.localize(datetime.utcnow())
        data = self._run_test('CAISO', node_id='SLAP_PGP2-APND',
                              start_at=now,
                              end_at=now+timedelta(days=1),
                              market=self.MARKET_CHOICES.dam)

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_bad_node(self):
        self._run_test('CAISO', node_id='badnode', expect_data=False, latest=True)

    def test_multiple_latest(self):
        node_list = ['SLAP_PGP2-APND', 'SLAP_PGEB-APND']
        data = self._run_test('CAISO', node_id=node_list,
                              latest=True, market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        nodes = [d['node_id'] for d in data]
        self.assertEqual(nodes.sort(), node_list.sort())

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)


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
        data = self._run_test('NYISO', node_id=None,
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


class TestPJMLMP(TestBaseLMP):
    def test_latest(self):
        # basic test
        data = self._run_test('PJM', node_id='COMED',
                              market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_latest_oasis(self):
        data = self._run_test('PJM', node_id=None, tol_min=10,
                              market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_date_range_dayahead_hourly(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('PJM', node_id=33092371,
                              start_at=today-timedelta(days=2),
                              end_at=today-timedelta(hours=40))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_date_range_realtime_hourly(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('PJM', node_id=33092371,
                              start_at=today-timedelta(days=3),
                              end_at=today-timedelta(days=1),
                              market=self.MARKET_CHOICES.hourly)

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_multiple_lmp_realtime(self):
        node_list = ['AECO', 'AEP', 'APS', 'ATSI', 'BGE', 'COMED', 'DAY', 'DAY',
                     'DOM', 'DPL', 'DUQ', 'EKPC', 'JCPL', 'METED', 'PECO', 'PENELEC',
                     'PEPCO', 'PPL', 'PSEG', 'RECO']
        data = self._run_test('PJM', tol_min=10, node_id=node_list, latest=True,
                              market=self.MARKET_CHOICES.fivemin)

        nodes_returned = [d['node_id'] for d in data]
        for node in nodes_returned:
            self.assertIn(node, nodes_returned)

    def test_multiple_lmp_realtime_mixed(self):
        node_list = ['AECO', 'AEP', 'APS', 'ATSI', 'BGE', 'COMED', 'DAY', 'DAY',
                     'DOM', 'DPL', 'DUQ', 'EKPC', 'JCPL', 'METED', 'PECO', 'PENELEC',
                     'PEPCO', 'PPL', 'PSEG', 'RECO', '33092371']
        data = self._run_test('PJM', tol_min=10, node_id=node_list, latest=True,
                              market=self.MARKET_CHOICES.fivemin)

        nodes_returned = [d['node_id'] for d in data]
        for node in nodes_returned:
            self.assertIn(node, nodes_returned)

    def test_multiple_lmp_realtime_oasis(self):
        node_list = ['MERIDIAN EWHITLEY', 'LANSDALE']
        data = self._run_test('PJM', tol_min=10, node_id=node_list, latest=True,
                              market=self.MARKET_CHOICES.fivemin)

        nodes_returned = [d['node_id'] for d in data]
        for node in nodes_returned:
            self.assertIn(node, nodes_returned)


class TestMISOLMP(TestBaseLMP):
    def test_latest(self):
        # basic test
        data = self._run_test('MISO', node_id=None,
                              market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_today(self):
        now = datetime.now(pytz.utc)
        data = self._run_test('MISO',  start_at=now - timedelta(days=1), end_at=now,
                              market=self.MARKET_CHOICES.hourly)
        self.assertGreater(len(data), 1)

    def test_forecast(self):
        # basic test
        now = pytz.utc.localize(datetime.utcnow())
        data = self._run_test('MISO', node_id=None,
                              start_at=now, end_at=now+timedelta(days=1))

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.dam)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_historical(self):
        start = datetime(2016, 5, 1, 0, tzinfo=pytz.utc)
        data = self._run_test('MISO', start_at=start, end_at=start+timedelta(days=1))
        self.assertEqual(len(data), 25)


class TestERCOTLMP(TestBaseLMP):
    def test_latest(self):
        # basic test
        data = self._run_test('ERCOT', latest=True, tol_min=10,
                              market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_latest_single_node(self):
        data = self._run_test('ERCOT', node_id='HB_HOUSTON', latest=True,
                              tol_min=10,
                              market=self.MARKET_CHOICES.fivemin)

        self.assertEqual(len(data), 1)

    def test_latest_multi_node(self):
        data = self._run_test('ERCOT', node_id=['LZ_HOUSTON', 'LZ_NORTH'],
                              latest=True, tol_min=10,
                              market=self.MARKET_CHOICES.fivemin)

        self.assertEqual(len(data), 2)

    def test_date_range_dayahead_hourly(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('ERCOT', node_id=['LZ_HOUSTON', 'LZ_NORTH'],
                              start_at=today-timedelta(days=2),
                              end_at=today-timedelta(hours=40),
                              market=self.MARKET_CHOICES.dam)

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_forecast(self):
        # basic test
        now = pytz.utc.localize(datetime.utcnow())
        data = self._run_test('ERCOT', node_id=['LZ_HOUSTON', 'LZ_NORTH'],
                              start_at=now, end_at=now+timedelta(days=1))

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.dam)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_rt5m_historical(self):
        now = pytz.utc.localize(datetime.utcnow()) - timedelta(hours=1)
        data = self._run_test('ERCOT', start_at=now - timedelta(minutes=20),
                              end_at=now,
                              market=self.MARKET_CHOICES.fivemin)
        self.assertEqual(len(data), 4)

    def test_dam_historical(self):
        start = datetime.today().replace(tzinfo=pytz.utc) - timedelta(days=30)
        data = self._run_test('ERCOT', start_at=start, end_at=start+timedelta(days=1))
        # slicing is inclusive
        self.assertIn(len(data), [24, 25])


class TestMinimumLMP(TestBaseLMP):
    @parameterized.expand([
        ('CAISO', 'CAISO', True),
        ('MISO', 'MISO', True),
        ('ERCOT', 'ERCOT', True),
        ('NYISO', 'NYISO', True),
        ('ISONE', 'ISONE', True),
        ('PJM', 'PJM', True),
    ])
    def test_latest(self, name, ba, expected):
        data = self._run_test(ba, latest=True, tol_min=10,
                              market=self.MARKET_CHOICES.fivemin)
        self.assertEqual(len(data), 1)
        self.assertEqual(len(set([t['node_id'] for t in data])), 1)
        self.assertEqual(len(set([t['timestamp'] for t in data])), 1)

    @parameterized.expand([
        ('CAISO', 'CAISO', True),
        ('MISO', 'MISO', True),
        ('ERCOT', 'ERCOT', True),
        ('NYISO', 'NYISO', True),
        ('ISONE', 'ISONE', True),
        ('PJM', 'PJM', True),

    ])
    def test_forecast(self, name, ba, expected):
        now = datetime.now(pytz.utc)
        data = self._run_test(ba,  start_at=now, end_at=now + timedelta(days=1),
                              market=self.MARKET_CHOICES.dam, tol_min=10)
        self.assertGreater(len(data), 2)
        self.assertEqual(len(set([t['node_id'] for t in data])), 1)

    @parameterized.expand([
        ('CAISO', 'CAISO', True),
        ('MISO', 'MISO', True),
        # ('ERCOT', 'ERCOT', True),
        ('NYISO', 'NYISO', True),
        ('ISONE', 'ISONE', True),
        ('PJM', 'PJM', True),
    ])
    def test_today(self, name, ba, expected):
        now = datetime.now(pytz.utc)
        data = self._run_test(ba,  start_at=now - timedelta(days=1), end_at=now,
                              market=self.MARKET_CHOICES.hourly, tol_min=10)
        self.assertGreater(len(data), 2)
        self.assertEqual(len(set([t['node_id'] for t in data])), 1)
