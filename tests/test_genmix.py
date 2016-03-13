from pyiso import client_factory, BALANCING_AUTHORITIES
from pyiso.base import FUEL_CHOICES, BaseClient
from unittest import TestCase, skip
import pytz
from datetime import datetime, timedelta


class TestBaseGenMix(TestCase):
    def setUp(self):
        # set up expected values from base client
        bc = BaseClient()
        self.MARKET_CHOICES = bc.MARKET_CHOICES
        self.FREQUENCY_CHOICES = bc.FREQUENCY_CHOICES

        # set up other expected values
        self.FUEL_CHOICES = FUEL_CHOICES
        self.BA_CHOICES = BALANCING_AUTHORITIES.keys()

    def _run_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)

        # get data
        data = c.get_generation(**kwargs)

        # test number
        self.assertGreater(len(data), 1)

        # test contents
        for dp in data:
            # test key names
            self.assertEqual(set(['gen_MW', 'ba_name', 'fuel_name',
                                  'timestamp', 'freq', 'market']),
                             set(dp.keys()))

            # test values
            self.assertEqual(dp['timestamp'].tzinfo, pytz.utc)
            self.assertIn(dp['fuel_name'], self.FUEL_CHOICES)
            self.assertIn(dp['ba_name'], self.BA_CHOICES)

            # test for numeric gen
            self.assertGreaterEqual(dp['gen_MW']+1, dp['gen_MW'])

            # test earlier than now
            if c.options.get('forecast', False):
                self.assertGreater(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))
            else:
                self.assertLess(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))

        # return
        return data

    def _run_notimplemented_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)

        # method not implemented yet
        self.assertRaises(NotImplementedError, c.get_generation)


class TestISONEGenMix(TestBaseGenMix):
    def test_isne_latest(self):
        # basic test
        data = self._run_test('ISONE', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

    def test_isne_date_range(self):
        # basic test
        data = self._run_test('ISONE', start_at=datetime.today()-timedelta(days=2),
                              end_at=datetime.today()-timedelta(days=1))

        # test multiple
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)


class TestMISOGenMix(TestBaseGenMix):
    def test_miso_latest(self):
        # basic test
        data = self._run_test('MISO', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

    def test_forecast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('MISO', start_at=today + timedelta(hours=10),
                              end_at=today+timedelta(days=2))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test timestamps in range
        self.assertGreaterEqual(min(timestamps), today+timedelta(hours=10))
        self.assertLessEqual(min(timestamps), today+timedelta(days=2))


@skip
class TestSPPGenMix(TestBaseGenMix):
    @skip
    def test_spp_latest_hr(self):
        # basic test
        data = self._run_test('SPP', latest=True, market=self.MARKET_CHOICES.hourly)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    @skip
    def test_spp_date_range_hr(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('SPP', start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1),
                              market=self.MARKET_CHOICES.hourly)

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    @skip
    def test_spp_latest_5min(self):
        # basic test
        data = self._run_test('SPP', latest=True, market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    @skip
    def test_spp_yesterday_5min(self):
        # basic test
        data = self._run_test('SPP', yesterday=True, market=self.MARKET_CHOICES.fivemin)

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    @skip
    def test_preprocess(self):
        row = '04/09/2014 05:55,12966.33,0,3836.029,149.3688,1306.19,2.025,0,0,6.81,5540.4,23876.7'
        processed_row = client_factory('SPP')._preprocess(row)
        self.assertEqual(len(processed_row), len(row.split(',')))


class TestBPAGenMix(TestBaseGenMix):
    def test_bpa_latest(self):
        # basic test
        data = self._run_test('BPA', latest=True, market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_bpa_date_range(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('BPA', start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1))

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_bpa_date_range_farpast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('BPA', start_at=today-timedelta(days=20),
                              end_at=today-timedelta(days=10))

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)


class TestCAISOGenMix(TestBaseGenMix):
    def test_caiso_date_range(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('CAISO', start_at=today-timedelta(days=3),
                              end_at=today-timedelta(days=2), market=self.MARKET_CHOICES.hourly)

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

        # test fuel names
        fuels = set([d['fuel_name'] for d in data])
        expected_fuels = ['solarpv', 'solarth', 'geo', 'smhydro', 'wind', 'biomass', 'biogas',
                          'thermal', 'hydro', 'nuclear']
        for expfuel in expected_fuels:
            self.assertIn(expfuel, fuels)

    def test_caiso_yesterday(self):
        # basic test
        data = self._run_test('CAISO', yesterday=True, market=self.MARKET_CHOICES.hourly)

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

        # test fuel names
        fuels = set([d['fuel_name'] for d in data])
        expected_fuels = ['solarpv', 'solarth', 'geo', 'smhydro', 'wind', 'biomass', 'biogas',
                          'thermal', 'hydro', 'nuclear']
        for expfuel in expected_fuels:
            self.assertIn(expfuel, fuels)

    def test_caiso_latest(self):
        # basic test
        data = self._run_test('CAISO', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.tenmin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.tenmin)

        # test fuel names
        fuels = set([d['fuel_name'] for d in data])
        expected_fuels = ['renewable', 'other']
        for expfuel in expected_fuels:
            self.assertIn(expfuel, fuels)

    def test_caiso_forecast(self):
        # basic test
        now = pytz.utc.localize(datetime.utcnow())
        data = self._run_test('CAISO', start_at=now+timedelta(hours=2),
                              end_at=now+timedelta(hours=12))

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.dam)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

        # test fuel names
        fuels = set([d['fuel_name'] for d in data])
        expected_fuels = ['wind', 'solar', 'other']
        for expfuel in expected_fuels:
            self.assertIn(expfuel, fuels)


class TestERCOTGenMix(TestBaseGenMix):
    def test_ercot_latest(self):
        data = self._run_test('ERCOT', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

        # test fuel names
        fuels = set([d['fuel_name'] for d in data])
        expected_fuels = ['wind', 'nonwind']
        for expfuel in expected_fuels:
            self.assertIn(expfuel, fuels)


class TestPJMGenMix(TestBaseGenMix):
    def test_failing(self):
        self._run_notimplemented_test('PJM')


class TestNYISOGenMix(TestBaseGenMix):
    def test_latest(self):
        # basic test
        data = self._run_test('NYISO', latest=True, market=self.MARKET_CHOICES.fivemin)

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
        data = self._run_test('NYISO', start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1))

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_date_range_farpast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('NYISO', start_at=today-timedelta(days=20),
                              end_at=today-timedelta(days=18))

        # test timestamps are different 5-min for 2 days for 7 fuels
        # subtract one hour's worth for DST
        timestamps = [d['timestamp'] for d in data]
        self.assertGreaterEqual(len(timestamps), 12*24*2*7-12)


class TestNEVPGenMix(TestBaseGenMix):
    def test_failing(self):
        self._run_notimplemented_test('NEVP')


class TestSPPCGenMix(TestBaseGenMix):
    def test_failing(self):
        self._run_notimplemented_test('SPPC')


class TestSVERIGenMix(TestBaseGenMix):
    def setUp(self):
        super(TestSVERIGenMix, self).setUp()
        self.bas = [k for k, v in BALANCING_AUTHORITIES.items() if v['module'] == 'sveri']

    def test_latest_all(self):
        for ba in self.bas:
            self._test_latest(ba)

    def test_date_range_all(self):
        for ba in self.bas:
            self._test_date_range(ba)

    def _test_latest(self, ba):
        # basic test
        data = self._run_test(ba, latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

        # test fuel names
        fuels = set([d['fuel_name'] for d in data])
        expected_fuels = ['solar', 'natgas', 'renewable', 'fossil', 'hydro', 'wind', 'coal', 'nuclear']
        for expfuel in expected_fuels:
            self.assertIn(expfuel, fuels)

    def _test_date_range(self, ba):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test(ba, start_at=today - timedelta(days=3),
                              end_at=today - timedelta(days=2), market=self.MARKET_CHOICES.fivemin)

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

        # test fuel names
        fuels = set([d['fuel_name'] for d in data])
        expected_fuels = ['solar', 'natgas', 'renewable', 'fossil', 'hydro', 'wind', 'coal', 'nuclear']
        for expfuel in expected_fuels:
            self.assertIn(expfuel, fuels)
