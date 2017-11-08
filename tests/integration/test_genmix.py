from pyiso import client_factory, BALANCING_AUTHORITIES
from pyiso.base import FUEL_CHOICES, BaseClient
from unittest import TestCase, skip
from datetime import datetime, timedelta
import pytz
import mock


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
                self.assertGreater(dp['timestamp'], datetime.now(pytz.utc))
            else:
                self.assertLess(dp['timestamp'], datetime.now(pytz.utc))

            # test within date range
            start_at = c.options.get('start_at', False)
            end_at = c.options.get('end_at', False)
            if start_at and end_at:
                self.assertGreaterEqual(dp['timestamp'], start_at)
                self.assertLessEqual(dp['timestamp'], end_at)

        # return
        return data

    def _run_notimplemented_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)

        # method not implemented yet
        self.assertRaises(NotImplementedError, c.get_generation)

    def _run_null_response_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)

        # mock request
        with mock.patch.object(c, 'request') as mock_request:
            mock_request.return_value = None

            # get data
            data = c.get_generation(**kwargs)

            # test
            self.assertEqual(data, [])


class TestISONEGenMix(TestBaseGenMix):
    def test_null_response_latest(self):
        self._run_null_response_test('ISONE', latest=True)

    def test_latest(self):
        # basic test
        data = self._run_test('ISONE', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

    def test_date_range(self):
        # basic test
        data = self._run_test('ISONE', start_at=datetime.today()-timedelta(days=2),
                              end_at=datetime.today()-timedelta(days=1))

        # test multiple
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)


class TestMISOGenMix(TestBaseGenMix):
    def test_null_response_latest(self):
        self._run_null_response_test('MISO', latest=True)

    def test_latest(self):
        # basic test
        data = self._run_test('MISO', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

    # @freezegun.freeze_time('2016-05-17 06:00', tz_offset=0, tick=True)
    # @requests_mock.mock()
    def test_forecast(self):  # , mocker):
        # url = ('https://www.misoenergy.org/Library/Repository/Market%20Reports/'
        #        '20160517_da_ex.xls')
        # mocker.get(url, content=open('responses/20160517_da_ex.xls', 'r').read())
        # mocker.get(url.replace('0517', '0518'), status_code=404)
        # basic test
        today = datetime.now(pytz.utc)
        data = self._run_test('MISO', start_at=today+timedelta(hours=2),
                              end_at=today+timedelta(days=1))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test timestamps in range
        self.assertGreaterEqual(min(timestamps), today+timedelta(hours=2))
        self.assertLessEqual(min(timestamps), today+timedelta(days=2))


@skip('SPP broken')
class TestSPPGenMix(TestBaseGenMix):
    def test_latest_hr(self):
        # basic test
        data = self._run_test('SPP', latest=True, market=self.MARKET_CHOICES.hourly)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_date_range_hr(self):
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

    def test_latest_5min(self):
        # basic test
        data = self._run_test('SPP', latest=True, market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_yesterday_5min(self):
        # basic test
        data = self._run_test('SPP', yesterday=True, market=self.MARKET_CHOICES.fivemin)

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_preprocess(self):
        row = '04/09/2014 05:55,12966.33,0,3836.029,149.3688,1306.19,2.025,0,0,6.81,5540.4,23876.7'
        processed_row = client_factory('SPP')._preprocess(row)
        self.assertEqual(len(processed_row), len(row.split(',')))


class TestBPAGenMix(TestBaseGenMix):
    def test_null_response_latest(self):
        self._run_null_response_test('BPA', latest=True)

    def test_latest(self):
        # basic test
        data = self._run_test('BPA', latest=True, market=self.MARKET_CHOICES.fivemin)

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
        data = self._run_test('BPA', start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1))

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_date_range_farpast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('BPA', start_at=today-timedelta(days=20),
                              end_at=today-timedelta(days=10))

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)


class TestCAISOGenMix(TestBaseGenMix):
    def test_null_response_latest(self):
        self._run_null_response_test('CAISO', latest=True)

    def test_date_range_rthr(self):
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

    def test_date_range_dahr(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('CAISO',
                              start_at=today-timedelta(days=3, hours=3),
                              end_at=today-timedelta(days=3, hours=1),
                              market=self.MARKET_CHOICES.dam)

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

    def test_yesterday(self):
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

    def test_latest(self):
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

    def test_forecast(self):
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
    def test_null_response_latest(self):
        self._run_null_response_test('ERCOT', latest=True)

    def test_latest(self):
        data = self._run_test('ERCOT', latest=True, market=self.MARKET_CHOICES.fivemin)

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
    def test_null_response_latest(self):
        self._run_null_response_test('PJM', latest=True)

    def test_latest(self):
        # basic test
        data = self._run_test('PJM', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_date_range_fails(self):
        # only latest data
        today = datetime.today().replace(tzinfo=pytz.utc)
        self.assertRaises(ValueError, self._run_test, 'PJM',
                          start_at=today-timedelta(days=2),
                          end_at=today-timedelta(days=1))


class TestNSPowerGenMix(TestBaseGenMix):
    def test_null_response_latest(self):
        self._run_null_response_test('NSP', latest=True)

    def test_latest(self):
        # basic test
        data = self._run_test('NSP', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

    def test_date_range(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('NSP', start_at=today-timedelta(days=1),
                              end_at=today)

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)


class TestNYISOGenMix(TestBaseGenMix):
    def test_null_response_latest(self):
        self._run_null_response_test('NYISO', latest=True)

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

    # @freezegun.freeze_time('2016-05-18 12:00', tz_offset=0, tick=True)
    # @requests_mock.mock()
    def test_date_range_farpast(self):  # , mocker):
        # for n in range(28, 30+1):
        #     mocker.get(
        #         'http://mis.nyiso.com/public/csv/rtfuelmix/201604%srtfuelmix.csv' % n,
        #         text='Too far back',
        #         status_code=404)
        # mocker.get(
        #     'http://mis.nyiso.com/public/csv/rtfuelmix/20160401rtfuelmix_csv.zip',
        #     content=open('responses/20160401rtfuelmix.csv.zip', 'rb').read())

        # basic test
        today = datetime.now(pytz.utc)
        data = self._run_test('NYISO', start_at=today-timedelta(days=20),
                              end_at=today-timedelta(days=18))

        # test timestamps are different 5-min for 2 days for 7 fuels
        # subtract one hour's worth for DST
        timestamps = [d['timestamp'] for d in data]
        self.assertGreaterEqual(len(timestamps), 12*24*2*7-12)


class TestNEVPGenMix(TestBaseGenMix):
    def test_failing(self):
        self._run_notimplemented_test('NEVP')


class TestPEIGenMix(TestBaseGenMix):
    def test_null_response_latest(self):
        self._run_null_response_test('PEI', latest=True)

    def test_latest(self):
        # basic test
        data = self._run_test('PEI', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)


class TestSPPCGenMix(TestBaseGenMix):
    def test_failing(self):
        self._run_notimplemented_test('SPPC')


class TestSVERIGenMix(TestBaseGenMix):
    def setUp(self):
        super(TestSVERIGenMix, self).setUp()
        self.bas = [k for k, v in BALANCING_AUTHORITIES.items() if v['module'] == 'sveri']

    def test_null_response_latest(self):
        self._run_null_response_test(self.bas[0], latest=True)

    # @freezegun.freeze_time('2016-05-20 12:10', tz_offset=0, tick=True)
    # @requests_mock.mock()
    def test_latest_all(self):  # , mocker):
        for ba in self.bas:
            # Open both response files and setup mocking
            # with open('responses/' + ba + '_1-4_latest.txt', 'r') as ffile:
            #     resp1 = ffile.read()
            # with open('responses/' + ba + '_5-8_latest.txt', 'r') as ffile:
            #     resp2 = ffile.read()
            # mocker.get(self.client.BASE_URL, [{'content': resp1}, {'content': resp2}])

            # run tests
            self._test_latest(ba)

    # @freezegun.freeze_time('2016-05-20 09:00', tz_offset=0, tick=True)
    # @requests_mock.mock()
    def test_date_range_all(self):  # , mocker):
        for ba in self.bas:
            # Open both response files and setup mocking
            # with open('responses/' + ba + '_1-4.txt', 'r') as ffile:
            #     resp1 = ffile.read()
            # with open('responses/' + ba + '_5-8.txt', 'r') as ffile:
            #     resp2 = ffile.read()
            # mocker.get(self.client.BASE_URL, [{'content': resp1}, {'content': resp2}])

            # run tests
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


class TestYukonEnergyClientGenMix(TestBaseGenMix):
    def test_null_response_latest(self):
        self._run_null_response_test('YUKON', latest=True)

    def test_latest(self):
        data = self._run_test('YUKON', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.tenmin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.tenmin)

    def test_date_range(self):
        # basic test
        now = datetime.utcnow().replace(tzinfo=pytz.utc)
        start_at = now - timedelta(hours=24)
        data = self._run_test('YUKON', start_at=start_at, end_at=now)

        # test timestamps are different
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)
