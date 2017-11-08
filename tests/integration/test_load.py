from pyiso import client_factory, BALANCING_AUTHORITIES
from pyiso.base import BaseClient
from pyiso.eu import EUClient
from unittest import TestCase
from datetime import datetime, timedelta
import unittest
import pytz
import mock
import libfaketime
libfaketime.reexec_if_needed()


class TestBaseLoad(TestCase):
    def setUp(self):
        # set up expected values from base client
        bc = BaseClient()
        self.MARKET_CHOICES = bc.MARKET_CHOICES
        self.FREQUENCY_CHOICES = bc.FREQUENCY_CHOICES

        # set up other expected values
        self.BA_CHOICES = BALANCING_AUTHORITIES.keys()

    def _run_test(self, ba_name, expect_data=True, tol_min=0, **kwargs):
        # set up
        c = client_factory(ba_name)
        # get data
        data = c.get_load(**kwargs)

        # test number
        if expect_data:
            self.assertGreaterEqual(len(data), 1)
        else:
            self.assertEqual(data, [])

        # test contents
        for dp in data:
            # test key names
            self.assertEqual(set(['load_MW', 'ba_name',
                                  'timestamp', 'freq', 'market']),
                             set(dp.keys()))

            # test values
            self.assertEqual(dp['timestamp'].tzinfo, pytz.utc)
            self.assertIn(dp['ba_name'], self.BA_CHOICES)

            # test for numeric gen
            self.assertGreaterEqual(dp['load_MW']+1, dp['load_MW'])

            # test correct temporal relationship to now
            if c.options['forecast']:
                self.assertGreaterEqual(dp['timestamp'],
                                        pytz.utc.localize(datetime.utcnow())-timedelta(minutes=tol_min))
            else:
                self.assertLess(dp['timestamp'], pytz.utc.localize(datetime.utcnow()))

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
        self.assertRaises(NotImplementedError, c.get_load)

    def _run_null_repsonse_test(self, ba_name, **kwargs):
        # set up
        c = client_factory(ba_name)

        # mock request
        with mock.patch.object(c, 'request') as mock_request:
            mock_request.return_value = None

            # get data
            data = c.get_load(**kwargs)

            # test
            self.assertEqual(data, [])


class TestBPALoad(TestBaseLoad):
    def test_null_response_latest(self):
        self._run_null_repsonse_test('BPA', latest=True)

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

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_date_range_strings(self):
        # basic test
        self._run_test('BPA', start_at='2016-05-01', end_at='2016-05-03')

    def test_date_range_farpast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('BPA', start_at=today-timedelta(days=20),
                              end_at=today-timedelta(days=10))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)


class TestCAISOLoad(TestBaseLoad):
    def test_null_response_latest(self):
        self._run_null_repsonse_test('CAISO', latest=True)

    def test_latest(self):
        # basic test
        data = self._run_test('CAISO', latest=True, market=self.MARKET_CHOICES.fivemin)

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
        data = self._run_test('CAISO', start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1),
                              tol_min=1)

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_date_range_strings(self):
        # basic test
        self._run_test('CAISO', start_at='2016-05-01', end_at='2016-05-03')

#     @freezegun.freeze_time('2015-05-20 14:30', tz_offset=0, tick=True)
#     @requests_mock.mock()
#     def test_forecast(self, mocker):
#         url = 'http://oasis.caiso.com/oasisapi/SingleZip'
#         with open('responses/SLD_FCST.zip', 'rb') as ffile:
#             mocker.get(url, content=ffile.read())
#
    def test_forecast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('CAISO', start_at=today+timedelta(hours=4),
                              end_at=today+timedelta(days=2),
                              tol_min=4*60)

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)


class TestERCOTLoad(TestBaseLoad):
    def test_null_response_latest(self):
        self._run_null_repsonse_test('ERCOT', latest=True)

    def test_null_response_forecast(self):
        today = datetime.today().replace(tzinfo=pytz.utc)
        self._run_null_repsonse_test('ERCOT', start_at=today + timedelta(hours=20),
                                     end_at=today+timedelta(days=2))

    def test_latest(self):
        # basic test
        data = self._run_test('ERCOT', latest=True, market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_forecast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('ERCOT', start_at=today + timedelta(hours=20),
                              end_at=today+timedelta(days=2))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test timestamps in range
        self.assertGreaterEqual(min(timestamps), today+timedelta(hours=20))
        self.assertLessEqual(min(timestamps), today+timedelta(days=2))


class TestISONELoad(TestBaseLoad):
    def test_null_response_latest(self):
        self._run_null_repsonse_test('ISONE', latest=True)

    def test_latest(self):
        # basic test
        data = self._run_test('ISONE', latest=True)

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
        data = self._run_test('ISONE', start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_date_range_strings(self):
        # basic test
        self._run_test('ISONE', start_at='2016-05-01', end_at='2016-05-03')

    def test_forecast(self):
        # basic test
        data = self._run_test('ISONE', forecast=True, market='DAHR', freq='1hr')

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)


class TestMISOLoad(TestBaseLoad):
    def test_null_response_forecast(self):
        today = pytz.utc.localize(datetime.utcnow())
        self._run_null_repsonse_test('MISO', start_at=today + timedelta(hours=2),
                                     end_at=today+timedelta(days=2))

    def test_forecast(self):
        # basic test
        today = pytz.utc.localize(datetime.utcnow())
        data = self._run_test('MISO', start_at=today + timedelta(hours=2),
                              end_at=today+timedelta(days=2))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test timestamps in range
        self.assertGreaterEqual(min(timestamps), today+timedelta(hours=2))
        self.assertLessEqual(min(timestamps), today+timedelta(days=2))


class TestNEVPLoad(TestBaseLoad):
    def test_null_response_latest(self):
        self._run_null_repsonse_test('NEVP', latest=True)

    def test_latest(self):
        # basic test
        data = self._run_test('NEVP', latest=True)

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
        data = self._run_test('NEVP', start_at=today-timedelta(days=1),
                              end_at=today)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_date_range_strings(self):
        # basic test
        self._run_test('NEVP', start_at='2016-05-01', end_at='2016-05-03')

#     @libfaketime.fake_time('2016-05-20 14:45')
#     @requests_mock.mock()
#     def test_date_range_farpast(self, mocker):
#         url = ('http://www.oasis.oati.com/NEVP/NEVPdocs/inetloading/'
#                'Monthly_Ties_and_Loads_L_from_04_01_2016_to_04_30_2016_.html')
#         with open('responses/NEVP_load_farpast.htm', 'r') as ffile:
#             mocker.get(url, content=ffile.read())
#
    def test_date_range_farpast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('NEVP', start_at=today-timedelta(days=35),
                              end_at=today-timedelta(days=33))
        self.assertEqual(len(data), 2*24)


class TestNLHydroLoad(TestBaseLoad):
    def test_null_response_latest(self):
        self._run_null_repsonse_test('NLH', latest=True)

    def test_latest(self):
        data = self._run_test('NLH', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)


class TestNSPowerLoad(TestBaseLoad):
    def test_null_response_range(self):
        now = datetime.utcnow().replace(tzinfo=pytz.utc)
        self._run_null_repsonse_test('NSP', start_at=now-timedelta(hours=2), end_at=now+timedelta(hours=2))

    def test_null_response_latest(self):
        self._run_null_repsonse_test('NSP', latest=True)

    def test_latest(self):
        data = self._run_test('NSP', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.hourly)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.hourly)

    def test_historical(self):
        now = datetime.utcnow().replace(tzinfo=pytz.utc)
        start_at = now - timedelta(hours=24)
        data = self._run_test('NSP', start_at=start_at, end_at=now)

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test timestamps in range
        self.assertGreaterEqual(min(timestamps), start_at)
        self.assertLessEqual(max(timestamps), now)

    def test_forecast(self):
        now = datetime.utcnow().replace(tzinfo=pytz.utc)
        end_at = now+timedelta(hours=24)
        data = self._run_test('NSP', start_at=now, end_at=end_at)

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test timestamps in range
        self.assertGreaterEqual(min(timestamps), now)
        self.assertLessEqual(max(timestamps), end_at)


class TestNYISOLoad(TestBaseLoad):
    def test_null_response_latest(self):
        self._run_null_repsonse_test('NYISO', latest=True)

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

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_date_range_strings(self):
        # basic test
        self._run_test('NYISO', start_at='2016-05-01', end_at='2016-05-03')

    def test_forecast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('NYISO', start_at=today + timedelta(hours=20),
                              end_at=today+timedelta(days=2))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test timestamps in range
        self.assertGreaterEqual(min(timestamps), today+timedelta(hours=20))
        self.assertLessEqual(min(timestamps), today+timedelta(days=2))


class TestPEILoad(TestBaseLoad):
    def test_null_response_latest(self):
        self._run_null_repsonse_test('PEI', latest=True)

    def test_latest(self):
        # basic test
        data = self._run_test('PEI', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.tenmin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.tenmin)


class TestPJMLoad(TestBaseLoad):
    def test_null_response_latest(self):
        self._run_null_repsonse_test('PJM', latest=True)

    def test_latest(self):
        # basic test
        data = self._run_test('PJM', latest=True, market=self.MARKET_CHOICES.fivemin)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        # test flags
        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.fivemin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.fivemin)

    def test_forecast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('PJM', start_at=today + timedelta(hours=20),
                              end_at=today+timedelta(days=2))

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test timestamps in range
        self.assertGreaterEqual(min(timestamps), today+timedelta(hours=20))
        self.assertLessEqual(min(timestamps), today+timedelta(days=2))

    def test_historical(self):
        start_at = datetime(2015, 1, 2, 0, tzinfo=pytz.utc)
        end_at = datetime(2015, 12, 31, 23, tzinfo=pytz.utc)
        data = self._run_test('PJM', start_at=start_at, end_at=end_at)

        timestamps = [d['timestamp'] for d in data]

        # 364 days, except for DST transition hours
        # TODO handle DST transitions instead of dropping them
        self.assertEqual(len(set(timestamps)), 364*24-2)

    def test_date_range_strings(self):
        data = self._run_test('PJM', start_at='2016-06-10', end_at='2016-06-11')

        timestamps = [d['timestamp'] for d in data]

        # 3 days plus 1 hr
        self.assertEqual(len(set(timestamps)), 24 + 1)


class TestSPPLoad(TestBaseLoad):
    def test_failing(self):
        self._run_notimplemented_test('SPP')


class TestSPPCLoad(TestBaseLoad):
    def test_null_response(self):
        self._run_null_repsonse_test('SPPC', latest=True)

    def test_latest(self):
        # basic test
        data = self._run_test('SPPC', latest=True)

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
        data = self._run_test('SPPC', start_at=today-timedelta(days=1),
                              end_at=today)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

#     @freezegun.freeze_time('2015-05-20 11:30', tz_offset=0, tick=True)
#     @requests_mock.mock()
#     def test_date_range_farpast(self, mocker):
#         url = ('http://www.oasis.oati.com/NEVP/NEVPdocs/inetloading/'
#                'Monthly_Ties_and_Loads_L_from_04_01_2015_to_04_30_2015_.html')
#         with open('responses/SPPC_load_farpast.htm', 'r') as ffile:
#             mocker.get(url, content=ffile.read())
    def test_date_range_farpast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('SPPC', start_at=today-timedelta(days=35),
                              end_at=today-timedelta(days=33))

    def test_date_range_strings(self):
        # basic test
        self._run_test('SPPC', start_at='2016-05-01', end_at='2016-05-03')


class TestSVERILoad(TestBaseLoad):
    def setUp(self):
        super(TestSVERILoad, self).setUp()
        self.bas = [k for k, v in BALANCING_AUTHORITIES.items() if v['module'] == 'sveri']

    def test_null_response(self):
        self._run_null_repsonse_test(self.bas[0], latest=True)

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


@unittest.skip('Not ready')
class TestEULoad(TestBaseLoad):
    def setUp(self):
        super(TestEULoad, self).setUp()
        self.BA_CHOICES = EUClient.CONTROL_AREAS.keys()

    def test_latest(self):
        # basic test
        data = self._run_test('EU', latest=True, market=self.MARKET_CHOICES.hourly,
                              control_area='IT')

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
        data = self._run_test('EU', start_at=today-timedelta(days=2),
                              end_at=today-timedelta(days=1),
                              control_area='IT')

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

    def test_forecast(self):
        # basic test
        today = datetime.today().replace(tzinfo=pytz.utc)
        data = self._run_test('EU', start_at=today+timedelta(hours=20),
                              end_at=today+timedelta(days=1),
                              control_area='IT')

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)


class TestYukonEnergyClientLoad(TestBaseLoad):
    def test_null_response_latest(self):
        self._run_null_repsonse_test('YUKON', latest=True)

    def test_latest(self):
        data = self._run_test('YUKON', latest=True)

        # test all timestamps are equal
        timestamps = [d['timestamp'] for d in data]
        self.assertEqual(len(set(timestamps)), 1)

        for dp in data:
            self.assertEqual(dp['market'], self.MARKET_CHOICES.tenmin)
            self.assertEqual(dp['freq'], self.FREQUENCY_CHOICES.tenmin)

    def test_historical(self):
        now = datetime.utcnow().replace(tzinfo=pytz.utc)
        start_at = now - timedelta(hours=24)
        data = self._run_test('YUKON', start_at=start_at, end_at=now)

        # test timestamps are not equal
        timestamps = [d['timestamp'] for d in data]
        self.assertGreater(len(set(timestamps)), 1)

        # test timestamps in range
        self.assertGreaterEqual(min(timestamps), start_at)
        self.assertLessEqual(max(timestamps), now)
