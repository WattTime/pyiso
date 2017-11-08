from pyiso import client_factory, tasks
from unittest import TestCase
from datetime import datetime, timedelta
import pytz
import pandas  # pandas must be imported before freezegun # noqa
# import requests_mock
# import freezegun


class TestGenerationTask(TestCase):
    def setUp(self):
        self.latest_kwargs = {'latest': True}
        now = pytz.utc.localize(datetime.utcnow())
        self.forecast_kwargs = {'start_at': now + timedelta(minutes=20),
                                'end_at': now + timedelta(days=1)}
        self.past_kwargs = {'start_at': now - timedelta(days=1),
                            'end_at': now - timedelta(hours=1)}
        self.past_kwargs_str = {'start_at': (now - timedelta(days=1)).strftime('%Y-%m-%d %H:%M'),
                                'end_at': (now - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M')}

    def _run_test(self, ba, kwargs):
        expected = client_factory(ba).get_generation(**kwargs)
        received = tasks.get_generation(ba, **kwargs)
        self.assertEqual(len(expected), len(received))

        for i in range(len(received)):
            if expected[i]['timestamp'] == received[i]['timestamp']:
                self.assertEqual(expected[i]['gen_MW'], received[i]['gen_MW'])
                self.assertEqual(expected[i]['fuel_name'], received[i]['fuel_name'])

    def test_bpa_latest(self):
        self._run_test('BPA', self.latest_kwargs)

    def test_bpa_past(self):
        self._run_test('BPA', self.past_kwargs)

    def test_bpa_past_str(self):
        self._run_test('BPA', self.past_kwargs_str)

    def test_caiso_latest(self):
        self._run_test('CAISO', self.latest_kwargs)

    def test_caiso_past(self):
        self._run_test('CAISO', self.past_kwargs)

    def test_caiso_past_str(self):
        self._run_test('CAISO', self.past_kwargs_str)

    # @freezegun.freeze_time('2016-05-20 12:45', tz_offset=0, tick=True)
    # @requests_mock.mock()
    def test_caiso_forecast(self):  # , mocker):
        # Set up mocking
        # url = 'http://oasis.caiso.com/oasisapi/SingleZip'
        # f = open('responses/ENE_SLRS.zip', 'rb')
        # first_resp = f.read()
        # f.close()
        # f = open('responses/SLD_REN_FCST.zip', 'rb')
        # second_resp = f.read()
        # f.close()
        # # get_generation first calls ENE_SLRS, then SLD_REN_FCST; gen_generation() called twice
        # mocker.get(url, [{'content': first_resp}, {'content': second_resp},
        #                  {'content': first_resp}, {'content': second_resp}])
        self._run_test('CAISO', self.forecast_kwargs)

    def test_ercot_latest(self):
        self._run_test('ERCOT', self.latest_kwargs)

    def test_isone_latest(self):
        self._run_test('ISONE', self.latest_kwargs)

    def test_isone_past(self):
        self._run_test('ISONE', self.past_kwargs)

    def test_isone_past_str(self):
        self._run_test('ISONE', self.past_kwargs_str)

    def test_miso_latest(self):
        self._run_test('MISO', self.latest_kwargs)

    def test_pjm_latest(self):
        self._run_test('PJM', self.latest_kwargs)

    def test_sveri_latest(self):
        self._run_test('AZPS', self.latest_kwargs)

    def test_sveri_past(self):
        self._run_test('AZPS', self.past_kwargs)

    def test_sveri_past_str(self):
        self._run_test('AZPS', self.past_kwargs_str)


class TestLoadTask(TestCase):
    def setUp(self):
        self.latest_kwargs = {'latest': True}
        now = pytz.utc.localize(datetime.utcnow())
        self.forecast_kwargs = {'start_at': now + timedelta(minutes=20), 'end_at': now + timedelta(days=1)}
        self.past_kwargs = {'start_at': now - timedelta(days=1),
                            'end_at': now - timedelta(hours=1)}
        self.past_kwargs_str = {'start_at': (now - timedelta(days=1)).strftime('%Y-%m-%d %H:%M'),
                                'end_at': (now - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M')}

    def _run_test(self, ba, kwargs):
        expected = client_factory(ba).get_load(**kwargs)
        received = tasks.get_load(ba, **kwargs)
        self.assertEqual(len(expected), len(received))

        for i in range(len(expected)):
            if expected[i]['timestamp'] == received[i]['timestamp']:
                self.assertEqual(expected[i]['load_MW'], received[i]['load_MW'])

    def test_bpa_latest(self):
        self._run_test('BPA', self.latest_kwargs)

    def test_bpa_past(self):
        self._run_test('BPA', self.past_kwargs)

    def test_bpa_past_str(self):
        self._run_test('BPA', self.past_kwargs_str)

    def test_caiso_latest(self):
        self._run_test('CAISO', self.latest_kwargs)

    def test_caiso_forecast(self):
        self._run_test('CAISO', self.forecast_kwargs)

    def test_caiso_past(self):
        self._run_test('CAISO', self.past_kwargs)

    def test_caiso_past_str(self):
        self._run_test('CAISO', self.past_kwargs_str)

    def test_isone_latest(self):
        self._run_test('ISONE', self.latest_kwargs)

    def test_isone_forecast(self):
        self._run_test('ISONE', self.forecast_kwargs)

    def test_isone_past(self):
        self._run_test('ISONE', self.past_kwargs)

    def test_isone_past_str(self):
        self._run_test('ISONE', self.past_kwargs_str)

    def test_pjm_latest(self):
        self._run_test('PJM', self.latest_kwargs)

    def test_pjm_forecast(self):
        self._run_test('PJM', self.forecast_kwargs)

    def test_ercot_latest(self):
        self._run_test('ERCOT', self.latest_kwargs)

    def test_ercot_forecast(self):
        self._run_test('ERCOT', self.forecast_kwargs)

    def test_nyiso_latest(self):
        self._run_test('NYISO', self.latest_kwargs)

    def test_nyiso_forecast(self):
        self._run_test('NYISO', self.forecast_kwargs)

    def test_nyiso_past(self):
        self._run_test('NYISO', self.past_kwargs)

    def test_nyiso_past_str(self):
        self._run_test('NYISO', self.past_kwargs_str)

    def test_sveri_latest(self):
        self._run_test('AZPS', self.latest_kwargs)

    def test_sveri_past(self):
        self._run_test('AZPS', self.past_kwargs)

    def test_sveri_past_str(self):
        self._run_test('AZPS', self.past_kwargs_str)


class TestTradeTask(TestCase):
    def setUp(self):
        self.latest_kwargs = {'latest': True}
        now = pytz.utc.localize(datetime.utcnow())
        self.forecast_kwargs = {'start_at': now + timedelta(minutes=20), 'end_at': now + timedelta(days=1)}
        self.past_kwargs = {'start_at': now - timedelta(days=1),
                            'end_at': now - timedelta(hours=1)}
        self.past_kwargs_str = {'start_at': (now - timedelta(days=1)).strftime('%Y-%m-%d %H:%M'),
                                'end_at': (now - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M')}

    def _run_test(self, ba, kwargs):
        expected = client_factory(ba).get_trade(**kwargs)
        received = tasks.get_trade(ba, **kwargs)
        self.assertEqual(len(expected), len(received))

        for i in range(len(expected)):
            if expected[i]['timestamp'] == received[i]['timestamp']:
                self.assertEqual(expected[i]['net_exp_MW'], received[i]['net_exp_MW'])

    def test_caiso_latest(self):
        self._run_test('CAISO', self.latest_kwargs)

    def test_caiso_forecast(self):
        self._run_test('CAISO', self.forecast_kwargs)

    def test_caiso_past(self):
        self._run_test('CAISO', self.past_kwargs)

    def test_caiso_past_str(self):
        self._run_test('CAISO', self.past_kwargs_str)

    def test_nyiso_latest(self):
        self._run_test('NYISO', self.latest_kwargs)

    def test_pjm_latest(self):
        self._run_test('PJM', self.latest_kwargs)

    def test_miso_forecast(self):
        self._run_test('MISO', self.forecast_kwargs)

