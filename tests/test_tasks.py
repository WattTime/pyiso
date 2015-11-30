from pyiso import client_factory, tasks
from unittest import TestCase
from datetime import datetime, timedelta
import pytz


class TestGenerationTask(TestCase):
    def setUp(self):
        self.latest_kwargs = {'latest': True}
        now = pytz.utc.localize(datetime.utcnow())
        self.forecast_kwargs = {'start_at': now + timedelta(minutes=20),
                                'end_at': now + timedelta(days=1)}

    def _run_test(self, ba, kwargs):
        expected = client_factory(ba).get_generation(**kwargs)
        received = tasks.get_generation(ba, **kwargs)
        for i in range(len(expected)):
            if expected[i]['timestamp'] == received[i]['timestamp']:
                self.assertEqual(expected[i]['gen_MW'], received[i]['gen_MW'])
                self.assertEqual(expected[i]['fuel_name'], received[i]['fuel_name'])

    def test_bpa_latest(self):
        self._run_test('BPA', self.latest_kwargs)

    def test_caiso_latest(self):
        self._run_test('CAISO', self.latest_kwargs)

    def test_caiso_forecast(self):
        self._run_test('CAISO', self.forecast_kwargs)

    def test_ercot_latest(self):
        self._run_test('ERCOT', self.latest_kwargs)

    def test_isone_latest(self):
        self._run_test('ISONE', self.latest_kwargs)

    def test_miso_latest(self):
        self._run_test('MISO', self.latest_kwargs)

    def test_pjm_latest(self):
        self._run_test('PJM', self.latest_kwargs)

    def test_sveri_latest(self):
        self._run_test('AZPS', self.latest_kwargs)


class TestLoadTask(TestCase):
    def setUp(self):
        self.latest_kwargs = {'latest': True}
        now = pytz.utc.localize(datetime.utcnow())
        self.forecast_kwargs = {'start_at': now + timedelta(minutes=20), 'end_at': now + timedelta(days=1)}

    def _run_test(self, ba, kwargs):
        expected = client_factory(ba).get_load(**kwargs)
        received = tasks.get_load(ba, **kwargs)
        for i in range(len(expected)):
            if expected[i]['timestamp'] == received[i]['timestamp']:
                self.assertEqual(expected[i]['load_MW'], received[i]['load_MW'])

    def test_bpa_latest(self):
        self._run_test('BPA', self.latest_kwargs)

    def test_caiso_latest(self):
        self._run_test('CAISO', self.latest_kwargs)

    def test_caiso_forecast(self):
        self._run_test('CAISO', self.forecast_kwargs)

    def test_isone_latest(self):
        self._run_test('ISONE', self.latest_kwargs)

    def test_isone_forecast(self):
        self._run_test('ISONE', self.forecast_kwargs)

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

    def test_sveri_latest(self):
        self._run_test('AZPS', self.latest_kwargs)


class TestTradeTask(TestCase):
    def setUp(self):
        self.latest_kwargs = {'latest': True}
        now = pytz.utc.localize(datetime.utcnow())
        self.forecast_kwargs = {'start_at': now + timedelta(minutes=20), 'end_at': now + timedelta(days=1)}

    def _run_test(self, ba, kwargs):
        expected = client_factory(ba).get_trade(**kwargs)
        received = tasks.get_trade(ba, **kwargs)
        for i in range(len(expected)):
            if expected[i]['timestamp'] == received[i]['timestamp']:
                self.assertEqual(expected[i]['net_exp_MW'], received[i]['net_exp_MW'])

    def test_caiso_latest(self):
        self._run_test('CAISO', self.latest_kwargs)

    def test_caiso_forecast(self):
        self._run_test('CAISO', self.forecast_kwargs)

    def test_nyiso_latest(self):
        self._run_test('NYISO', self.latest_kwargs)

    def test_miso_forecast(self):
        self._run_test('MISO', self.forecast_kwargs)
