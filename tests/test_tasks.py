from pyiso import client_factory, tasks
from unittest import TestCase
from datetime import datetime, timedelta
import pytz


class TestGenerationTask(TestCase):
    def setUp(self):
        self.latest_kwargs = {'latest': True}

    def test_bpa_latest(self):
        expected = client_factory('BPA').get_generation(**self.latest_kwargs)
        received = tasks.get_generation('BPA', **self.latest_kwargs)
        self.assertEqual(expected, received)

    def test_caiso_latest(self):
        expected = client_factory('CAISO').get_generation(**self.latest_kwargs)
        received = tasks.get_generation('CAISO', **self.latest_kwargs)
        self.assertEqual(expected, received)

    def test_ercot_latest(self):
        expected = client_factory('ERCOT').get_generation(**self.latest_kwargs)
        received = tasks.get_generation('ERCOT', **self.latest_kwargs)
        self.assertEqual(expected, received)

    def test_isone_latest(self):
        expected = client_factory('ISONE').get_generation(**self.latest_kwargs)
        received = tasks.get_generation('ISONE', **self.latest_kwargs)
        self.assertEqual(expected, received)

    def test_miso_latest(self):
        expected = client_factory('MISO').get_generation(**self.latest_kwargs)
        received = tasks.get_generation('MISO', **self.latest_kwargs)
        self.assertEqual(expected, received)

    def test_pjm_latest(self):
        expected = client_factory('PJM').get_generation(**self.latest_kwargs)
        received = tasks.get_generation('PJM', **self.latest_kwargs)
        self.assertEqual(expected, received)


class TestLoadTask(TestCase):
    def setUp(self):
        self.latest_kwargs = {'latest': True}
        now = pytz.utc.localize(datetime.utcnow())
        self.forecast_kwargs = {'start_at': now + timedelta(minutes=20), 'end_at': now + timedelta(days=1)}

    def test_bpa_latest(self):
        expected = client_factory('BPA').get_load(**self.latest_kwargs)
        received = tasks.get_load('BPA', **self.latest_kwargs)
        self.assertEqual(expected, received)

    def test_caiso_latest(self):
        expected = client_factory('CAISO').get_load(**self.latest_kwargs)
        received = tasks.get_load('CAISO', **self.latest_kwargs)
        self.assertEqual(expected, received)

    def test_caiso_forecast(self):
        expected = client_factory('CAISO').get_load(**self.forecast_kwargs)
        received = tasks.get_load('CAISO', **self.forecast_kwargs)
        self.assertEqual(expected, received)

    def test_pjm_latest(self):
        expected = client_factory('PJM').get_load(**self.latest_kwargs)
        received = tasks.get_load('PJM', **self.latest_kwargs)
        self.assertEqual(expected, received)
