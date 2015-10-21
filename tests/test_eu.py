from pyiso import client_factory
from unittest import TestCase
from datetime import datetime
import mock
import pytz


class TestEU(TestCase):
    def setUp(self):
        self.c = client_factory('EU')

    def test_auth(self):
        self.assertTrue(self.c.auth())

    def test_auth_wrongemail(self):
        self.c.session = mock.MagicMock()
        response = mock.MagicMock()
        response.text = 'non_exists_user_or_bad_password'
        self.c.session.post.return_value = response
        r = self.c.auth()
        self.assertEqual(r, 'Wrong email or password')

    def test_auth_suspended(self):
        self.c.session = mock.MagicMock()
        response = mock.MagicMock()
        response.text = 'suspended_use'
        self.c.session.post.return_value = response
        r = self.c.auth()
        self.assertEqual(r, 'User is suspended')

    def test_auth_nothuman(self):
        self.c.session = mock.MagicMock()
        response = mock.MagicMock()
        response.text = 'not_human'
        self.c.session.post.return_value = response
        r = self.c.auth()
        self.assertEqual(r, 'This account is not allowed to access web portal')

    def test_auth_unknown(self):
        self.c.session = mock.MagicMock()
        response = mock.MagicMock()
        response.text = 'This error is not known'
        self.c.session.post.return_value = response
        r = self.c.auth()
        self.assertIn('Unknown error:', r)

    def test_throttled(self):
        self.c.session = mock.MagicMock()
        response = mock.MagicMock()
        response.text = ''
        self.c.session.get.return_value = response
        r = self.c.fetch_entsoe('url', 'payload')
        self.assertFalse(r)

    def test_unknownexception(self):
        self.c.session = mock.MagicMock()
        response = mock.MagicMock()
        response.text = 'UNKNOWN_EXCEPTION'
        self.c.session.get.return_value = response
        r = self.c.fetch_entsoe('url', 'payload')
        self.assertFalse(r)

    def test_bad_control_area(self):
        self.assertRaises(ValueError, self.c.get_load, 'not-a-cta', latest=True)

    def test_get_load(self):
        r = self.c.get_load('IT', start_at=datetime(2015, 9, 6, 0, tzinfo=pytz.utc),
                            end_at=datetime(2015, 9, 7, 0, tzinfo=pytz.utc))
        self.assertEqual(len(r), 24)

        self.assertEqual(r[12]['load_MW'], 26745)

    def test_get_forecast_load(self):
        r = self.c.get_load('IT', forecast=True,
                            start_at=datetime(2015, 9, 6, 0, tzinfo=pytz.utc),
                            end_at=datetime(2015, 9, 7, 0, tzinfo=pytz.utc),)
        self.assertEqual(len(r), 24)

        self.assertEqual(r[12]['load_MW'], 27780)

    def test_latest(self):
        r = self.c.get_load('IT', latest=True)
        self.assertEqual(len(r), 1)
        self.assertGreater(r[0]['load_MW'], 0)
