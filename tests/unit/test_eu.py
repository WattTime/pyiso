from os import environ
from pyiso import client_factory
from unittest import TestCase
import mock
from mock import patch


class TestEU(TestCase):
    def setUp(self):
        environ['ENTSOe_USERNAME'] = 'test'
        environ['ENTSOe_PASSWORD'] = 'test'
        self.c = client_factory('EU')

    def test_auth(self):
        self.c.session = mock.MagicMock()
        response = mock.MagicMock()
        response.text = 'ok'
        self.c.session.post.return_value = response
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

    @patch('time.sleep', return_value=True)
    def test_throttled(self, patched_time_sleep):
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
