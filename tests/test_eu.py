from pyiso import client_factory
from unittest import TestCase
from datetime import datetime
import mock  # manually installed
import pytz
from pyiso.eu import example_properties



class TestEU(TestCase):
    def setUp(self):
        self.c = client_factory('EU')

    def test_payload(self):
        p = self.c.construct_payload(example_properties)
        self.assertIn('EnergyAccountReport', p)
        self.assertIn('TimeInterval', p)
        self.assertIn('2015-03-08 00:00:00/2015-03-09 00:00:00', p)
        self.assertIn('10YES-REE------0</', p)
        self.assertEqual(len(p), 1346)


    def test_temp_example(self):
        r = self.c.temp_example()
        print r
        
    def test_temp_get_load(self):
        r = self.c.temp_get_load(control_area='IT')
        print r

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
