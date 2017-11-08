import os

from pyiso import client_factory
from unittest import TestCase
from datetime import time, datetime, timedelta
import pytz

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), '../fixtures/sveri')


class TestSVERI(TestCase):
    def setUp(self):
        self.c = client_factory('AZPS')
        self.TZ_NAME = 'America/Phoenix'

        self.now = datetime.now(pytz.timezone(self.TZ_NAME))
        self.today = datetime.combine(self.now.date(), time())
        self.yesterday = self.today - timedelta(days=1)
        self.tomorrow = self.today + timedelta(days=1)
        self.last_month = self.today - timedelta(days=32)
        self.next_month = self.today + timedelta(days=32)

        self.sample = open(FIXTURES_DIR + '/api_response.csv').read().encode('utf8')
        self.sample_start = pytz.timezone(self.TZ_NAME).localize(datetime(2015, 7, 18, 0, 0))
        self.sample_end = pytz.timezone(self.TZ_NAME).localize(datetime(2015, 7, 19, 0, 0))

    def test_parse_to_df(self):
        df = self.c.parse_to_df(self.sample, header=0, parse_dates=True, date_parser=self.c.date_parser, index_col=0)
        self.assertEquals(df.index.name, "Time (MST)")
        self.assertEquals(df.index.dtype, 'datetime64[ns]')
        headers = ["Solar Aggregate (MW)", "Wind Aggregate (MW)", "Other Renewables Aggregate (MW)", "Hydro Aggregate (MW)"]
        self.assertEquals(list(df.columns.values), headers)
        self.assertEquals(len(df.index), len(self.sample.splitlines()) - 1)

    def test_clean_df_gen(self):
        self.c.handle_options(data='gen', latest=False, yesterday=False,
                              start_at=self.sample_start, end_at=self.sample_end)
        parsed_df = self.c.parse_to_df(self.sample, header=0, parse_dates=True, date_parser=self.c.date_parser, index_col=0)
        cleaned_df = self.c.clean_df(parsed_df)
        headers = ["fuel_name", "gen_MW"]
        self.assertEquals(list(cleaned_df.columns.values), headers)
        self.assertTrue((cleaned_df.index == self.c.utcify(parsed_df.index[0])).all())
        self.assertEquals(cleaned_df['fuel_name'].tolist(), ['solar', 'wind', 'renewable', 'hydro'])
        self.assertEquals(cleaned_df['gen_MW'].tolist(), parsed_df.ix[0].tolist())

    def test_serialize(self):
        self.c.handle_options(data='gen', latest=False, yesterday=False,
                              start_at=self.sample_start, end_at=self.sample_end)
        parsed_df = self.c.parse_to_df(self.sample, header=0, parse_dates=True, date_parser=self.c.date_parser, index_col=0)
        cleaned_df = self.c.clean_df(parsed_df)
        extras = {
            'ba_name': self.c.NAME,
            'market': self.c.MARKET_CHOICES.fivemin,
            'freq': self.c.FREQUENCY_CHOICES.fivemin
        }
        result = self.c.serialize_faster(cleaned_df, extras)
        for index, dp in enumerate(result):
            self.assertEquals(dp['fuel_name'], cleaned_df['fuel_name'].tolist()[index])
            self.assertEquals(dp['gen_MW'], cleaned_df['gen_MW'].tolist()[index])
            self.assertEquals(dp['timestamp'], cleaned_df.index.tolist()[index])
            self.assertEquals(dp['ba_name'], extras['ba_name'])
            self.assertEquals(dp['market'], extras['market'])
            self.assertEquals(dp['freq'], extras['freq'])

    def test_get_payloads_latest(self):
        # latest
        self.c.handle_options(data='gen', latest=True, yesterday=False,
                              start_at=False, end_at=False)
        start = self.today.strftime('%Y-%m-%d')
        end = self.tomorrow.strftime('%Y-%m-%d')
        result = {  # gen
            'ids': '1,2,3,4',
            'startDate': start,
            'endDate': end,
            'saveData': 'true'
        }
        result2 = result.copy()  # gen
        result2['ids'] = '5,6,7,8'
        self.assertEquals(self.c.get_gen_payloads(), (result, result2))

        result['ids'] = '0'  # load
        self.assertEquals(self.c.get_load_payload(), result)

    def test_get_payloads(self):
        # variable start/end
        self.c.handle_options(data='gen', latest=True, yesterday=False,
                              start_at=self.yesterday, end_at=self.today)
        start = self.yesterday.strftime('%Y-%m-%d')
        end = self.today.strftime('%Y-%m-%d')
        result = {  # gen
            'ids': '1,2,3,4',
            'startDate': start,
            'endDate': end,
            'saveData': 'true'
        }
        result2 = result.copy()  # gen
        result2['ids'] = '5,6,7,8'
        self.assertEquals(self.c.get_gen_payloads(), (result, result2))

        result['ids'] = '0'  # load
        self.assertEquals(self.c.get_load_payload(), result)
