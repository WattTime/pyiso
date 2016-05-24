from pyiso.base import BaseClient
from pyiso import LOGGER
from datetime import datetime, timedelta
from dateutil.parser import parse as dateutil_parse
import pandas as pd
import pytz


class SVERIClient(BaseClient):
    """
    Interface to SVERI data sources.

    For information about the data sources,
    see https://sveri.uaren.org/#howto
    """
    NAME = 'SVERI'
    TZ_NAME = 'America/Phoenix'
    BASE_URL = 'https://sveri.energy.arizona.edu/api?'

    fuels = {
        'Solar Aggregate (MW)': 'solar',
        'Wind Aggregate (MW)': 'wind',
        'Other Renewables Aggregate (MW)': 'renewable',
        'Hydro Aggregate (MW)': 'hydro',
        'Coal Aggregate (MW)': 'coal',
        'Gas Aggregate (MW)': 'natgas',
        'Other Fossil Fuels Aggregate (MW)': 'fossil',
        'Nuclear Aggregate (MW)': 'nuclear',
    }

    def _get_payload(self, ids):
        if self.options['latest']:
            now = datetime.now(pytz.timezone(self.TZ_NAME))
            start = now.strftime('%Y-%m-%d')
            end = (now + timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            start = self.options['start_at'].astimezone(pytz.timezone(self.TZ_NAME)).strftime('%Y-%m-%d')
            end = self.options['end_at'].astimezone(pytz.timezone(self.TZ_NAME)).strftime('%Y-%m-%d')
        return {
            'ids': ids,
            'startDate': start,
            'endDate': end,
            'saveData': 'true'
        }

    def get_gen_payloads(self):
        p1 = self._get_payload('1,2,3,4')
        p2 = self._get_payload('5,6,7,8')
        return (p1, p2)

    def get_load_payload(self):
        return self._get_payload('0')

    def clean_df(self, df):
        # take only data at 5 minute marks
        df = df[df.index.second == 5]
        df = df[df.index.minute % 5 == 0]
        # unpivot and rename
        if self.options['data'] == 'gen':
            df.rename(columns=self.fuels, inplace=True)
            df = self.unpivot(df)
            df.rename(columns={'level_1': 'fuel_name', 0: 'gen_MW'}, inplace=True)
        else:
            df.rename(columns={"Load Aggregate (MW)": "load_MW"}, inplace=True)

        df.index.names = ['timestamp']

        # change timestamps to utc and slice
        df.index = self.utcify_index(df.index)
        sliced = self.slice_times(df)
        return sliced

    def _clean_and_serialize(self, df):
        # if no data, nothing to do
        if len(df) == 0:
            return []

        # clean
        cleaned_df = self.clean_df(df)

        # serialize
        extras = {
            'ba_name': self.NAME,
            'market': self.MARKET_CHOICES.fivemin,
            'freq': self.FREQUENCY_CHOICES.fivemin
        }
        return self.serialize_faster(cleaned_df, extras)

    def get_generation(self, latest=False, yesterday=False,
                       start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='gen', latest=latest, yesterday=yesterday,
                            start_at=start_at, end_at=end_at, **kwargs)
        self.no_forecast_warn()

        # fetch data
        payloads = self.get_gen_payloads()
        response = self.request(self.BASE_URL, params=payloads[0])
        response2 = self.request(self.BASE_URL, params=payloads[1])
        if not response or not response2:
            return []

        if response.text == 'Invalid ids string.' or response2.text == 'Invalid ids string':
            return []

        # parse
        df = self.parse_to_df(response.content, header=0,
                              parse_dates=True, date_parser=self.date_parser, index_col=0)
        df2 = self.parse_to_df(response2.content, header=0,
                               parse_dates=True, date_parser=self.date_parser, index_col=0)
        df = pd.concat([df, df2], axis=1, join='inner')

        # clean and serialize
        return self._clean_and_serialize(df)

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='load', latest=latest, yesterday=yesterday,
                            start_at=start_at, end_at=end_at, **kwargs)
        self.no_forecast_warn()

        # fetch data
        payload = self.get_load_payload()
        response = self.request(self.BASE_URL, params=payload)
        if not response:
            return []

        # parse
        df = self.parse_to_df(response.content, header=0, parse_dates=True, date_parser=self.date_parser, index_col=0)

        # clean and serialize
        return self._clean_and_serialize(df)

    def date_parser(self, ts_str):
        TZINFOS = {
            'MST': pytz.timezone(self.TZ_NAME),
        }

        return dateutil_parse(ts_str, tzinfos=TZINFOS)

    def no_forecast_warn(self):
        if not self.options['latest'] and self.options['start_at'] >= pytz.utc.localize(datetime.utcnow()):
            LOGGER.warn("SVERI does not have forecast data. There will be no data for the chosen time frame.")
