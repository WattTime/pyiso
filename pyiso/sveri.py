from pyiso.base import BaseClient
from datetime import datetime, timedelta
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
    BASE_URL = 'https://sveri.uaren.org/api?'

    fuels = {
        'Solar Aggregate (MW)': 'solar',
        'Wind Aggregate (MW)': 'wind',
        'Other Renewables Aggregate (MW)': 'renewables',
        'Hydro Aggregate (MW)': 'hydro',
        'Coal Aggregate (MW)': 'coal',
        'Gas Aggregate (MW)': 'natgas',
        'Other Fossil Fuels Aggregate (MW)': 'other',
        'Nuclear Aggregate (MW)': 'nuclear',
    }

    def _get_payload(self, ids):
        if self.options['latest']:
            now = datetime.now(pytz.timezone(self.TZ_NAME))
            start = now.strftime('%Y-%m-%d')
            end = (now + timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            start = self.options['start_at'].astimezone(pytz.timezone(self.TZ_NAME)).strftime('%Y-%m-%d')
            end = self.options['end_at'].strftime('%Y-%m-%d')
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

    def _get_gen_dps_from_row(self, row):
        dps = []
        for f in self.fuels:
            dp = {
                'timestamp': self.utcify(row[0]),
                'ba_name': self.NAME,
                'freq': self.FREQUENCY_CHOICES.onemin,
                'market': self.MARKET_CHOICES.onemin,
                'fuel_name': self.fuels[f],
                'gen_MW': row.loc[f],
            }
            dps.append(dp)
        return dps

    def _get_load_dp_from_row(self, row):
        header = 'Load Aggregate (MW)'
        dp = {
            'timestamp': self.utcify(row[0]),
            'ba_name': self.NAME,
            'freq': self.FREQUENCY_CHOICES.onemin,
            'market': self.MARKET_CHOICES.onemin,
            'load_MW': row.loc[header],
        }
        return [dp]

    def get_generation(self, latest=False, yesterday=False,
                       start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='gen', latest=latest, yesterday=yesterday,
                            start_at=start_at, end_at=end_at, **kwargs)
        if not self.options['latest'] and self.options['start_at'] > pytz.utc.localize(datetime.utcnow()):
            self.logger.warn("SVERI does not have forecast data. There will be no data for the chosen time frame.")
        payloads = self.get_gen_payloads()
        response = self.request(self.BASE_URL, params=payloads[0])
        response2 = self.request(self.BASE_URL, params=payloads[1])
        result = []
        if response and response2:
            df = self.parse_to_df(response.content, header=0)
            df2 = self.parse_to_df(response2.content, header=0)
            df = pd.merge(df, df2, on=df.columns[0])
            if self.options['latest']:
                df = df.tail(1)
            for index, row in df.iterrows():
                if self.options['latest'] or row[0][-3:] == ':05':
                    result += self._get_gen_dps_from_row(row)
        return result

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='gen', latest=latest, yesterday=yesterday,
                            start_at=start_at, end_at=end_at, **kwargs)
        if not self.options['latest'] and self.options['start_at'] > pytz.utc.localize(datetime.utcnow()):
            self.logger.warn("SVERI does not have forecast data. There will be no data for the chosen time frame.")
        payload = self.get_load_payload()
        response = self.request(self.BASE_URL, params=payload)
        result = []
        if response:
            df = self.parse_to_df(response.content, header=0)
            if self.options['latest']:
                df = df.tail(1)
            for index, row in df.iterrows():
                if self.options['latest'] or row[0][-3:] == ':05':
                    result += self._get_load_dp_from_row(row)
        return result
