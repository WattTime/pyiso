from pyiso.base import BaseClient
from pyiso import LOGGER
import pandas as pd
from io import StringIO
import pytz


class MISOClient(BaseClient):
    NAME = 'MISO'

    base_url = 'https://www.misoenergy.org/ria/'

    fuels = {
        'Coal': 'coal',
        'Natural Gas': 'natgas',
        'Nuclear': 'nuclear',
        'Other': 'other',
        'Wind': 'wind',
    }

    TZ_NAME = 'America/New_York'

    def utcify(self, local_ts, **kwargs):
        # MISO is always on Eastern Standard Time, even during DST
        # ie UTC offset = -5 always
        utc_ts = super(MISOClient, self).utcify(local_ts, is_dst=False)
        utc_ts += utc_ts.astimezone(pytz.timezone(self.TZ_NAME)).dst()  # adjust for EST
        return utc_ts

    def get_generation(self, latest=False, **kwargs):
        # set args
        self.handle_options(data='gen', latest=latest, **kwargs)

        # get data
        if self.options['latest']:
            data = self.latest_fuel_mix()
            extras = {
                'ba_name': self.NAME,
                'market': self.MARKET_CHOICES.fivemin,
                'freq': self.FREQUENCY_CHOICES.fivemin,
            }
        else:
            raise ValueError('latest must be True')

        # return
        return self.serialize_faster(data, extras=extras)

    def latest_fuel_mix(self):
        # set up request
        url = self.base_url + 'FuelMix.aspx?CSV=True'

        # carry out request
        response = self.request(url)
        if not response:
            return pd.DataFrame()

        # test for valid content
        if 'The page cannot be displayed' in response.text:
            LOGGER.error('MISO: Error in source data for generation')
            return pd.DataFrame()

        # preliminary parsing
        df = pd.read_csv(StringIO(response.text), header=0, index_col=0, parse_dates=True)

        # set index
        df.index = self.utcify_index(df.index)
        df.index.set_names(['timestamp'], inplace=True)

        # set names and labels
        df['fuel_name'] = df.apply(lambda x: self.fuels[x['CATEGORY']], axis=1)
        df['gen_MW'] = df['ACT']

        # return
        return df[['fuel_name', 'gen_MW']]
