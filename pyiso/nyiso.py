import numpy as np
from pyiso.base import BaseClient
from datetime import timedelta


class NYISOClient(BaseClient):
    NAME = 'NYISO'
    
    base_url = 'http://mis.nyiso.com/public/csv'

    TZ_NAME = 'America/New_York'

    def utcify(self, *args, **kwargs):
        # regular utcify
        ts = super(NYISOClient, self).utcify(*args, **kwargs)

        # timestamp is end of interval
        freq = self.options.get('freq', self.FREQUENCY_CHOICES.fivemin)
        if freq == self.FREQUENCY_CHOICES.fivemin:
            ts -= timedelta(minutes=5)
        else:
            raise ValueError('Not sure whether this freq is allowed for')

        # return
        return ts

    def get_load(self, latest=False, start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='load', latest=latest,
                            start_at=start_at, end_at=end_at, **kwargs)

        # get data
        return self.get_any('pal', self.parse_load)

    def get_trade(self, latest=False, start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='trade', latest=latest,
                            start_at=start_at, end_at=end_at, **kwargs)

        # get data
        return self.get_any('ExternalLimitsFlows', self.parse_trade)

    def get_any(self, label, parser):
        # set up storage
        data = []

        # fetch and parse all csvs
        for date in self.dates():
            content = self.fetch_csv(date, label)
            data += parser(content)

        # handle latest
        if self.options.get('latest', False):
            latest_ts = max([d['timestamp'] for d in data])
            latest_data = [d for d in data if d['timestamp']==latest_ts]
            return latest_data

        # handle sliceable
        else:
            data_to_return = []
            for dp in data:
                if (dp['timestamp'] <= self.options['end_at']) and (dp['timestamp'] >= self.options['start_at']):
                    data_to_return.append(dp)
            return data_to_return

    def fetch_csv(self, date, label):
        # construct url
        datestr = date.strftime('%Y%m%d')
        url = '%s/%s/%s%s.csv' % (self.base_url, label, datestr, label)

        # make request
        result = self.request(url)

        # return content
        return result.text

    def parse_load(self, content):
        # parse csv to df
        df = self.parse_to_df(content)

        # total load grouped by timestamp
        try:
            total_loads = df.groupby('Time Stamp').aggregate(np.sum)
        except KeyError:
            raise ValueError('Could not parse content:\n%s' % content)

        # collect options
        freq = self.options.get('freq', self.FREQUENCY_CHOICES.fivemin)
        market = self.options.get('market', self.MARKET_CHOICES.fivemin)
        base_dp = {
                'freq': freq,
                'market': market,
                'ba_name': self.NAME,
        }

        # serialize
        data = []
        for idx, row in total_loads.iterrows():
            dp = {
                'timestamp': self.utcify(idx),
                'load_MW': row[1]
            }
            dp.update(base_dp)
            data.append(dp)

        # return
        return data

    def parse_trade(self, content):
        # parse csv to df
        df = self.parse_to_df(content)
        try:
            df.drop_duplicates(['Timestamp', 'Interface Name'], inplace=True)
        except KeyError:
            raise ValueError('Could not parse content:\n%s' % content)

        # pivot
        pivoted = df.pivot(index='Timestamp', columns='Interface Name', values='Flow (MWH)')

        # only keep flows across external interfaces
        interfaces = [
            'SCH - HQ - NY', 'SCH - HQ_CEDARS', 'SCH - HQ_IMPORT_EXPORT', # HQ
            'SCH - NE - NY', 'SCH - NPX_1385', 'SCH - NPX_CSC', # ISONE
            'SCH - OH - NY', # Ontario
            'SCH - PJ - NY', 'SCH - PJM_HTP', 'SCH - PJM_NEPTUNE', 'SCH - PJM_VFT', # PJM
        ]
        subsetted = pivoted[interfaces]

        # collect options
        freq = self.options.get('freq', self.FREQUENCY_CHOICES.fivemin)
        market = self.options.get('market', self.MARKET_CHOICES.fivemin)
        base_dp = {
                'freq': freq,
                'market': market,
                'ba_name': self.NAME,
        }

        # serialize
        data = []
        for idx, row in subsetted.iterrows():
            # imports are positive, exports are negative
            net_imp = row.sum()

            dp = {
                'timestamp': self.utcify(idx),
                'net_exp_MW': -net_imp,
            }
            dp.update(base_dp)
            data.append(dp)

        # return
        return data
