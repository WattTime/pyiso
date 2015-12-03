from pyiso.base import BaseClient
import numpy as np
import pandas as pd
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

        # return
        return ts

    def utcify_index(self, *args, **kwargs):
        # regular utcify
        idx = super(NYISOClient, self).utcify_index(*args, **kwargs)

        # timestamp is end of interval
        freq = self.options.get('freq', self.FREQUENCY_CHOICES.fivemin)
        if freq == self.FREQUENCY_CHOICES.fivemin:
            idx -= timedelta(minutes=5)

        # return
        return idx

    def get_load(self, latest=False, start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='load', latest=latest,
                            start_at=start_at, end_at=end_at, **kwargs)

        # get data
        if self.options['forecast']:
            # always include today
            dates_list = self.dates() + [self.local_now().date()]

            # get data
            df = self.get_any('isolf', self.parse_load_forecast, dates_list=dates_list)
            extras = {
                'ba_name': self.NAME,
                'freq': self.FREQUENCY_CHOICES.hourly,
                'market': self.MARKET_CHOICES.dam,
            }
        else:
            # get data
            df = self.get_any('pal', self.parse_load_rtm)
            extras = {
                'ba_name': self.NAME,
                'freq': self.FREQUENCY_CHOICES.fivemin,
                'market': self.MARKET_CHOICES.fivemin,
            }

        # serialize and return
        return self.serialize_faster(df, extras=extras)

    def get_trade(self, latest=False, start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='trade', latest=latest,
                            start_at=start_at, end_at=end_at, **kwargs)

        # get data
        df = self.get_any('ExternalLimitsFlows', self.parse_trade)
        extras = {
            'ba_name': self.NAME,
            'freq': self.FREQUENCY_CHOICES.fivemin,
            'market': self.MARKET_CHOICES.fivemin,
        }

        # serialize and return
        return self.serialize_faster(df, extras=extras)

    def get_any(self, label, parser, dates_list=None):
        # set up storage
        pieces = []

        # get dates
        if not dates_list:
            dates_list = self.dates()

        # fetch and parse all csvs
        for date in dates_list:
            content = self.fetch_csv(date, label)
            try:
                pieces.append(parser(content))
            except AttributeError:
                pass

        # combine and slice
        df = pd.concat(pieces)
        sliced = self.slice_times(df)

        # return
        return sliced

    def fetch_csv(self, date, label):
        # construct url
        datestr = date.strftime('%Y%m%d')
        url = '%s/%s/%s%s.csv' % (self.base_url, label, datestr, label)

        # make request
        result = self.request(url)

        # return content
        return result.text

    def parse_load_rtm(self, content):
        # parse csv to df
        df = self.parse_to_df(content)

        # total load grouped by timestamp
        try:
            total_loads = df.groupby('Time Stamp').aggregate(np.sum)
        except KeyError:
            raise ValueError('Could not parse content:\n%s' % content)

        # set index
        total_loads['timestamp'] = total_loads.index.map(pd.to_datetime)
        total_loads.set_index('timestamp', inplace=True)
        total_loads.index = self.utcify_index(total_loads.index)
        total_loads.index.set_names(['timestamp'], inplace=True)

        # pull out column
        series = total_loads['Load']
        final_df = pd.DataFrame({'load_MW': series})

        # return
        return final_df

    def parse_load_forecast(self, content):
        # parse csv to df
        df = self.parse_to_df(content, index_col=0, header=0, parse_dates=True)

        # set index
        df.index.name = 'timestamp'
        df.index = self.utcify_index(df.index)

        # pull out column
        final_df = pd.DataFrame({'load_MW': df['NYISO']})

        # return
        return final_df

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
            'SCH - HQ - NY', 'SCH - HQ_CEDARS', 'SCH - HQ_IMPORT_EXPORT',  # HQ
            'SCH - NE - NY', 'SCH - NPX_1385', 'SCH - NPX_CSC',  # ISONE
            'SCH - OH - NY',  # Ontario
            'SCH - PJ - NY', 'SCH - PJM_HTP', 'SCH - PJM_NEPTUNE', 'SCH - PJM_VFT',  # PJM
        ]
        subsetted = pivoted[interfaces].copy()

        # set index
        subsetted['timestamp'] = subsetted.index.map(pd.to_datetime)
        subsetted.set_index('timestamp', inplace=True)
        subsetted.index = self.utcify_index(subsetted.index)
        subsetted.index.set_names(['timestamp'], inplace=True)

        # sum up
        cleaned = subsetted.dropna(axis=0)
        series = cleaned.apply(lambda x: -1*np.sum(x), axis=1)
        final_df = pd.DataFrame({'net_exp_MW': series})

        # return
        return final_df
