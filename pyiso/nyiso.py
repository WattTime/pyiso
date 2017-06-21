from pyiso.base import BaseClient
from pyiso import LOGGER
import numpy as np
import pandas as pd
from datetime import timedelta
import re


class NYISOClient(BaseClient):
    NAME = 'NYISO'

    base_url = 'http://mis.nyiso.com/public/csv'

    TZ_NAME = 'America/New_York'

    fuel_names = {
        'Other Fossil Fuels': 'fossil',  # coal or oil
        'Other Renewables': 'renewable',  # solar, methane, refuse, wood
        'Hydro': 'hydro',  # including pumped storage
        'Nuclear': 'nuclear',
        'Natural Gas': 'natgas',  # not including dual fuel
        'Wind': 'wind',
        'Dual Fuel': 'dual',  # nat gas and/or other fossil
    }

    def utcify(self, *args, **kwargs):
        # regular utcify
        ts = super(NYISOClient, self).utcify(*args, **kwargs)

        # timestamp is end of interval
        freq = self.options.get('freq', self.FREQUENCY_CHOICES.fivemin)
        if freq == self.FREQUENCY_CHOICES.fivemin and self.options['data'] != 'lmp':
            ts -= timedelta(minutes=5)

        # return
        return ts

    def utcify_index(self, *args, **kwargs):
        # regular utcify
        idx = super(NYISOClient, self).utcify_index(*args, **kwargs)

        # timestamp is end of interval
        freq = self.options.get('freq', self.FREQUENCY_CHOICES.fivemin)
        if freq == self.FREQUENCY_CHOICES.fivemin and self.options['data'] != 'lmp':
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

    def get_generation(self, latest=False, start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='gen', latest=latest,
                            start_at=start_at, end_at=end_at, **kwargs)

        # get data
        df = self.get_any('rtfuelmix', self.parse_genmix)
        extras = {
            'ba_name': self.NAME,
            'freq': self.FREQUENCY_CHOICES.fivemin,
            'market': self.MARKET_CHOICES.fivemin,
        }

        # serialize and return
        return self.serialize_faster(df, extras=extras)

    def get_lmp(self, node_id='CENTRL', latest=False, start_at=False, end_at=False, **kwargs):
        # node CENTRL is relatively central and seems to have low congestion costs
        if node_id and not isinstance(node_id, list):
            node_id = [node_id]
        self.handle_options(data='lmp', latest=latest, node_id=node_id,
                            start_at=start_at, end_at=end_at, **kwargs)

        # get data
        if self.options['forecast'] or self.options.get('market', None) == self.MARKET_CHOICES.dam:
            # always include today
            dates_list = self.dates() + [self.local_now().date()]

            # get data
            df = self.get_any('damlbmp', self.parse_lmp, dates_list=dates_list)
            extras = {
                'ba_name': self.NAME,
                'freq': self.FREQUENCY_CHOICES.hourly,
                'market': self.MARKET_CHOICES.dam,
            }
        else:
            # get data
            df = self.get_any('realtime', self.parse_lmp)
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
            for csv in self.fetch_csvs(date, label):

                try:
                    pieces.append(parser(csv))
                except AttributeError:
                    pass

            # if fetch_csvs cannot get the individual days, it gets the whole month
            # Shortcut the loop if any call to fetch_csvs gets all dates in dates_list
            try:
                if (pieces[-1].index[-1] - timedelta(days=1)).date() > max(dates_list):
                    break
            except IndexError:
                pass

        # combine pieces
        if len(pieces) > 0:
            df = pd.concat(pieces)
        else:
            return pd.DataFrame()

        # genmix may have repeated times, so dedup
        if 'fuel_name' in df.columns:
            # can't drop dups on index, only columns
            df['dummy_timestamp'] = df.index
            df.drop_duplicates(subset=['dummy_timestamp', 'fuel_name'], inplace=True, keep='last')
            del df['dummy_timestamp']

        # slice and return
        sliced = self.slice_times(df)
        return sliced

    def fetch_csvs(self, date, label):
        # construct url
        datestr = date.strftime('%Y%m%d')
        if self.options['data'] == 'lmp':
            url = '%s/%s/%s%s_zone.csv' % (self.base_url, label, datestr, label)
        else:
            url = '%s/%s/%s%s.csv' % (self.base_url, label, datestr, label)

        # make request
        response = self.request(url)

        # if 200, return
        if response and response.status_code == 200:
            return [response.text]

        # if failure, try zipped monthly data
        datestr = date.strftime('%Y%m01')
        if self.options['data'] == 'lmp':
            url = '%s/%s/%s%s_zone_csv.zip' % (self.base_url, label, datestr, label)
        else:
            url = '%s/%s/%s%s_csv.zip' % (self.base_url, label, datestr, label)

        # make request and unzip
        response_zipped = self.request(url)
        if response_zipped:
            unzipped = self.unzip(response_zipped.content)
        else:
            return []

        # return
        if unzipped:
            LOGGER.info('Failed to find daily %s data for %s but found monthly data, using that' % (self.options['data'], date))
            return unzipped
        else:
            return []

    def parse_load_rtm(self, content):
        # parse csv to df
        df = self.parse_to_df(content, header=0, index_col=0, parse_dates=True)

        # set index
        df.index = self.utcify_index(df.index, tz_col=df['Time Zone'])
        df['timestamp'] = df.index

        # total load grouped by timestamp
        try:
            total_loads = df.groupby('timestamp').aggregate(np.sum)
        except KeyError:
            raise ValueError('Could not parse content:\n%s' % str(content))

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

    def parse_genmix(self, content):
        # parse csv to df
        df = self.parse_to_df(content, header=0, index_col=0, parse_dates=True)

        # set index
        df.index = self.utcify_index(df.index, tz_col=df['Time Zone'])
        df.index.name = 'timestamp'

        # convert fuel names
        df['fuel_name'] = df.apply(lambda x: self.fuel_names[x['Fuel Category']],
                                   axis=1)

        # assemble final
        final_df = pd.DataFrame({'gen_MW': df['Gen MWh'], 'fuel_name': df['fuel_name']})

        # return
        return final_df

    def parse_lmp(self, content):
        # parse csv to df
        df = self.parse_to_df(content, header=0, index_col=0, parse_dates=True)

        # set index
        df.index.name = 'timestamp'
        df.index = self.utcify_index(df.index)

        # if latest, throw out 15 min predicted data
        if self.options['latest']:
            df = df.truncate(after=self.local_now())

        rename_d = {'LBMP ($/MWHr)': 'lmp',
                    'Name': 'node_id'}
        df.rename(columns=rename_d, inplace=True)
        df['lmp_type'] = 'energy'

        df.drop([u'PTID', u'Marginal Cost Losses ($/MWHr)'], axis=1, inplace=True)
        try:
            df.drop(u'Marginal Cost Congestion ($/MWHr)', axis=1, inplace=True)
        except ValueError:
            df.drop(u'Marginal Cost Congestion ($/MWH', axis=1, inplace=True)

        # strip out unwanted nodes
        node_id = self.options['node_id']
        if node_id:
            reg = re.compile('|'.join(node_id))
            df = df.ix[df['node_id'].str.contains(reg)]
        # return
        return df
