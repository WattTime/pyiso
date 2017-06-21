from bs4 import BeautifulSoup
from pyiso.base import BaseClient
from pyiso import LOGGER
import pandas as pd
from io import StringIO
import re
from datetime import datetime, timedelta
import pytz


class ERCOTClient(BaseClient):
    NAME = 'ERCOT'
    base_report_url = 'http://mis.ercot.com'
    real_time_url = 'http://www.ercot.com/content/cdr/html/real_time_system_conditions.html'

    report_type_ids = {
        'wind_5min': '13071',
        'wind_hrly': '13028',
        'gen_hrly': '12358',
        'load_7day': '12311',
        'dam_hrly_lmp': '12331',
        'rt5m_lmp': '12300',
    }

    TZ_NAME = 'US/Central'

    def _request_report(self, report_type, date=None):
        # request reports list
        params = {'reportTypeId': self.report_type_ids[report_type]}
        response = self.request(self.base_report_url+'/misapp/GetReports.do',
                                params=params)
        if not response:
            raise ValueError('ERCOT: No report available for %s' % (report_type))
        report_list_soup = BeautifulSoup(response.content, 'lxml')

        # Round minute down to nearest 5 minute period
        if date:
            date = datetime(date.year, date.month, date.day, date.hour,
                            date.minute - (date.minute % 5), tzinfo=date.tzinfo)
            date = pytz.timezone(self.TZ_NAME).normalize(date)

            # DAM reports named 20150520 are for day 20150521
            if report_type == 'dam_hrly_lmp':
                date = date - timedelta(days=1)

        # find the endpoint to download
        report_endpoint = None
        for elt in report_list_soup.find_all('tr'):
            label = elt.find(class_='labelOptional_ind')
            if label and 'csv' in label.string:
                if date:
                    if label.string.split('.')[3] == date.strftime('%Y%m%d'):
                        # RT5M requires correct 5minute report
                        if report_type == 'rt5m_lmp':
                            if not label.string.split('.')[4].startswith(date.strftime('%H%M')):
                                continue
                    else:
                        continue
                report_endpoint = self.base_report_url + elt.a.attrs['href']
                break

        # test endpoint found
        if not report_endpoint:
            raise ValueError(
                'ERCOT: No report available for %s' % (report_type))

        # read report from zip
        r = self.request(report_endpoint)
        if r:
            content = self.unzip(r.content)
        else:
            return pd.DataFrame()

        # parse csv
        df = pd.read_csv(StringIO(content[0].decode('unicode_escape')))
        df.columns = [x.strip() for x in df.columns]
        df = df.dropna(axis=0)

        # return
        return df

    def is_dst(self, val, standard):
        return val != standard

    def get_generation(self, latest=False, **kwargs):
        # set args
        self.handle_options(data='gen', latest=latest, **kwargs)

        if self.options['latest']:
            # get latest load site
            response = self.request(self.real_time_url)

            # parse load from response
            if response:
                data = self.parse_rtm(response.text)
            else:
                data = []

        else:
            raise ValueError('Only latest genmix data available in ERCOT')

        # return
        return data

    def get_load(self, latest=False, **kwargs):
        # set args
        self.handle_options(data='load', latest=latest, **kwargs)

        if self.options['latest']:
            # get latest load site
            response = self.request(self.real_time_url)

            # parse load from response
            if response:
                data = self.parse_rtm(response.text)
            else:
                data = []

        elif self.options['forecast']:
            # get 7 day forecast load
            try:
                df = self._request_report('load_7day')
            except ValueError:
                return []

            # convert column of hour ending (1:00-24:00) to hour beginning (0:00-23:00)
            df['HourBeginning'] = df.apply(lambda dp: int(dp['HourEnding'].split(':')[0])-1,
                                           axis=1)

            # create datetime index of hour beginning
            df.index = df.apply(lambda dp: self.utcify(pd.to_datetime('%s %d:00' % (dp['DeliveryDate'], dp['HourBeginning'])),
                                                       is_dst=self.is_dst(dp['DSTFlag'], 'N')),
                                axis=1)

            # slice times
            sliced = self.slice_times(df)

            # pull out total load series
            series = sliced['SystemTotal']
            series.name = 'load_MW'
            series.index.set_names(['timestamp'], inplace=True)

            # slice and format
            extras = {
                'ba_name': self.NAME,
                'market': self.MARKET_CHOICES.dam,
                'freq': self.FREQUENCY_CHOICES.hourly,
            }
            data = self.serialize_faster(series, extras=extras)

        else:
            raise ValueError('Load only available for latest or forecast in ERCOT')

        # return
        return data

    def parse_rtm(self, content):
        # make soup
        soup = BeautifulSoup(content, 'lxml')

        # timestamp text starts with 'Last Updated'
        timestamp_elt = soup.find(text=re.compile('Last Updated'))
        timestamp_str = timestamp_elt.strip('Last Updated: ')
        timestamp = self.utcify(timestamp_str)

        # parse rest of page as html table
        df = pd.read_html(content, index_col=0)[0]

        # get other values
        load_val = df.loc['Actual System Demand'][1]
        wind_val = df.loc['Total Wind Output'][1]
        tie_flow_labels = ['DC_E (East)', 'DC_L (Laredo VFT)', 'DC_N (North)',
                           'DC_R (Railroad)', 'DC_S (Eagle Pass)']
        total_imports_val = sum([df.loc[label][1] for label in tie_flow_labels])

        # use options to get labels
        if self.options['data'] == 'load':
            data = [
                {
                    'timestamp': timestamp,
                    'ba_name': self.NAME,
                    'market': self.options.get('market', self.MARKET_CHOICES.fivemin),
                    'freq': self.options.get('freq', self.FREQUENCY_CHOICES.fivemin),
                    'load_MW': load_val,
                },
            ]
        elif self.options['data'] == 'gen':
            data = [
                {
                    'timestamp': timestamp,
                    'ba_name': self.NAME,
                    'market': self.options.get('market', self.MARKET_CHOICES.fivemin),
                    'freq': self.options.get('freq', self.FREQUENCY_CHOICES.fivemin),
                    'fuel_name': 'wind',
                    'gen_MW': wind_val,
                },
                {
                    'timestamp': timestamp,
                    'ba_name': self.NAME,
                    'market': self.options.get('market', self.MARKET_CHOICES.fivemin),
                    'freq': self.options.get('freq', self.FREQUENCY_CHOICES.fivemin),
                    'fuel_name': 'nonwind',
                    'gen_MW': (load_val - total_imports_val) - wind_val,
                },
            ]
        else:
            raise ValueError('Cannot get real-time data for %s' % self.options['data'])

        # return
        return data

    def handle_options(self, **kwargs):
        super(ERCOTClient, self).handle_options(**kwargs)

        # ensure market and freq are set
        if 'market' not in self.options:
            if self.options['forecast']:
                self.options['market'] = self.MARKET_CHOICES.dam
            else:
                self.options['market'] = self.MARKET_CHOICES.dam
        if 'freq' not in self.options:
            if self.options['forecast']:
                self.options['freq'] = self.FREQUENCY_CHOICES.hourly
            else:
                self.options['freq'] = self.FREQUENCY_CHOICES.fivemin

        if 'latest' not in self.options:
            self.options['latest'] = False
        if 'forecast' not in self.options:
            self.options['forecast'] = False

    def _parse_dam_times(self, df):
        # Construct datetime string and convert to naive datetime
        df['hour'] = (df['HourEnding'].str.replace(':', '.').astype(float) - 1).astype(int)
        df['datetime_str'] = df['DeliveryDate'] + ' ' + df['hour'].astype(str)
        date_format = '%m/%d/%Y %H'
        df.index = pd.to_datetime(df['datetime_str'], format=date_format)

        # convert to local time, ambiguous times fixed by DSTFlag, then to utc
        df.index = df.index.tz_localize(self.TZ_NAME, ambiguous=df['DSTFlag'] == 'Y')
        df.drop(['DeliveryDate', 'HourEnding', 'DSTFlag', 'hour', 'datetime_str'], axis=1, inplace=True)
        df.rename(columns={'SettlementPointPrice': 'lmp', 'SettlementPoint': 'node_id'}, inplace=True)
        return df

    def _parse_rtm_times(self, df):
        date_format = '%m/%d/%Y %H:%M:%S'
        df.index = pd.to_datetime(df['SCEDTimestamp'], format=date_format)

        # When DST ends in the Fall, the repeated hour is NOT in DST
        df.index = df.index.tz_localize(self.TZ_NAME, ambiguous=df['RepeatedHourFlag'] == 'N')
        df.drop(['SCEDTimestamp', 'RepeatedHourFlag'], axis=1, inplace=True)
        df.rename(columns={'LMP': 'lmp', 'SettlementPoint': 'node_id'}, inplace=True)
        return df

    def format_lmp(self, df):
        if 'SCEDTimestamp' in df.columns:
            df = self._parse_rtm_times(df)
        else:
            df = self._parse_dam_times(df)

        df.index = df.index.tz_convert('utc')

        df['freq'] = self.options['freq']
        df['ba_name'] = 'ERCOT'
        df['market'] = self.options['market']
        df['lmp_type'] = 'prc'

        df.sort_index(inplace=True)
        df['timestamp'] = df.index
        return df

    def get_lmp(self, node_id='HB_HUBAVG', **kwargs):
        self.handle_options(data='lmp', node_id=node_id, **kwargs)

        if self.options['market'] == self.MARKET_CHOICES.fivemin:
            report_name = 'rt5m_lmp'
        elif self.options['market'] == self.MARKET_CHOICES.dam:
            report_name = 'dam_hrly_lmp'
        elif self.options['market'] == self.MARKET_CHOICES.hourly:
            raise NotImplementedError('ERCOT does not produce realtime hourly prices?')

        self.now = datetime.now(pytz.utc)

        if 'start_at' in self.options:
            # get start and end days in local time
            tz = pytz.timezone(self.TZ_NAME)
            start = tz.normalize(self.options['start_at'])
            end = tz.normalize(self.options['end_at'])

            pieces = []
            if self.options['market'] == self.MARKET_CHOICES.fivemin:
                # set up periods of length 5 min
                fivemin_periods = int((end-start).total_seconds()/(60*5)) + 1
                p_list = [end - timedelta(minutes=5*x) for x in range(fivemin_periods)]

                # warn if this could take a long time
                if len(p_list) > 5:
                    LOGGER.warn('Making %d data requests (one for each 5min period), this could take a while' % len(p_list))

                # make request for each period
                for period in p_list:
                    try:
                        report = self._request_report(report_name, date=period)
                        pieces.append(report)
                    except ValueError:
                        pass

            else:
                start = datetime(start.year, start.month, start.day, tzinfo=start.tzinfo)
                days_list = [end - timedelta(days=x) for x in range((end-start).days + 1)]
                for day in days_list:
                    try:
                        report = self._request_report(report_name, day)
                        pieces.append(report)
                    except ValueError:
                        pass

            # combine pieces, if any
            if len(pieces) > 0:
                report = pd.concat(pieces)
            else:
                LOGGER.warn('No ERCOT LMP found for %s' % self.options)
                return []
        else:
            report = self._request_report(report_name, self.now)
            if report is None:
                report = self._request_report(report_name, self.now - timedelta(days=1))
        df = self.format_lmp(report)

        # strip uneeded times
        df = self.slice_times(df)

        # strip out unwanted nodes
        if node_id:
            if not isinstance(node_id, list):
                node_id = [node_id]
            reg = re.compile('|'.join(node_id))
            df = df.ix[df['node_id'].str.contains(reg)]

        return df.to_dict(orient='records')
