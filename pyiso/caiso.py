import copy
import re
from datetime import datetime, timedelta, time
from io import BytesIO, StringIO

import pandas as pd
import pytz
from bs4 import BeautifulSoup

from pyiso import LOGGER
from pyiso.base import BaseClient


class CAISOClient(BaseClient):
    """
    Interface to CAISO data sources.

    For information about the data sources,
    see http://www.caiso.com/Documents/InterfaceSpecifications-OASISv4_1_3.pdf
    """
    NAME = 'CAISO'

    base_url_oasis = 'http://oasis.caiso.com/oasisapi/SingleZip'
    base_url_gen = 'http://content.caiso.com/green/renewrpt/'
    base_url_outlook = 'http://content.caiso.com/outlook/SP/'
    price_map_url = 'http://wwwmobile.caiso.com/Web.Service.Chart/api/v3/ChartService/PriceContourMap1'
    base_payload = {'version': 1}
    oasis_request_time_format = '%Y%m%dT%H:%M-0000'

    TZ_NAME = 'America/Los_Angeles'

    fuels = {
        'GEOTHERMAL': 'geo',
        'BIOMASS': 'biomass',
        'BIOGAS': 'biogas',
        'SMALL HYDRO': 'smhydro',
        'WIND TOTAL': 'wind',
        'SOLAR': 'solar',
        'SOLAR PV': 'solarpv',
        'SOLAR THERMAL': 'solarth',
        'NUCLEAR': 'nuclear',
        'THERMAL': 'thermal',
        'HYDRO': 'hydro',
    }

    oasis_markets = {  # {'RT5M': 'RTM', 'DAHR': 'DAM', 'RTHR': 'HASP'}
        BaseClient.MARKET_CHOICES.hourly: 'HASP',
        BaseClient.MARKET_CHOICES.fivemin: 'RTM',  # There are actually three codes used: RTPD (Real-time Pre-dispatch), RTD (real-time dispatch), and RTM (Real-Time Market). I can't figure out what the difference is.
        BaseClient.MARKET_CHOICES.dam: 'DAM',
        BaseClient.MARKET_CHOICES.fifteenmin: 'RTPD'
    }

    def __init__(self):
        super(CAISOClient, self).__init__()
        self.ca_tz = pytz.timezone(self.TZ_NAME)

    def handle_options(self, **kwargs):
        # regular handle options
        super(CAISOClient, self).handle_options(**kwargs)

        # ensure market and freq are set
        if 'market' not in self.options:
            if self.options['forecast']:
                self.options['market'] = self.MARKET_CHOICES.dam
            elif self.options['sliceable'] and self.options['data'] == 'gen':
                self.options['market'] = self.MARKET_CHOICES.dam
            else:
                self.options['market'] = self.MARKET_CHOICES.fivemin
        if 'freq' not in self.options:
            if self.options['forecast']:
                self.options['freq'] = self.FREQUENCY_CHOICES.hourly
            elif self.options['sliceable'] and self.options['data'] == 'gen':
                self.options['freq'] = self.FREQUENCY_CHOICES.hourly
            else:
                self.options['freq'] = self.FREQUENCY_CHOICES.fivemin

    def get_generation(self, latest=False, yesterday=False,
                       start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='gen', latest=latest, yesterday=yesterday,
                            start_at=start_at, end_at=end_at, **kwargs)

        if self.options['latest']:
            return self._generation_latest()
        elif self.options['forecast'] or self.options['market'] == self.MARKET_CHOICES.dam:
            return self._generation_forecast()
        else:
            return self._generation_historical()

    def get_load(self, latest=False,
                 start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='load', latest=latest,
                            start_at=start_at, end_at=end_at, **kwargs)

        # construct and execute OASIS request
        payload = self.construct_oasis_payload('SLD_FCST')
        oasis_data = self.fetch_oasis(payload=payload)

        # parse data
        parsed_data = self.parse_oasis_demand_forecast(oasis_data)

        if self.options['latest']:
            # select latest
            latest_dp = None
            latest_ts = self.utcify('1900-01-01 12:00')
            now = self.utcify(datetime.utcnow(), tz_name='utc')
            for dp in parsed_data:
                if dp['timestamp'] < now and dp['timestamp'] > latest_ts:
                    latest_dp = dp
                    latest_ts = dp['timestamp']

            # return latest
            if latest_dp:
                return [latest_dp]
            else:
                return []
        else:
            # return all data
            return parsed_data

    def get_trade(self, latest=False,
                  start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='trade', latest=latest,
                            start_at=start_at, end_at=end_at, **kwargs)

        # construct and execute OASIS request
        payload = self.construct_oasis_payload('ENE_SLRS')
        oasis_data = self.fetch_oasis(payload=payload)

        # parse data
        parsed_data = self.parse_oasis_slrs(oasis_data)

        if self.options['latest']:
            # select latest
            latest_dp = None
            latest_ts = self.utcify('1900-01-01 12:00')
            now = self.utcify(datetime.utcnow(), tz_name='utc')
            for dp in parsed_data:
                if dp['timestamp'] < now and dp['timestamp'] > latest_ts:
                    latest_dp = dp
                    latest_ts = dp['timestamp']

            # return latest
            if latest_dp:
                return [latest_dp]
            else:
                return []
        else:
            # return all data
            return parsed_data

    def construct_oasis_payload(self, queryname, **kwargs):
        # get start and end times
        if self.options['latest']:
            now = self.utcify(datetime.utcnow(), tz_name='utc')
            startdatetime = now - timedelta(minutes=20)
            enddatetime = now + timedelta(minutes=20)
        else:
            startdatetime = self.options['start_at']
            enddatetime = self.options['end_at']

        # get market id
        try:
            market_run_id = self.options['market_run_id']
        except KeyError:
            market_run_id = self.oasis_markets[self.options['market']]

        self.options.update(market_run_id=market_run_id)

        # construct payload
        payload = {
            'queryname': queryname,
            'market_run_id': market_run_id,
            'startdatetime': (startdatetime).strftime(self.oasis_request_time_format),
            'enddatetime': (enddatetime).strftime(self.oasis_request_time_format),
        }
        payload.update(self.base_payload)
        payload.update(kwargs)

        # return
        return payload

    def set_dt_index(self, df, date, hours, end_of_hour=True):
        if end_of_hour:
            offset = -1
        else:
            offset = 0

        # create list of combined datetimes
        dts = [datetime.combine(date, time(hour=(int(h)+offset))) for h in hours]

        # set list as index
        df.index = dts

        # utcify
        df.index = self.utcify_index(df.index)

        return df

    def _generation_historical(self):
        # set up storage
        parsed_data = []

        # collect data
        request_date = self.options['start_at'].astimezone(self.ca_tz).date()
        local_end_at = self.options['end_at'].astimezone(self.ca_tz).date()
        while request_date <= local_end_at:
            # set up request
            url_file = request_date.strftime('%Y%m%d_DailyRenewablesWatch.txt')
            url = self.base_url_gen + url_file

            # carry out request
            response = self.request(url)
            if not response:
                request_date += timedelta(days=1)
                continue

            dst_error_text = 'The supplied DateTime represents an invalid time.  For example, when the clock is ' \
                             'adjusted forward, any time in the period that is skipped is invalid.'
            header_idx = 1
            for part in [1, 2]:  # process both halves of page (i.e. two parts)
                num_data_rows = 24

                # The day transitioning to daylight saving time adds extra erroneous lines of text.
                if part == 1 and dst_error_text in response.text:
                    num_data_rows = 29

                df = self.parse_to_df(response.text, nrows=num_data_rows, header=header_idx, delimiter='\t+')

                # The day transitioning to daylight saving time has errors in part two of the file that need removal.
                if part == 2:
                    df = df[df.THERMAL.map(str) != '#VALUE!']

                # combine date with hours to index
                try:
                    indexed = self.set_dt_index(df, request_date, df['Hour'])
                except Exception as e:
                    LOGGER.error(e)
                    continue

                # original header is fuel names
                indexed.rename(columns=self.fuels, inplace=True)

                # remove non-fuel cols
                fuel_cols = list(set(self.fuels.values()) & set(indexed.columns))
                subsetted = indexed[fuel_cols]

                # pivot
                pivoted = self.unpivot(subsetted)
                pivoted.rename(columns={'level_1': 'fuel_name', 0: 'gen_MW'}, inplace=True)

                # slice times
                sliced = self.slice_times(pivoted)

                # store
                parsed_data += self.serialize(sliced,
                                      header=['timestamp', 'fuel_name', 'gen_MW'],
                                      extras={'ba_name': self.NAME,
                                              'market': self.MARKET_CHOICES.hourly,
                                              'freq': self.FREQUENCY_CHOICES.hourly})

                # If processing the first part, set the header index for second part.
                if part == 1:
                    header_idx = num_data_rows + 3

            # finish day
            request_date += timedelta(days=1)

        # return
        return parsed_data

    def fetch_oasis(self, payload={}, return_all_files=False):
        """
        Returns a list of report data elements, or an empty list if an error was encountered.

        If return_all_files=False, returns only the content from the first file in the .zip -
        this is the default behavior and was used in earlier versions of this function.

        If return_all_files=True, will return an array representing the content from each file.
        """
        # set up storage
        raw_data = []

        if return_all_files is True:
            default_return_val = []
        else:
            default_return_val = ''

        # try get
        response = self.request(self.base_url_oasis, params=payload)
        if not response:
            return default_return_val

        # read data from zip
        # This will be an array of content if successful, and None if unsuccessful
        content = self.unzip(response.content)
        if not content:
            return default_return_val

        # check xml content for errors
        soup = BeautifulSoup(content[0], 'xml')
        error = soup.find(['error', 'ERROR'])
        if error:
            code = error.find(['err_code', 'ERR_CODE'])
            desc = error.find(['err_desc', 'ERR_DESC'])
            msg = 'XML error for CAISO OASIS with payload %s: %s %s' % (payload, code, desc)
            LOGGER.error(msg)
            return default_return_val
        # return xml or csv data
        if payload.get('resultformat', False) == 6:
            # If we requested CSV files
            if return_all_files:
                return content
            else:
                return content[0]
        else:
            # Return XML content
            if return_all_files:
                raw_data = [BeautifulSoup(thisfile, 'xml').find_all(['REPORT_DATA', 'report_data']) for thisfile in content]
                return raw_data
            else:
                raw_data = soup.find_all(['REPORT_DATA', 'report_data'])
                return raw_data

    def parse_oasis_renewable(self, raw_data):
        """Parse raw data output of fetch_oasis for renewables."""
        # set up storage
        preparsed_data = {}
        parsed_data = []

        # extract values from xml

        for raw_soup_dp in raw_data:
            # set up storage for timestamp
            ts = self.utcify(raw_soup_dp.find(['INTERVAL_START_GMT', 'interval_start_gmt']).string)
            if ts not in preparsed_data:
                preparsed_data[ts] = {'wind': 0, 'solar': 0}

            # store generation value
            try:
                fuel_name = raw_soup_dp.find(['RENEWABLE_TYPE', 'renewable_type']).string.lower()
                gen_MW = float(raw_soup_dp.find(['VALUE', 'value']).string)
                preparsed_data[ts][fuel_name] += gen_MW
            except TypeError:
                LOGGER.error('Error in schema for CAISO OASIS result %s' % raw_soup_dp.prettify())
                continue

        # collect values into dps
        freq = self.options.get('freq', self.FREQUENCY_CHOICES.hourly)
        market = self.options.get('market', self.MARKET_CHOICES.hourly)

        for ts, preparsed_dp in preparsed_data.items():
            # set up base
            base_parsed_dp = {'timestamp': ts,
                              'freq': freq,
                              'market': market,
                              'gen_MW': 0, 'ba_name': self.NAME}

            # collect data
            for fuel_name in ['wind', 'solar']:
                parsed_dp = copy.deepcopy(base_parsed_dp)
                parsed_dp['fuel_name'] = fuel_name
                parsed_dp['gen_MW'] += preparsed_dp[fuel_name]
                parsed_data.append(parsed_dp)

        # return
        return parsed_data

    def parse_oasis_slrs(self, raw_data):
        """Parse raw data output of fetch_oasis for System Load and Resource Schedules."""
        # set strings to search on
        if self.options['data'] == 'gen':
            data_items = ['ISO_TOT_GEN_MW']
            data_label = 'gen_MW'
        elif self.options['data'] == 'trade':
            data_items = ['ISO_TOT_EXP_MW', 'ISO_TOT_IMP_MW']
            data_label = 'net_exp_MW'
        else:
            data_items = []
            data_label = None

        freq = self.options.get('freq', self.FREQUENCY_CHOICES.fivemin)
        market = self.options.get('market', self.MARKET_CHOICES.fivemin)

        # set up storage
        extracted_data = {}
        parsed_data = []

        # extract values from xml
        for raw_soup_dp in raw_data:
            data_item = raw_soup_dp.find(['DATA_ITEM', 'data_item']).string
            if data_item in data_items:
                # parse timestamp
                ts = self.utcify(raw_soup_dp.find(['INTERVAL_START_GMT', 'interval_start_gmt']).string)

                # parse val
                if data_item == 'ISO_TOT_IMP_MW':
                    val = -float(raw_soup_dp.find(['VALUE', 'value']).string)
                else:
                    val = float(raw_soup_dp.find(['VALUE', 'value']).string)

                # add to storage
                try:
                    extracted_data[ts] += val
                except KeyError:
                    extracted_data[ts] = val

        # assemble data
        for ts in sorted(extracted_data.keys()):
            parsed_dp = {data_label: extracted_data[ts]}
            parsed_dp.update({'timestamp': ts, 'freq': freq, 'market': market, 'ba_name': self.NAME})
            if self.options['data'] == 'gen':
                parsed_dp.update({'fuel_name': 'other'})

            # add to storage
            parsed_data.append(parsed_dp)

        # return
        return parsed_data

    def parse_oasis_demand_forecast(self, raw_data):
        """Parse raw data output of fetch_oasis for system-wide 5-min RTM demand forecast."""
        # set up storage
        parsed_data = []

        # set up freq and market
        freq = self.options.get('freq', self.FREQUENCY_CHOICES.fivemin)
        market = self.options.get('market', self.MARKET_CHOICES.fivemin)
        if market == self.MARKET_CHOICES.dam:
            data_item_key = 'SYS_FCST_DA_MW'
        else:
            data_item_key = 'SYS_FCST_5MIN_MW'

        # extract values from xml
        for raw_soup_dp in raw_data:
            if raw_soup_dp.find(['DATA_ITEM', 'data_item']).string == data_item_key and \
                    raw_soup_dp.find(['RESOURCE_NAME', 'resource_name']).string == 'CA ISO-TAC':

                # parse timestamp
                ts = self.utcify(raw_soup_dp.find(['INTERVAL_START_GMT', 'interval_start_gmt']).string)

                # set up base
                parsed_dp = {'timestamp': ts,
                             'freq': freq,
                             'market': market,
                             'ba_name': self.NAME}

                # store generation value
                parsed_dp['load_MW'] = float(raw_soup_dp.find(['VALUE', 'value']).string)
                parsed_data.append(parsed_dp)

        # return
        return parsed_data

    def todays_outlook_time(self, demand_soup):       
        for ts_soup in demand_soup.find_all(class_='docdate'):
            if str(ts_soup) is None:
                continue
            match = re.search('\d{1,2}-[a-zA-Z]+-\d{4} \d{1,2}:\d{2}', str(ts_soup))
            if match:
                ts_str = match.group(0)
                return self.utcify(ts_str)
        return None

    def fetch_todays_outlook_renewables(self):
        # get renewables data
        response = self.request(self.base_url_outlook+'renewables.html')
        try:
            return BeautifulSoup(response.content, 'lxml')
        except AttributeError:
            LOGGER.warn('No response for CAISO today outlook renewables')
            return None

    def parse_todays_outlook_renewables(self, soup, ts):
        # set up storage
        parsed_data = []

        # get all renewables values
        for (id_name, fuel_name) in [('totalrenewables', 'renewable'),
                                     ('currentsolar', 'solar'),
                                     ('currentwind', 'wind')]:
            resource_soup = soup.find(id=id_name)
            if resource_soup:
                match = re.search('(?P<val>\d+.?\d+)\s+MW', resource_soup.string)
                if match:
                    parsed_dp = {
                        'timestamp': ts,
                        'freq': self.FREQUENCY_CHOICES.tenmin,
                        'market': self.MARKET_CHOICES.tenmin,
                        'ba_name': self.NAME,
                        'gen_MW': float(match.group('val')),
                        'fuel_name': fuel_name,
                    }
                    parsed_data.append(parsed_dp)

        # actual 'renewable' value should be only renewables that aren't accounted for in other categories
        accounted_for_ren = 0
        for dp in parsed_data:
            if dp['fuel_name'] != 'renewable':
                accounted_for_ren += dp['gen_MW']
        for dp in parsed_data:
            if dp['fuel_name'] == 'renewable':
                dp['gen_MW'] -= accounted_for_ren

        return parsed_data

    def _generation_latest(self, **kwargs):
        # set up
        parsed_data = []

        # override market and freq to 10 minute
        self.options['market'] = self.MARKET_CHOICES.tenmin
        self.options['freq'] = self.FREQUENCY_CHOICES.tenmin

        # get "Today's Outlook" data
        soup = self.fetch_todays_outlook_renewables()
        if not soup:
            return []

        # parse "Today's Outlook" data

        # get timestamp
        response = self.request(self.base_url_outlook+'systemconditions.html')
        ts = None
        if response:
            demand_soup = BeautifulSoup(response.content, 'lxml')
            ts = self.todays_outlook_time(demand_soup)

        parsed_data += self.parse_todays_outlook_renewables(soup, ts)
        if len(parsed_data) == 0:
            return parsed_data
        total_ren_MW = sum([dp['gen_MW'] for dp in parsed_data])
        ts = parsed_data[0]['timestamp']

        # get OASIS total gen data
        payload = self.construct_oasis_payload(queryname='ENE_SLRS', schedule='ALL')
        oasis_data = self.fetch_oasis(payload=payload)

        # parse OASIS data
        for dp in self.parse_oasis_slrs(oasis_data):
            if dp['timestamp'] == ts:
                dp['gen_MW'] -= total_ren_MW
                dp['freq'] = self.options['freq']
                parsed_data.append(dp)
                return parsed_data

        # no matching OASIS data found, so return null
        return []

    def _generation_forecast(self, **kwargs):
        # set up
        parsed_data = []

        # get OASIS total gen data
        gen_payload = self.construct_oasis_payload(queryname='ENE_SLRS', schedule='ALL')
        gen_oasis_data = self.fetch_oasis(payload=gen_payload)
        gen_dps = self.parse_oasis_slrs(gen_oasis_data)

        # get OASIS renewable gen data
        ren_payload = self.construct_oasis_payload(queryname='SLD_REN_FCST')
        ren_oasis_data = self.fetch_oasis(payload=ren_payload)
        ren_dps = self.parse_oasis_renewable(ren_oasis_data)

        # set of times with both gen and renewable data
        times = set([dp['timestamp'] for dp in ren_dps]) & set([dp['timestamp'] for dp in gen_dps])

        # handle renewables
        total_ren_MW = {}
        for dp in ren_dps:
            if dp['timestamp'] in times:
                # assemble renewable totals for each time
                try:
                    total_ren_MW[dp['timestamp']] += dp['gen_MW']
                except KeyError:
                    total_ren_MW[dp['timestamp']] = dp['gen_MW']

                # add to storage
                parsed_data.append(dp)

        # handle generation
        for dp in gen_dps:
            if dp['timestamp'] in times:
                # subtract off renewable totals
                dp['gen_MW'] -= total_ren_MW[dp['timestamp']]
                # add to storage
                parsed_data.append(dp)

        # return
        return parsed_data
