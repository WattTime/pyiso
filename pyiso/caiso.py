from datetime import datetime, timedelta, time
from pyiso.base import BaseClient
from pyiso import LOGGER
import copy
import re
from bs4 import BeautifulSoup
from io import BytesIO, StringIO
import pandas as pd
import pytz


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
    LMP_MARKETS = {
        'RTM': 'PRC_INTVL_LMP',
        'DAM': 'PRC_LMP',
        'HASP': 'PRC_HASP_LMP',
        'RTPD': 'PRC_RTPD_LMP',
        BaseClient.MARKET_CHOICES.fivemin: 'PRC_INTVL_LMP',
        BaseClient.MARKET_CHOICES.dam: 'PRC_LMP',
        BaseClient.MARKET_CHOICES.hourly: 'PRC_HASP_LMP',
        BaseClient.MARKET_CHOICES.fifteenmin: 'PRC_RTPD_LMP',
    }
    AS_MARKETS = {
        'RTM': 'PRC_INTVL_AS',
        'DAM': 'PRC_AS',
        'HASP': 'PRC_AS',
        BaseClient.MARKET_CHOICES.fivemin: 'PRC_INTVL_AS',
        BaseClient.MARKET_CHOICES.dam: 'PRC_AS',
        BaseClient.MARKET_CHOICES.hourly: 'PRC_AS',
    }

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

    def get_lmp_loc(self):
        """
        CURRENTLY INCONSISTENTLY FAILING
        status 401, response content "Access is restricted to authorized clients."

        FORMERLY CONSISTENT BEHAVIOR:
        Returns a list of dictionaries containing node atlas data.  Each
        dictionary will contain

        {'node_id': <name of the node>
         'latitide': <latitude of the node>,
         'longitude': <longitude of the node>,
         'area': <control area the node belongs to>}

        This list is generated by parsing the JSON returned from the CAISO
        price map URL:
        http://wwwmobile.caiso.com/Web.Service.Chart/api/v3/ChartService/PriceContourMap1

        This json is what is used in the interactive CAISO price
        contour map found at:
        http://wwwmobile.caiso.com/Web.Service.Chart/pricecontourmap.html

        This retuned JSON does not contain geographic data for all CAISO
        pricing nodes
        """
        # get json from the price map url used in the CAISO interactive price
        # map http://wwwmobile.caiso.com/Web.Service.Chart/pricecontourmap.html
        r = self.request(self.price_map_url)
        json_obj = r.json()

        # parse the json to create the node location dictionary
        node_entries = json_obj['l'][2]['m']
        return_list = [{'node_id': str(entry['n']),
                        'latitude': entry['c'][0],
                        'longitude': entry['c'][1],
                        'area': str(entry['a'])} for entry in node_entries]

        return return_list

    def get_lmp(self, node_id='SLAP_PGP2-APND', **kwargs):
        """
        Returns a dictionary with keys of datetime.datetime objects
        Values holds $/MW float
        for final LMP price (i.e., LMP_TYPE Energy)
        """
        if not isinstance(node_id, list):
            node_id = [node_id]

        if len(node_id) > 10:
            # CAISO will not accept more than 10 node_ids
            # to do, if less than 10 node_ids, only get requested node ids
            node_list = node_id
            node_id = 'ALL'

        df = self.get_lmp_as_dataframe(node_id, **kwargs)
        df = self._standardize_lmp_dataframe(df)

        # drop non-requested nodes
        if node_id == 'ALL':
            df = df[df['node_id'].isin(node_list)]

        return df.to_dict(orient='records')

    def _standardize_lmp_dataframe(self, df):
        if df.empty:
            return df

        # RTPD returns column name PRC, some others return MW/LMP_PRC
        rename_dict = {'MW': 'lmp', 'PRC': 'lmp', 'LMP_PRC': 'lmp', 'NODE': 'node_id',
                       'LMP_TYPE': 'lmp_type', 'MARKET_RUN_ID': 'market'}
        df.rename(columns=rename_dict, inplace=True)

        # Get all data indexed on 'INTERVALSTARTTIME_GMT' as panda datetime
        if df.index.name != 'INTERVALSTARTTIME_GMT':
            df.set_index('INTERVALSTARTTIME_GMT', inplace=True)
            df.index.name = 'INTERVALSTARTTIME_GMT'
        df.index = pd.to_datetime(df.index)

        # utcify
        df.index = self.utcify_index(df.index, tz_name='UTC')
        df.sort_index(inplace=True)

        # revert MARKET_RUN_ID to standardized markets
        invert_oasis_markets = {v: k for k, v in self.oasis_markets.items() if k != v}
        df.replace({'market': invert_oasis_markets}, inplace=True)

        # add expected columns
        df['timestamp'] = df.index
        df['ba_name'] = 'CAISO'
        df['freq'] = self.options['freq']

        # drop unwanted columns
        df = df[['market', 'freq', 'lmp', 'lmp_type', 'node_id', 'ba_name', 'timestamp']]

        return df

    def get_lmp_as_dataframe(self, node_id, latest=True, start_at=False, end_at=False,
                             lmp_only=True, **kwargs):
        """
        Returns a pandas DataFrame with columns
        INTERVALSTARTTIME_GMT, MW, XML_DATA_ITEM, LMP_TYPE and others.
        Seperate rows for each LMP_TYPE (Congestion, Loss, Energy)
        MW columns holds $/MW float.
        If no data, returns an empty dataframe.
        """
        # set args
        self.handle_options(data='lmp', latest=latest,
                            start_at=start_at, end_at=end_at,
                            **kwargs)

        if self.options['latest']:
            queryname = 'PRC_CURR_LMP'
        else:
            queryname = self.LMP_MARKETS[self.options['market']]

        payload = self.construct_oasis_payload(queryname,
                                               resultformat=6,  # csv
                                               node=node_id)

        # Fetch data
        data = self.fetch_oasis(payload=payload, return_all_files=not(lmp_only))
        # data will be a single csv-derived string if lmp_only==True
        # data will be an array of csv-derived strings if lmp_only==False
        if lmp_only is True:
            # Turn into pandas Dataframe
            if len(data) == 0:
                return pd.DataFrame()

            try:
                str_data = BytesIO(data)    # Changed from StringIO for Python 3.4
            except TypeError:
                str_data = StringIO(data)

            df = pd.DataFrame.from_csv(str_data, sep=",")

            # strip congestion and loss prices
            try:
                df = df.ix[df['LMP_TYPE'] == 'LMP']
            except KeyError:  # no good data
                return pd.DataFrame()
        else:
            # data is an array of csv-derived strings
            df = pd.DataFrame()
            for thisFile in data:
                # Turn into pandas Dataframe
                try:
                    str_data = BytesIO(thisFile)  # Changed from StringIO for Python 3.4
                except TypeError:
                    str_data = StringIO(thisFile)

                tempDf = pd.DataFrame.from_csv(str_data, sep=",")

                df = pd.concat([df, tempDf])
            # Check to ensure good data
            try:
                df['LMP_TYPE'][0]
            except KeyError:  # no good data
                return pd.DataFrame()

        return df

    def get_AS_dataframe(self, node_id='AS_CAISO_EXP', latest=True, start_at=False, end_at=False,
                         market_run_id='DAM', **kwargs):
        """
        Returns a pandas DataFrame with columns
        INTERVALSTARTTIME_GMT, MW, XML_DATA_ITEM, ANC_TYPE and others.
        Seperate rows for each ANC_TYPE.
        MW columns holds $/MW float.
        If no data, returns an empty dataframe.
        """
        # set args, handle latest differently than LMP b/c daily publication schedule
        if latest and not start_at:
            end_at = datetime.now(pytz.utc)
            start_at = end_at - timedelta(minutes=61)
            latest = False

        self.handle_options(data='lmp', latest=latest,
                            start_at=start_at, end_at=end_at,
                            market_run_id=market_run_id,
                            **kwargs)

        queryname = self.AS_MARKETS[market_run_id]

        payload = self.construct_oasis_payload(queryname,
                                               resultformat=6,  # csv
                                               anc_region=node_id,
                                               **kwargs)

        # Fetch data
        data = self.fetch_oasis(payload=payload)

        if len(data) == 0:
            return pd.DataFrame()

        # Turn into pandas Dataframe
        try:
            str_data = BytesIO(data)    # Changed from StringIO.StringIO() for Python 3.4
        except TypeError:
            str_data = StringIO(data)

        df = pd.DataFrame.from_csv(str_data, sep=",")

        # Get all data indexed on 'INTERVALSTARTTIME_GMT' as panda datetime
        if df.index.name != 'INTERVALSTARTTIME_GMT':
            df.set_index('INTERVALSTARTTIME_GMT', inplace=True)
            df.index.name = 'INTERVALSTARTTIME_GMT'
        df.index = pd.to_datetime(df.index)

        # utcify
        df.index = self.utcify_index(df.index, tz_name='UTC')

        return df

    def get_ancillary_services(self, node_id, **kwargs):
        """
        Returns list of dicts

        If no data, returns an empty dict
        """
        df = self.get_AS_dataframe(node_id=node_id, **kwargs)
        if df.empty:
            return {}

        ret_list = []

        # loop through start_times, returns DataFrame rows are RU/RD/etc...
        for i in df.index.unique():
            a = df.ix[i]
            dp = {
                'timestamp': i.to_pydatetime(),  # INTERVALSTARTTIME_GMT is the index
                'market': a['MARKET_RUN_ID'],
                'zone_name': a['ANC_REGION'],
                'ba_name': 'CAISO',
            }
            # Loop through types of ancillary services, or if only 1 append it
            if isinstance(a['ANC_TYPE'], str):
                # a has only one ANC_TYPE
                dp[a['ANC_TYPE']] = a['MW']
            else:
                for i, row in a.iterrows():
                    dp[row['ANC_TYPE']] = row['MW']

            ret_list.append(dp)

        return ret_list

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
        dts = [datetime.combine(date, time(hour=(h+offset))) for h in hours]

        # set list as index
        df.index = dts

        # utcify
        df.index = self.utcify_index(df.index)

        # return
        return df

    def _generation_historical(self):
        # set up storage
        parsed_data = []

        # collect data
        this_date = self.options['start_at'].date()
        while this_date <= self.options['end_at'].date():
            # set up request
            url_file = this_date.strftime('%Y%m%d_DailyRenewablesWatch.txt')
            url = self.base_url_gen + url_file

            # carry out request
            response = self.request(url)
            if not response:
                this_date += timedelta(days=1)
                continue

            # process both halves of page
            for header in [1, 27]:
                df = self.parse_to_df(response.text,
                                      nrows=24, header=header,
                                      delimiter='\t+')

                # combine date with hours to index
                indexed = self.set_dt_index(df, this_date, df['Hour'])

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

            # finish day
            this_date += timedelta(days=1)

        # return
        return parsed_data

    def fetch_oasis(self, payload={}, return_all_files=False):
        """
        Returns a list of report data elements, or an empty list if an error was encountered.

        If return_all_files=False, returns only the content from the first file in the .zip -
        this is the default behavior and was used in earlier versions of this function.

        If return_all_files=True, will return an array representing the content from each file.
        This is useful for processing LMP data or other fields where multiple price components are returned in a zip.
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
