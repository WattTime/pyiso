from datetime import datetime, timedelta, time
from pyiso.base import BaseClient
import copy
import re
from bs4 import BeautifulSoup


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

    oasis_markets = {
        BaseClient.MARKET_CHOICES.hourly: 'RTM',
        BaseClient.MARKET_CHOICES.fivemin: 'RTM',
        BaseClient.MARKET_CHOICES.dam: 'DAM',
    }

    def get_generation(self, latest=False, yesterday=False,
                       start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='gen', latest=latest, yesterday=yesterday,
                            start_at=start_at, end_at=end_at, **kwargs)

        # ensure market and freq are set
        if 'market' not in self.options:
            if self.options['forecast']:
                self.options['market'] = self.MARKET_CHOICES.dam
            else:
                self.options['market'] = self.MARKET_CHOICES.fivemin
        if 'freq' not in self.options:
            if self.options['forecast']:
                self.options['freq'] = self.FREQUENCY_CHOICES.hourly
            else:
                self.options['freq'] = self.FREQUENCY_CHOICES.fivemin

        if latest:
            return self._generation_latest()
        elif self.options['forecast']:
            return self._generation_forecast()
        else:
            return self._generation_historical()

    def get_load(self, latest=False,
                       start_at=False, end_at=False, **kwargs):
        # set args
        self.handle_options(data='load', latest=latest,
                            start_at=start_at, end_at=end_at, **kwargs)

        # ensure market and freq are set
        if 'market' not in self.options:
            if self.options['forecast']:
                self.options['market'] = self.MARKET_CHOICES.dam
            else:
                self.options['market'] = self.MARKET_CHOICES.fivemin
        if 'freq' not in self.options:
            if self.options['forecast']:
                self.options['freq'] = self.FREQUENCY_CHOICES.hourly
            else:
                self.options['freq'] = self.FREQUENCY_CHOICES.fivemin

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

        # ensure market and freq are set
        if 'market' not in self.options:
            if self.options['forecast']:
                self.options['market'] = self.MARKET_CHOICES.dam
            else:
                self.options['market'] = self.MARKET_CHOICES.fivemin
        if 'freq' not in self.options:
            if self.options['forecast']:
                self.options['freq'] = self.FREQUENCY_CHOICES.hourly
            else:
                self.options['freq'] = self.FREQUENCY_CHOICES.fivemin

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
        market_run_id = self.oasis_markets[self.options['market']]
 
        # construct payload
        payload = {'queryname': queryname,
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
            for header in [0, 0]:
                df = self.parse_to_df(response.text,
                                    skiprows=header, nrows=24, header=header,
                                    delimiter='\t+', engine='python')

                # combine date with hours to index
                indexed = self.set_dt_index(df, this_date, df['Hour'])

                # original header is fuel names
                indexed.rename(columns=self.fuels, inplace=True)

                # remove non-fuel cols
                fuel_cols = list( set(self.fuels.values()) & set(indexed.columns) )
                subsetted = indexed[fuel_cols]

                # pivot
                pivoted = self.unpivot(subsetted)
                pivoted.rename(columns={'level_1': 'fuel_name', 0: 'gen_MW'}, inplace=True)

                # store
                parsed_data += self.serialize(pivoted,
                                      header=['timestamp', 'fuel_name', 'gen_MW'],
                                      extras={'ba_name': self.NAME,
                                              'market': self.MARKET_CHOICES.hourly,
                                              'freq': self.FREQUENCY_CHOICES.hourly})

            # finish day
            this_date += timedelta(days=1)

        # return
        return parsed_data
        
    def fetch_oasis(self, payload={}):
        """Returns a list of report data elements, or an empty list if an error was encountered."""
        # set up storage
        raw_data = []
 
        # try get
        response = self.request(self.base_url_oasis, params=payload) # have request
        if not response:
            return []
            
        # read data from zip
        content = self.unzip(response.content)
        if not content:
            return []
        
        # load xml into soup
        soup = BeautifulSoup(content)
        
        # check xml content
        error = soup.find('m:error')
        if error:
            code = error.find('m:err_code')
            desc = error.find('m:err_desc')
            msg = 'XML error for CAISO OASIS with payload %s: %s %s' % (payload, code, desc)
            self.logger.error(msg)
            return []
                
        else:
            raw_data = soup.find_all('report_data')
            return raw_data
        
    def parse_oasis_renewable(self, raw_data):
        """Parse raw data output of fetch_oasis for renewables."""
        # set up storage
        preparsed_data = {}
        parsed_data = []
        
        # extract values from xml
        for raw_soup_dp in raw_data:
            # set up storage for timestamp
            ts = self.utcify(raw_soup_dp.find('interval_start_gmt').string)
            if ts not in preparsed_data:
                preparsed_data[ts] = {'wind': 0, 'solar': 0}
                
            # store generation value
            try:
                fuel_name = raw_soup_dp.find('renewable_type').string.lower()
                gen_MW = float(raw_soup_dp.find('value').string)
                preparsed_data[ts][fuel_name] += gen_MW
            except TypeError:
                self.logger.error('Error in schema for CAISO OASIS result %s' % raw_soup_dp.prettify())
                continue
            
        # collect values into dps
        freq = self.options.get('freq', self.FREQUENCY_CHOICES.hourly)
        market = self.options.get('market', self.MARKET_CHOICES.hourly)

        for ts, preparsed_dp in preparsed_data.iteritems():
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
            data_item = raw_soup_dp.find('data_item').string
            if data_item in data_items:
                # parse timestamp
                ts = self.utcify(raw_soup_dp.find('interval_start_gmt').string)

                # parse val
                if data_item == 'ISO_TOT_IMP_MW':
                    val = -float(raw_soup_dp.find('value').string)
                else:
                    val = float(raw_soup_dp.find('value').string)

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
            if raw_soup_dp.find('data_item').string == data_item_key and \
                    raw_soup_dp.find('resource_name').string == 'CA ISO-TAC':
                
                # parse timestamp
                ts = self.utcify(raw_soup_dp.find('interval_start_gmt').string)

                # set up base
                parsed_dp = {'timestamp': ts,
                              'freq': freq,
                              'market': market,
                              'ba_name': self.NAME}
                    
                # store generation value
                parsed_dp['load_MW'] = float(raw_soup_dp.find('value').string)
                parsed_data.append(parsed_dp)
                
        # return
        return parsed_data
        
    def todays_outlook_time(self):
       # get timestamp
        response = self.request(self.base_url_outlook+'systemconditions.html')
        if not response:
            return None

        demand_soup = BeautifulSoup(response.content)
        for ts_soup in demand_soup.find_all(class_='docdate'):
            match = re.search('\d{1,2}-[a-zA-Z]+-\d{4} \d{1,2}:\d{2}', ts_soup.string)
            if match:
                ts_str = match.group(0)
                return self.utcify(ts_str)

    def fetch_todays_outlook_renewables(self):
        # get renewables data
        response = self.request(self.base_url_outlook+'renewables.html')
        return BeautifulSoup(response.content)
        
    def parse_todays_outlook_renewables(self, soup, ts):
        # set up storage
        parsed_data = []

        freq = self.options.get('freq', self.FREQUENCY_CHOICES.tenmin)
        market = self.options.get('market', self.MARKET_CHOICES.tenmin)

        # get all renewables values
        for (id_name, fuel_name) in [('totalrenewables', 'renewable'),
                                        ('currentsolar', 'solar'),
                                        ('currentwind', 'wind')]:
            resource_soup = soup.find(id=id_name)
            if resource_soup:
                match = re.search('(?P<val>\d+.?\d+)\s+MW', resource_soup.string)
                if match:
                    parsed_dp = {'timestamp': ts,
                                  'freq': freq,
                                  'market': market,
                                  'ba_name': self.NAME}
                    parsed_dp['gen_MW'] = float(match.group('val'))
                    parsed_dp['fuel_name'] = fuel_name
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
        
        # get and parse "Today's Outlook" data
        soup = self.fetch_todays_outlook_renewables()
        ts = self.todays_outlook_time()
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
