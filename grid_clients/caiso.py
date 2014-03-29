import requests
from datetime import datetime, timedelta, time
from dateutil.parser import parse as dateutil_parse
import pytz
from grid_clients.base import BaseClient
import copy
import zipfile
import StringIO
import re
from bs4 import BeautifulSoup


class CAISOClient(BaseClient):
    def __init__(self):
        self.ba_name = 'CAISO'
        
        self.base_url_oasis = 'http://oasis.caiso.com/oasisapi/SingleZip'
        self.base_url_gen = 'http://content.caiso.com/green/renewrpt/'
        self.base_url_outlook = 'http://content.caiso.com/outlook/SP/'
        self.base_payload = {'version': 1}
        self.oasis_request_time_format = '%Y%m%dT%H:%M-0000'
        
        self.tz = pytz.timezone('America/Los_Angeles')
        
        self.fuels = {
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

    def get_generation(self, latest=False, yesterday=False,
                       start_at=False, end_at=False, **kwargs):
        if latest:
            return self.get_generation_latest(**kwargs)
        else:
            if yesterday:
                ca_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('America/Los_Angeles'))
                end_at = ca_now.replace(hour=0, minute=0, second=0, microsecond=0)
                start_at = end_at - timedelta(days=1)
            return self.get_generation_historical(start_at, end_at, **kwargs)
            
    def _split_tsv(self, row):
        vals = row.split('\t')
        real_vals = [val for val in vals if val != '']
        return real_vals
            
    def _preprocess_tsv(self, row, date):
        vals = self._split_tsv(row)
        vals[0] = self._utcify(datetime.combine(date, time(hour=int(vals[0])-1)))
        return vals

    def _utcify(self, naive_local_timestamp):
        aware_local_timestamp = self.tz.localize(naive_local_timestamp)
        aware_utc_timestamp = aware_local_timestamp.astimezone(pytz.utc)
        return aware_utc_timestamp

    def get_generation_historical(self, start_at, end_at, **kwargs):
        # process args
        assert start_at <= end_at

        # set up storage
        raw_data = []
        parsed_data = []

        # collect raw data
        this_date = start_at.date()
        while this_date <= end_at.date():
            # set up request
            url_file = this_date.strftime('%Y%m%d_DailyRenewablesWatch.txt')
            url = self.base_url_gen + url_file
            
            # carry out request
            response = requests.get(url)
            if response.status_code == 200:
                self.logger.debug('Got source data for CAISO generation for %s' % this_date)
            else:
                self.logger.error('Error in source data for CAISO generation for %s' % this_date)
                this_date += timedelta(days=1)
                continue
            rows = response.text.split('\n')
        
            # process renewables
            ren_header = [x.strip() for x in self._split_tsv(rows[1])]
            for row in rows[2:(2+24)]:
                vals = self._preprocess_tsv(row, this_date)
                raw_data.append(dict(zip(ren_header, vals)))
            
            # process other
            other_header = [x.strip() for x in self._split_tsv(rows[29])]
            for row in rows[30:(30+24)]:
                vals = self._preprocess_tsv(row, this_date)
                raw_data.append(dict(zip(other_header, vals)))

            # finish day
            this_date += timedelta(days=1)

        # parse data
        for raw_dp in raw_data:
            for raw_fuel_name, parsed_fuel_name in self.fuels.iteritems():
                # set up storage
                parsed_dp = {}

                # check for fuel
                try:
                    parsed_dp['gen_MW'] = float(raw_dp[raw_fuel_name])
                except KeyError:
                    continue                    

                # add other values
                parsed_dp['timestamp'] = raw_dp['Hour']
                parsed_dp['fuel_name'] = parsed_fuel_name
                parsed_dp['ba_name'] = self.ba_name
                parsed_dp['market'] = self.MARKET_CHOICES.hourly
                parsed_dp['freq'] = self.FREQUENCY_CHOICES.hourly
                
                # add to full storage
                parsed_data.append(parsed_dp)

        return parsed_data
        
    def _request_oasis(self, payload={}):
        # set up storage
        raw_data = []
 
        # try get
        try: 
            r = requests.get(self.base_url_oasis, params=payload) # have request
        except requests.exceptions.RequestException:
            self.logger.error('Exception raised for CAISO OASIS with payload %s' % payload)
            return []
            
        # read data from zip
        z = zipfile.ZipFile(StringIO.StringIO(r.content)) # have zipfile
        content = z.read(z.namelist()[0]) # have xml
        z.close()
        
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
        
    def _parse_oasis_renewable(self, raw_data):
        """Parse raw data output of _request_oasis for renewables."""
        # set up storage
        preparsed_data = {}
        parsed_data = []
        
        # extract values from xml
        for raw_soup_dp in raw_data:
            # set up storage for timestamp
            ts = dateutil_parse(raw_soup_dp.find('interval_start_gmt').string).astimezone(pytz.utc)
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
        for ts, preparsed_dp in preparsed_data.iteritems():
            # set up base
            base_parsed_dp = {'timestamp': ts,
                              'freq': self.FREQUENCY_CHOICES.hourly,
                              'market': self.MARKET_CHOICES.hourly,
                              'gen_MW': 0, 'ba_name': self.ba_name}
                          
            # collect data
            for fuel_name in ['wind', 'solar']:
                parsed_dp = copy.deepcopy(base_parsed_dp)
                parsed_dp['fuel_name'] = fuel_name
                parsed_dp['gen_MW'] += preparsed_dp[fuel_name]
                parsed_data.append(parsed_dp)
            
        # return
        return parsed_data

    def _parse_oasis_slrs(self, raw_data):
        """Parse raw data output of _request_oasis for System Load and Resource Schedules."""
        # set up storage
        parsed_data = []
        
        # extract values from xml
        for raw_soup_dp in raw_data:
            if raw_soup_dp.find('data_item').string == 'ISO_TOT_GEN_MW':
                
                # parse timestamp
                ts = dateutil_parse(raw_soup_dp.find('interval_start_gmt').string).astimezone(pytz.utc)

                # set up base
                parsed_dp = {'timestamp': ts, 'fuel_name': 'other',
                              'freq': self.FREQUENCY_CHOICES.fivemin,
                              'market': self.MARKET_CHOICES.fivemin,
                              'ba_name': self.ba_name}
                    
                # store generation value
                parsed_dp['gen_MW'] = float(raw_soup_dp.find('value').string)
                parsed_data.append(parsed_dp)
                
        # return
        return parsed_data
        
    def _get_todays_outlook(self):
        # set up storage
        parsed_data = []
        
        # get timestamp
        demand_contents = requests.get(self.base_url_outlook+'systemconditions.html').content
        demand_soup = BeautifulSoup(demand_contents)
        ts = None
        for ts_soup in demand_soup.find_all(class_='docdate'):
            match = re.search('\d{1,2}-[a-zA-Z]+-\d{4} \d{1,2}:\d{2}', ts_soup.string)
            if match:
                ts_str = match.group(0)
                ts = self.tz.localize(dateutil_parse(ts_str)).astimezone(pytz.utc)
                break
                
        # get renewables data
        ren_contents = requests.get(self.base_url_outlook+'renewables.html').content
        ren_soup = BeautifulSoup(ren_contents)
        
        # get all renewables values
        for (id_name, fuel_name) in [('totalrenewables', 'renewable'),
                                        ('currentsolar', 'solar'),
                                        ('currentwind', 'wind')]:
            resource_soup = ren_soup.find(id=id_name)
            match = re.search('(?P<val>\d+.?\d+) MW', resource_soup.string)
            if match:
                parsed_dp = {'timestamp': ts,
                              'freq': self.FREQUENCY_CHOICES.tenmin,
                              'market': self.MARKET_CHOICES.tenmin,
                              'ba_name': self.ba_name}
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
        
    def get_generation_latest(self, **kwargs):
        # set up
        parsed_data = []
        
        # get and parse "Today's Outlook" data
        parsed_data += self._get_todays_outlook()
        total_ren_MW = sum([dp['gen_MW'] for dp in parsed_data])
        ts = parsed_data[0]['timestamp']
        
        # get and parse OASIS total gen data
        gen_payload = {'queryname': 'ENE_SLRS',
                   'market_run_id': 'RTM', 'schedule': 'ALL',
                   'startdatetime': (ts-timedelta(minutes=20)).strftime(self.oasis_request_time_format),
                   'enddatetime': (ts+timedelta(minutes=20)).strftime(self.oasis_request_time_format),
                  }
        gen_payload.update(self.base_payload)
        gen_oasis_data = self._request_oasis(payload=gen_payload)
        has_other = False
        for dp in self._parse_oasis_slrs(gen_oasis_data):
            if dp['timestamp'] == ts:
                dp['gen_MW'] -= total_ren_MW
                dp['freq'] = self.FREQUENCY_CHOICES.tenmin
                parsed_data.append(dp)
                has_other = True
                break

        # check and return
        if has_other:
            return parsed_data
        else:
            return []
