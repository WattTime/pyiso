import requests
from datetime import datetime, timedelta, time
from dateutil.parser import parse as dateutil_parse
import pytz
from apps.griddata.models import DataPoint
import logging
import re


class CAISOClient:
    def __init__(self):
        self.ba_name = 'CAISO'
        
        self.base_url_oasis = 'http://oasis.caiso.com/mrtu-oasis/SingleZip'
        self.base_url_gen = 'http://content.caiso.com/green/renewrpt/'
        self.base_payload = {'resultformat': '6'}
        
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

        self.logger = logging.getLogger(__name__)

    def get_generation(self, latest=False, start_at=False, end_at=False, **kwargs):
        if latest:
            return self.get_generation_latest(**kwargs)
        else:
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
        aware_local_timestamp = pytz.timezone('America/Los_Angeles').localize(naive_local_timestamp)
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
            if response.status_code != 200:
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
                parsed_dp['market'] = DataPoint.RTHR
                parsed_dp['freq'] = DataPoint.HOURLY
                
                # add to full storage
                parsed_data.append(parsed_dp)

        return parsed_data
        
    def get_generation_latest(self, **kwargs):
        # set up get request
        payload = {'queryname': 'SLD_REN_FCST',
                   'market_run_id': 'ACTUAL',
                   'startdate': start_date.strftime(self.DATE_FRMT),
                   'enddate': end_date.strftime(self.DATE_FRMT),
                  }
        payload.update(self.BASE_PAYLOAD)
 
        # try get
        try: 
            r = requests.get(self.base_url, params=payload) # have request
        except requests.exceptions.RequestException, e:
            raise Exception('unable to get CAISO data' + str(e))
            
        # read data from zip
        z = zipfile.ZipFile(StringIO.StringIO(r.content)) # have zipfile
        f = z.read(z.namelist()[0]) # have csv
        z.close()
        
        # return file-like object
        stream = StringIO.StringIO(f)
        return stream