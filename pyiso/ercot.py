from datetime import timedelta
import copy
from bs4 import BeautifulSoup
from pyiso.base import BaseClient


class ERCOTClient(BaseClient):
    NAME = 'ERCOT'
    base_report_url = 'http://mis.ercot.com'
            
    report_type_ids = {
        'wind_5min': '13071',
        'wind_hrly': '13028',
        'gen_hrly': '12358',
    }

    TZ_NAME = 'US/Central'
        
    def utcify(self, local_ts, **kwargs):
        # ERCOT is hour ending, want hour beginning
        utc_ts = super(ERCOTClient, self).utcify(local_ts, **kwargs)
        return utc_ts - timedelta(hours=1)

    def _request_report(self, report_type):
        # request reports list
        params = {'reportTypeId': self.report_type_ids[report_type]}
        report_list_contents = self.request(self.base_report_url+'/misapp/GetReports.do',
                                            params=params).content
        report_list_soup = BeautifulSoup(report_list_contents)
        
        # find the endpoint to download
        report_endpoint = None
        for elt in report_list_soup.find_all('tr'):
            label = elt.find(class_='labelOptional_ind')
            if label:
                if 'csv' in label.string:
                    report_endpoint = self.base_report_url + elt.a.attrs['href']
                    break

        # test endpoint found
        if not report_endpoint:
            raise ValueError('ERCOT: No report available for %s, soup:\n%s' % (report_type, report_list_soup))
                
        # read report from zip
        r = self.request(report_endpoint)
        if r:
            content = self.unzip(r.content)
        else:
            return []
        
        # parse csv
        rows = content.split('\n')
        header = rows[0].split(',')
        raw_data = [dict(zip(header, self.parse_row(row))) for row in rows[1:-1]]
        
        # return
        return raw_data

    def is_dst(self, val, standard):
        return val != standard
        
    def get_generation(self, latest=False, **kwargs):
        # get nonwind gen data
        raw_gen_data = self._request_report('gen_hrly')
        assert len(raw_gen_data) == 1
        total_dp = raw_gen_data[0]
        total_gen = float(total_dp['SE_MW'])
        
        # get timestamp on hour
        raw_ts = self.utcify(total_dp['SE_EXE_TIME'],
                             is_dst=self.is_dst(total_dp['SE_EXE_TIME_DST'], 's'))
        ts_hour_ending = raw_ts.replace(minute=0, second=0, microsecond=0)
        if raw_ts.minute > 30:
            ts_hour_ending += timedelta(hours=1)
        ts_hour_starting = ts_hour_ending - timedelta(hours=1)

        # process wind data
        wind_gen = None
        for wind_dp in self._request_report('wind_hrly'):
            wind_ts = self.utcify(wind_dp['HOUR_ENDING'],
                                  is_dst=self.is_dst(wind_dp['DSTFlag'], 'N'))
            if wind_ts == ts_hour_ending:
                try:
                    wind_gen = float(wind_dp['ACTUAL_SYSTEM_WIDE'])
                except ValueError: # empty string
                    wind_gen = None
                    self.logger.error('No wind data available at %s in ERCOT for hour ending %s' % (raw_ts, ts_hour_ending))
                break
            
        # set up storage
        parsed_data = []
        base_dp = {'timestamp': ts_hour_starting,
                   'freq': self.FREQUENCY_CHOICES.hourly, 'market': self.MARKET_CHOICES.hourly,
                   'gen_MW': 0, 'ba_name': self.NAME}

        # collect parsed data
        if wind_gen is not None:
            nonwind_gen = total_gen - wind_gen
            for gen_MW, fuel_name in [(wind_gen, 'wind'), (nonwind_gen, 'nonwind')]:
                parsed_dp = copy.deepcopy(base_dp)
                parsed_dp['fuel_name'] = fuel_name
                parsed_dp['gen_MW'] = gen_MW
                parsed_data.append(parsed_dp)
                
        # return
        return parsed_data
