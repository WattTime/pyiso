from pyiso.base import BaseClient
from pyiso import LOGGER
import requests
import pandas as pd
import numpy as np
from io import StringIO
from time import sleep
from datetime import datetime, timedelta
import pytz
from os import environ


class EUClient(BaseClient):
    NAME = 'EU'
    TZ_NAME = 'UTC'
    base_url = 'https://transparency.entsoe.eu/'
    export_endpoint_load = 'load-domain/r2/totalLoadR2/export'
    export_endpoint_gen = 'generation/r2/actualGenerationPerProductionType/export'

    CONTROL_AREAS = {
        'AL': {'country': 'Albania', 'Code': 'CTA|AL',
            'ENTSOe_ID': 'CTY|10YAL-KESH-----5!CTA|10YAL-KESH-----5',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'AT': {'country': 'Austria', 'Code': 'CTA|AT',
            'ENTSOe_ID': 'CTY|10YAT-APG------L!CTA|10YAT-APG------L',
            'gen_freq': '15m', 'gen_market': 'RTPD'},
        'BE': {'country': 'Belgium', 'Code': 'CTA|BE',
            'ENTSOe_ID': 'CTY|10YBE----------2!CTA|10YBE----------2',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'BA': {'country': 'Bosnia and Herz. ', 'Code': 'CTA|BA',
            'ENTSOe_ID': 'CTY|10YBA-JPCC-----D!CTA|10YBA-JPCC-----D',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'BG': {'country': 'Bulgaria', 'Code': 'CTA|BG',
            'ENTSOe_ID': 'CTY|10YCA-BULGARIA-R!CTA|10YCA-BULGARIA-R',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'HR': {'country': 'Croatia', 'Code': 'CTA|HR',
            'ENTSOe_ID': 'CTY|10YHR-HEP------M!CTA|10YHR-HEP------M',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'CY': {'country': 'Cyprus', 'Code': 'CTA|CY',
            'ENTSOe_ID': 'CTY|10YCY-1001A0003J!CTA|10YCY-1001A0003J',
            'gen_freq': '30m', 'gen_market': 'RT5M'},
        'CZ': {'country': 'Czech Republic', 'Code': 'CTA|CZ',
            'ENTSOe_ID': 'CTY|10YCZ-CEPS-----N!CTA|10YCZ-CEPS-----N',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'PL-CZ': {'country': 'Czech Republic', 'Code': 'CTA|PL-CZ',
            'ENTSOe_ID': 'CTY|10YCZ-CEPS-----N!CTA|10YDOM-1001A082L',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'DK': {'country': 'Denmark', 'Code': 'CTA|DK',
            'ENTSOe_ID': 'CTY|10Y1001A1001A65H!CTA|10Y1001A1001A796',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'EE': {'country': 'Estonia', 'Code': 'CTA|EE',
            'ENTSOe_ID': 'CTY|10Y1001A1001A39I!CTA|10Y1001A1001A39I',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'MK': {'country': 'FYR Macedonia', 'Code': 'CTA|MK',
            'ENTSOe_ID': 'CTY|10YMK-MEPSO----8!CTA|10YMK-MEPSO----8',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'FI': {'country': 'Finland', 'Code': 'CTA|FI',
            'ENTSOe_ID': 'CTY|10YFI-1--------U!CTA|10YFI-1--------U',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'FR': {'country': 'France', 'Code': 'CTA|FR',
            'ENTSOe_ID': 'CTY|10YFR-RTE------C!CTA|10YFR-RTE------C',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'DE(50HzT)': {'country': 'Germany', 'Code': 'CTA|DE(50HzT)',
            'ENTSOe_ID': 'CTY|10Y1001A1001A83F!CTA|10YDE-VE-------2',
            'gen_freq': '15m', 'gen_market': 'RTPD'},
        'DE(Amprion)': {'country': 'Germany', 'Code': 'CTA|DE(Amprion)',
            'ENTSOe_ID': 'CTY|10Y1001A1001A83F!CTA|10YDE-RWENET---I',
            'gen_freq': '15m', 'gen_market': 'RTPD'},
        'DE(TenneT GER)': {'country': 'Germany', 'Code': 'CTA|DE(TenneT GER)',
            'ENTSOe_ID': 'CTY|10Y1001A1001A83F!CTA|10YDE-EON------1&',
            'gen_freq': '15m', 'gen_market': 'RTPD'},
        'DE(TransnetBW)': {'country': 'Germany', 'Code': 'CTA|DE(TransnetBW)',
            'ENTSOe_ID': 'CTY|10Y1001A1001A83F!CTA|10YDE-ENBW-----N',
            'gen_freq': '15m', 'gen_market': 'RTPD'},
        'GR': {'country': 'Greece', 'Code': 'CTA|GR',
            'ENTSOe_ID': 'CTY|10YGR-HTSO-----Y!CTA|10YGR-HTSO-----Y',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'HU': {'country': 'Hungary', 'Code': 'CTA|HU',
            'ENTSOe_ID': 'CTY|10YHU-MAVIR----U!CTA|10YHU-MAVIR----U',
            'gen_freq': '15m', 'gen_market': 'RTPD'},
        'IE': {'country': 'Ireland', 'Code': 'CTA|IE',
            'ENTSOe_ID': 'CTY|10YIE-1001A00010!CTA|10YIE-1001A00010',
            'gen_freq': '30m', 'gen_market': 'RT5M'},
        'IT': {'country': 'Italy', 'Code': 'CTA|IT',
            'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!CTA|10YIT-GRTN-----B',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'LV': {'country': 'Latvia', 'Code': 'CTA|LV',
            'ENTSOe_ID': 'CTY|10YLV-1001A00074!CTA|10YLV-1001A00074',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'LT': {'country': 'Lithuania', 'Code': 'CTA|LT',
            'ENTSOe_ID': 'CTY|10YLT-1001A0008Q!CTA|10YLT-1001A0008Q',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'LU': {'country': 'Luxembourg', 'Code': 'CTA|LU',
            'ENTSOe_ID': 'CTY|10YLU-CEGEDEL-NQ!CTA|10YLU-CEGEDEL-NQ',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'MT': {'country': 'Malta', 'Code': 'CTA|MT',
            'ENTSOe_ID': 'CTY|MT!CTA|Not+delivered+MT',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'MD': {'country': 'Moldavia', 'Code': 'CTA|MD',
            'ENTSOe_ID': 'CTY|MD!CTA|Not+delivered+MD',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'ME': {'country': 'Montenegro', 'Code': 'CTA|ME',
            'ENTSOe_ID': 'CTY|10YCS-CG-TSO---S!CTA|10YCS-CG-TSO---S',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'NL': {'country': 'Netherlands', 'Code': 'CTA|NL',
            'ENTSOe_ID': 'CTY|10YNL----------L!CTA|10YNL----------L',
            'gen_freq': '15m', 'gen_market': 'RTPD'},
        'NO': {'country': 'Norway', 'Code': 'CTA|NO',
            'ENTSOe_ID': 'CTY|10YNO-0--------C!CTA|10YNO-0--------C',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'PL': {'country': 'Poland', 'Code': 'CTA|PL',
            'ENTSOe_ID': 'CTY|10YPL-AREA-----S!CTA|10YPL-AREA-----S',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'PT': {'country': 'Portugal', 'Code': 'CTA|PT',
            'ENTSOe_ID': 'CTY|10YPT-REN------W!CTA|10YPT-REN------W',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'RO': {'country': 'Romania', 'Code': 'CTA|RO',
            'ENTSOe_ID': 'CTY|10YRO-TEL------P!CTA|10YRO-TEL------P',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'RU': {'country': 'Russia', 'Code': 'CTA|RU',
            'ENTSOe_ID': 'CTY|10YRO-TEL------P!CTA|10YRO-TEL------P',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'RU-KGD': {'country': 'Russia', 'Code': 'CTA|RU-KGD',
            'ENTSOe_ID': 'CTY|RU!CTA|10Y1001A1001A50U',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'RS': {'country': 'Serbia', 'Code': 'CTA|RS',
            'ENTSOe_ID': 'CTY|10YCS-SERBIATSOV!CTA|10YCS-SERBIATSOV',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'SK': {'country': 'Slovakia', 'Code': 'CTA|SK',
            'ENTSOe_ID': 'CTY|10YSK-SEPS-----K!CTA|10YSK-SEPS-----K',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'SI': {'country': 'Slovenia', 'Code': 'CTA|SI',
            'ENTSOe_ID': 'CTY|10YSI-ELES-----O!CTA|10YSI-ELES-----O',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'ES': {'country': 'Spain', 'Code': 'CTA|ES',
            'ENTSOe_ID': 'CTY|10YES-REE------0!CTA|10YES-REE------0',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'SE': {'country': 'Sweden', 'Code': 'CTA|SE',
            'ENTSOe_ID': 'CTY|10YSE-1--------K!CTA|10YSE-1--------K',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'CH': {'country': 'Switzerland', 'Code': 'CTA|CH',
            'ENTSOe_ID': 'CTY|10YCH-SWISSGRIDZ!CTA|10YCH-SWISSGRIDZ',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'TR': {'country': 'Turkey', 'Code': 'CTA|TR',
            'ENTSOe_ID': 'CTY|TR!CTA|10YTR-TEIAS----W',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'UA': {'country': 'Ukraine', 'Code': 'CTA|UA',
            'ENTSOe_ID': 'CTY|UA!CTA|10Y1001A1001A869',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'UA-WEPS': {'country': 'Ukraine', 'Code': 'CTA|UA-WEPS',
            'ENTSOe_ID': 'CTY|UA!CTA|10YUA-WEPS-----0',
            'gen_freq': '1hr', 'gen_market': 'RTHR'},
        'NIE': {'country': 'United Kingdom', 'Code': 'CTA|NIE',
            'ENTSOe_ID': 'CTY|GB!CTA|10Y1001A1001A016',
            'gen_freq': '30m', 'gen_market': 'RT5M'},
        'National Grid': {'country': 'United Kingdom', 'Code': 'CTA|National Grid',
            'ENTSOe_ID': 'CTY|GB!CTA|10YGB----------A',
            'gen_freq': '30m', 'gen_market': 'RT5M'},
        }

    fuels = {
        'Biomass  - Actual Aggregated [MW]': 'biomass',
        'Fossil Brown coal/Lignite  - Actual Aggregated [MW]': 'coal_brown',
        'Fossil Coal-derived gas  - Actual Aggregated [MW]': 'coal_gas',
        'Fossil Gas  - Actual Aggregated [MW]': 'natgas',
        'Fossil Hard coal  - Actual Aggregated [MW]': 'coal_hard',
        'Fossil Oil  - Actual Aggregated [MW]': 'oil',
        'Fossil Oil shale  - Actual Aggregated [MW]': 'oil_shale',
        'Fossil Peat  - Actual Aggregated [MW]': 'peat',
        'Geothermal  - Actual Aggregated [MW]': 'geo',
        'Hydro Pumped Storage  - Actual Aggregated [MW]' : 'hydro_ps',
        'Hydro Run-of-river and poundage  - Actual Aggregated [MW]': 'hydro_rorp',
        'Hydro Water Reservoir  - Actual Aggregated [MW]': 'hydro_wr',
        'Marine  - Actual Aggregated [MW]': 'marine',
        'Nuclear  - Actual Aggregated [MW]': 'nuclear',
        'Other renewable  - Actual Aggregated [MW]': 'renewable',
        'Solar  - Actual Aggregated [MW]': 'solar',
        'Waste  - Actual Aggregated [MW]': 'waste',
        'Wind Offshore  - Actual Aggregated [MW]': 'wind_offsh',
        'Wind Onshore  - Actual Aggregated [MW]': 'wind_onsh',
        'Other  - Actual Aggregated [MW]': 'other'
    }


    def get_load(self, control_area=None, latest=False, start_at=None, end_at=None,
                 forecast=False, **kwargs):
        self.handle_options(data='load', start_at=start_at, end_at=end_at, forecast=forecast,
                            latest=latest, control_area=control_area, **kwargs)

        pieces = []
        for date in self.dates():
            payload = self.construct_payload(date)
            url = self.base_url + self.export_endpoint_load
            response = self.fetch_entsoe(url, payload)
            day_df = self.parse_load_response(response)
            pieces.append(day_df)

        df = pd.concat(pieces)
        sliced = self.slice_times(df)
        return self.serialize_faster(sliced)


    def get_generation(self, control_area=None, latest=False, yesterday=False, start_at=False, 
                       end_at=False, forecast=False, **kwargs):
        self.handle_options(data='gen', start_at=start_at, end_at=end_at, yesterday=yesterday, 
                            latest=latest, control_area=control_area, forecast=False, **kwargs)

        pieces = []
        for date in self.dates():
            payload = self.construct_payload(date)
            url = self.base_url + self.export_endpoint_gen
            response = self.fetch_entsoe(url, payload)
            day_df = self.parse_gen_response(response)
            pieces.append(day_df)

        df = pd.concat(pieces)
        sliced = self.slice_times(df)
        return self.serialize_faster(sliced)
        

    def handle_options(self, **kwargs):
        # regular handle options
        super(EUClient, self).handle_options(**kwargs)

        # if latest is True
        if self.options.get('latest', None):
            self.options.update(start_at=datetime.now(pytz.utc) - timedelta(days=1),
                                end_at=datetime.now(pytz.utc))

        # workaround for base.handle_options setting forecast to false if end_at too far in past
        if kwargs['forecast']:
            self.options['forecast'] = True

    def auth(self):
        if not getattr(self, 'session', None):
            self.session = requests.Session()

        payload = {'username': environ['ENTSOe_USERNAME'],
                   'password': environ['ENTSOe_PASSWORD'],
                   'url': '/dashboard/show'}

        # Fake an ajax login to get the cookie
        r = self.session.post(self.base_url + 'login', params=payload,
                              headers={'X-Ajax-call': 'true'})

        msg = r.text
        if msg == 'ok':
            return True
        elif msg == 'non_exists_user_or_bad_password':
            # TODO throw error
            return 'Wrong email or password'
        elif msg == 'not_human':
            return 'This account is not allowed to access web portal'
        elif msg == 'suspended_use':
            return 'User is suspended'
        else:
            return 'Unknown error:' + str(msg)

    def fetch_entsoe(self, url, payload, count=0):
        if not getattr(self, 'session', None):
            self.auth()

        r = self.request(url, params=payload)
        # TODO error checking

        if len(r.text) == 0:
            if count > 3:  # try 3 times to get response
                LOGGER.warn('Request failed, no response found after %i attempts' % count)
                return False
            # throttled
            sleep(5)
            return self.fetch_entsoe(url, payload, count + 1)
        if 'UNKNOWN_EXCEPTION' in r.text:
            LOGGER.warn('UNKNOWN EXCEPTION')
            return False
        return r.text

    def construct_payload(self, date):
        # format date
        payload = {}
        format_str = '%d.%m.%Y'
        date_str = date.strftime(format_str) + ' 00:00|UTC|DAY'

        TSO_ID = self.get_tso_id()

        if self.options['data'] == 'load':
            payload.update({'biddingZone.values': TSO_ID,})
        elif self.options['data'] == 'gen':
            date_str += 'TIMERANGE'
            payload.update({
                'productionType.values': 'B01, B02, B03, B04, B05, B06, B07, B08, B09, B10, B11, B12, B13, B14, B15, B16, B17, B18, B19, B20',
                'datepicker-day-offset-select-dv-date-from_input': 'D',
                'dateTime.endDateTime': date_str,
                'area.values': TSO_ID,
            })

        payload.update({
            'name': '',
            'defaultValue': 'false',
            'viewType': 'TABLE',
            'areaType': 'CTA',
            'atch': 'false',
            'dateTime.dateTime': date_str,
            'dateTime.timezone': 'UTC',
            'dateTime.timezone_input': 'UTC',
            'exportType': 'CSV',
            'dataItem': 'ALL',
            'timeRange': 'DEFAULT',
        })

        return payload

    def parse_load_response(self, response):
        df = pd.read_csv(StringIO(response))

        # get START_TIME_UTC as tz-aware datetime
        df['START_TIME_UTC'], df['END_TIME_UTC'] = zip(
            *df['Time (UTC)'].apply(lambda x: x.split('-')))

        # Why do these methods only work on Index and not Series?
        df.set_index(df.START_TIME_UTC, inplace=True)
        df.index = pd.to_datetime(df.index, utc=True, format='%d.%m.%Y %H:%M ')
        df.index.set_names('timestamp', inplace=True)

        # find column name and choose which to return and which to drop
        (forecast_load_col, ) = [c for c in df.columns if 'Day-ahead Total Load Forecast [MW]' in c]
        (actual_load_col, ) = [c for c in df.columns if 'Actual Total Load [MW]' in c]
        if self.options['forecast']:
            load_col = forecast_load_col
            drop_load_col = actual_load_col
        else:
            load_col = actual_load_col
            drop_load_col = forecast_load_col

        # rename columns for list of dicts
        rename_d = {load_col: 'load_MW'}
        df.rename(columns=rename_d, inplace=True)
        drop_col = ['Time (UTC)', 'END_TIME_UTC', 'START_TIME_UTC', drop_load_col]
        df.drop(drop_col, axis=1, inplace=True)

        # drop nan rows
        df.replace('-', np.nan, inplace=True)
        df.dropna(subset=['load_MW'], inplace=True)

        # Add columns
        df['ba_name'] = self.options['control_area']
        df['freq'] = '1hr'
        df['market'] = 'RTHR'  # not necessarily appropriate terminology

        return df

    def parse_gen_response(self, response):
        df = pd.read_csv(StringIO(response))

        # get START_TIME_UTC as tz-aware datetime
        df['START_TIME_UTC'], df['END_TIME_UTC'] = zip(*df['MTU'].apply(lambda x: x.split('-')))
        df.set_index('START_TIME_UTC', inplace=True)
        df.index = pd.to_datetime(df.index, utc=True, format='%d.%m.%Y %H:%M ')
        df.index.set_names('timestamp', inplace=True)

        # rename all columns
        df.rename(columns=self.fuels, inplace=True)
        drop_col = ['MTU', 'END_TIME_UTC', 'Area']
        df.drop(drop_col, axis=1, inplace=True)

        # filter out consumption columns
        fuel_columns = [self.fuels[x] for x in self.fuels]
        allowed_cols = ['timestamp']
        allowed_cols.extend(fuel_columns)
        df = df.filter(allowed_cols, axis=1)

        # drop nan rows
        print('Dropping emtpy rows in columns: %s' % fuel_columns)
        df.replace('-', np.nan, inplace=True)
        df.dropna(subset=fuel_columns, how='all', inplace=True)

        # Add columns
        df['ba_name'] = self.options['control_area']
        df['freq'] = self.CONTROL_AREAS[self.options['control_area']]['gen_freq']
        df['market'] = self.CONTROL_AREAS[self.options['control_area']]['gen_market']

        return df

    def get_tso_id(self):
        # TSO ID from control area code
        try:
            return self.CONTROL_AREAS[self.options['control_area']]['ENTSOe_ID']
        except KeyError:
            msg = 'Control area code not found for %s. Options are %s' % (self.options['control_area'],
                                                                          sorted(self.CONTROL_AREAS.keys()))
            raise ValueError(msg)


