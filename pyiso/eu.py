from pyiso.base import BaseClient
import requests
import pandas as pd
from io import StringIO
from time import sleep
from datetime import datetime, timedelta
import pytz
from os import environ


class EUClient(BaseClient):
    NAME = 'EU'

    base_url = '...'
    TZ_NAME = 'UTC'

    base_url = 'https://transparency.entsoe.eu/'
    export_endpoint = 'load-domain/r2/totalLoadR2/export'

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

        payload = {'j_username': environ['ENTSOe_USERNAME'],
                   'j_password': environ['ENTSOe_PASSWORD']}

        # Fake an ajax login to get the cookie
        r = self.session.post(self.base_url + 'j_spring_security_check', params=payload,
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

        r = self.session.get(url, params=payload)
        # TODO error checking
        if len(r.text) == 0:
            if count > 3:  # try 3 times to get response
                self.logger.warn('Request failed, no response found after %i attempts' % count)
                return False
            # throttled
            sleep(5)
            return self.fetch_entsoe(url, payload, count + 1)
        if 'UNKNOWN_EXCEPTION' in r.text:
            self.logger.warn('UNKNOWN EXCEPTION')
            return False
        return r.text

    def construct_payload(self):
        format_str = '%d.%m.%Y'
        date_str = self.options['start_at'].strftime(format_str) + ' 00:00|UTC|DAY'
        (TSO_ID, ) = [
            i['ENTSOe_ID'] for i in control_areas if i['Code'] == self.options['control_area']]

        payload = {
            'name': '',
            'defaultValue': 'false',
            'viewType': 'TABLE',
            'areaType': 'CTA',
            'atch': 'false',
            'dateTime.dateTime': date_str,
            'biddingZone.values': TSO_ID,
            'dateTime.timezone': 'UTC',
            'dateTime.timezone_input': 'UTC',
            'exportType': 'CSV',
            'dataItem': 'ALL',
            'timeRange': 'DEFAULT',
        }
        return payload

    def parse_load_response(self, response):
        df = pd.read_csv(StringIO(response))

        # get START_TIME_UTC as tz-aware datetime
        df['START_TIME_UTC'], df['END_TIME_UTC'] = zip(
            *df['Time (UTC)'].apply(lambda x: x.split('-')))

        # Why do these methods only work on Index and not Series?
        df.set_index(df.START_TIME_UTC, inplace=True)
        df.index = pd.to_datetime(df.index, utc=True, format='%d.%m.%Y %H:%M ')
        df.START_TIME_UTC = df.index.to_pydatetime()

        # find column name and choose which to return and which to drop
        (forecast_load_col, ) = [c for c in df.columns if 'Day-ahead Total Load Forecast [MW]' in c]
        (actual_load_col, ) = [c for c in df.columns if 'Actual Total Load [MW]' in c]
        if self.options['forecast']:
            load_col = forecast_load_col
            drop_load_col = actual_load_col

            # drop extra columns
            df = df[self.options['start_at']:self.options['end_at']]
        else:
            load_col = actual_load_col
            drop_load_col = forecast_load_col

        # rename columns for list of dicts return
        rename_d = {load_col: 'load_MW',
                    'START_TIME_UTC': 'timestamp',
                    }
        df.rename(columns=rename_d, inplace=True)
        drop_col = ['Time (UTC)', 'END_TIME_UTC', drop_load_col]
        df.drop(drop_col, axis=1, inplace=True)

        # drop nan rows
        df.dropna(subset=['load_MW'], inplace=True)

        # Add columns
        df['ba_name'] = self.options['control_area']
        df['freq'] = '1hr'
        df['market'] = 'RTHR'  # not necessarily appropriate terminology

        if self.options['latest']:
            # drop all but the latest record not in the future
            now = datetime.now(pytz.utc)
            df = df[:now].iloc[-1]
            return [df.to_dict()]

        return df.to_dict('records')

    def get_load(self, control_area='CTA|IT', latest=False, start_at=None, end_at=None,
                 forecast=False, **kwargs):
        self.handle_options(data='load', start_at=start_at, end_at=end_at, forecast=forecast,
                            latest=latest, control_area=control_area, **kwargs)

        payload = self.construct_payload()
        url = self.base_url + self.export_endpoint
        response = self.fetch_entsoe(url, payload)

        return self.parse_load_response(response)


control_areas = [
    {'country': 'Albania', 'Code': 'CTA|AL',
        'ENTSOe_ID': 'CTY|10YAL-KESH-----5!CTA|10YAL-KESH-----5'},
    {'country': 'Austria', 'Code': 'CTA|AT',
        'ENTSOe_ID': 'CTY|10YAT-APG------L!CTA|10YAT-APG------L'},
    {'country': 'Belgium', 'Code': 'CTA|BE',
        'ENTSOe_ID': 'CTY|10YBE----------2!CTA|10YBE----------2'},
    {'country': 'Bosnia and Herz. ', 'Code': 'CTA|BA',
        'ENTSOe_ID': 'CTY|10YBA-JPCC-----D!CTA|10YBA-JPCC-----D'},
    {'country': 'Bulgaria', 'Code': 'CTA|BG',
        'ENTSOe_ID': 'CTY|10YCA-BULGARIA-R!CTA|10YCA-BULGARIA-R'},
    {'country': 'Croatia', 'Code': 'CTA|HR',
        'ENTSOe_ID': 'CTY|10YHR-HEP------M!CTA|10YHR-HEP------M'},
    {'country': 'Cyprus', 'Code': 'CTA|CY',
        'ENTSOe_ID': 'CTY|10YCY-1001A0003J!CTA|10YCY-1001A0003J'},
    {'country': 'Czech Republic', 'Code': 'CTA|CZ',
        'ENTSOe_ID': 'CTY|10YCZ-CEPS-----N!CTA|10YCZ-CEPS-----N'},
    {'country': 'Czech Republic', 'Code': 'CTA|PL-CZ',
        'ENTSOe_ID': 'CTY|10YCZ-CEPS-----N!CTA|10YDOM-1001A082L'},
    {'country': 'Denmark', 'Code': 'CTA|DK',
        'ENTSOe_ID': 'CTY|10Y1001A1001A65H!CTA|10Y1001A1001A796'},
    {'country': 'Estonia', 'Code': 'CTA|EE',
        'ENTSOe_ID': 'CTY|10Y1001A1001A39I!CTA|10Y1001A1001A39I'},
    {'country': 'FYR Macedonia', 'Code': 'CTA|MK',
        'ENTSOe_ID': 'CTY|10YMK-MEPSO----8!CTA|10YMK-MEPSO----8'},
    {'country': 'Finland', 'Code': 'CTA|FI',
        'ENTSOe_ID': 'CTY|10YFI-1--------U!CTA|10YFI-1--------U'},
    {'country': 'France', 'Code': 'CTA|FR',
        'ENTSOe_ID': 'CTY|10YFR-RTE------C!CTA|10YFR-RTE------C'},
    {'country': 'Germany', 'Code': 'CTA|DE(50HzT)',
        'ENTSOe_ID': 'CTY|10Y1001A1001A83F!CTA|10YDE-VE-------2'},
    {'country': 'Germany', 'Code': 'CTA|DE(Amprion)',
        'ENTSOe_ID': 'CTY|10Y1001A1001A83F!CTA|10YDE-RWENET---I'},
    {'country': 'Germany', 'Code': 'CTA|DE(TenneT GER)',
        'ENTSOe_ID': 'CTY|10Y1001A1001A83F!CTA|10YDE-EON------1&'},
    {'country': 'Germany', 'Code': 'CTA|DE(TransnetBW)',
        'ENTSOe_ID': 'CTY|10Y1001A1001A83F!CTA|10YDE-ENBW-----N'},
    {'country': 'Greece', 'Code': 'CTA|GR',
        'ENTSOe_ID': 'CTY|10YGR-HTSO-----Y!CTA|10YGR-HTSO-----Y'},
    {'country': 'Hungary', 'Code': 'CTA|HU',
        'ENTSOe_ID': 'CTY|10YHU-MAVIR----U!CTA|10YHU-MAVIR----U'},
    {'country': 'Ireland', 'Code': 'CTA|IE',
        'ENTSOe_ID': 'CTY|10YIE-1001A00010!CTA|10YIE-1001A00010'},
    {'country': 'Italy', 'Code': 'CTA|IT',
        'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!CTA|10YIT-GRTN-----B'},
    {'country': 'Latvia', 'Code': 'CTA|LV',
        'ENTSOe_ID': 'CTY|10YLV-1001A00074!CTA|10YLV-1001A00074'},
    {'country': 'Lithuania', 'Code': 'CTA|LT',
        'ENTSOe_ID': 'CTY|10YLT-1001A0008Q!CTA|10YLT-1001A0008Q'},
    {'country': 'Luxembourg', 'Code': 'CTA|LU',
        'ENTSOe_ID': 'CTY|10YLU-CEGEDEL-NQ!CTA|10YLU-CEGEDEL-NQ'},
    {'country': 'Malta', 'Code': 'CTA|MT',
        'ENTSOe_ID': 'CTY|MT!CTA|Not+delivered+MT'},
    {'country': 'Moldavia', 'Code': 'CTA|MD',
        'ENTSOe_ID': 'CTY|MD!CTA|Not+delivered+MD'},
    {'country': 'Montenegro', 'Code': 'CTA|ME',
        'ENTSOe_ID': 'CTY|10YCS-CG-TSO---S!CTA|10YCS-CG-TSO---S'},
    {'country': 'Netherlands', 'Code': 'CTA|NL',
        'ENTSOe_ID': 'CTY|10YNL----------L!CTA|10YNL----------L'},
    {'country': 'Norway', 'Code': 'CTA|NO',
        'ENTSOe_ID': 'CTY|10YNO-0--------C!CTA|10YNO-0--------C'},
    {'country': 'Poland', 'Code': 'CTA|PL',
        'ENTSOe_ID': 'CTY|10YPL-AREA-----S!CTA|10YPL-AREA-----S'},
    {'country': 'Portugal', 'Code': 'CTA|PT',
        'ENTSOe_ID': 'CTY|10YPT-REN------W!CTA|10YPT-REN------W'},
    {'country': 'Romania', 'Code': 'CTA|RO',
        'ENTSOe_ID': 'CTY|10YRO-TEL------P!CTA|10YRO-TEL------P'},
    {'country': 'Russia', 'Code': 'CTA|RU',
        'ENTSOe_ID': 'CTY|10YRO-TEL------P!CTA|10YRO-TEL------P'},
    {'country': 'Russia', 'Code': 'CTA|RU-KGD',
        'ENTSOe_ID': 'CTY|RU!CTA|10Y1001A1001A50U'},
    {'country': 'Serbia', 'Code': 'CTA|RS',
        'ENTSOe_ID': 'CTY|10YCS-SERBIATSOV!CTA|10YCS-SERBIATSOV'},
    {'country': 'Slovakia', 'Code': 'CTA|SK',
        'ENTSOe_ID': 'CTY|10YSK-SEPS-----K!CTA|10YSK-SEPS-----K'},
    {'country': 'Slovenia', 'Code': 'CTA|SI',
        'ENTSOe_ID': 'CTY|10YSI-ELES-----O!CTA|10YSI-ELES-----O'},
    {'country': 'Spain', 'Code': 'CTA|ES',
        'ENTSOe_ID': 'CTY|10YES-REE------0!CTA|10YES-REE------0'},
    {'country': 'Sweden', 'Code': 'CTA|SE',
        'ENTSOe_ID': 'CTY|10YSE-1--------K!CTA|10YSE-1--------K'},
    {'country': 'Switzerland', 'Code': 'CTA|CH',
        'ENTSOe_ID': 'CTY|10YCH-SWISSGRIDZ!CTA|10YCH-SWISSGRIDZ'},
    {'country': 'Turkey', 'Code': 'CTA|TR',
        'ENTSOe_ID': 'CTY|TR!CTA|10YTR-TEIAS----W'},
    {'country': 'Ukraine', 'Code': 'CTA|UA',
        'ENTSOe_ID': 'CTY|UA!CTA|10Y1001A1001A869'},
    {'country': 'Ukraine', 'Code': 'CTA|UA-WEPS',
        'ENTSOe_ID': 'CTY|UA!CTA|10YUA-WEPS-----0'},
    {'country': 'United Kingdom', 'Code': 'CTA|NIE',
        'ENTSOe_ID': 'CTY|GB!CTA|10Y1001A1001A016'},
    {'country': 'United Kingdom', 'Code': 'CTA|National Grid',
        'ENTSOe_ID': 'CTY|GB!CTA|10YGB----------A'},
    ]

