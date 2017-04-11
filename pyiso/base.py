from collections import namedtuple
from dateutil.parser import parse as dateutil_parse
from datetime import datetime, timedelta
import pytz
import requests
import pandas as pd
import zipfile
from io import StringIO, BytesIO
from time import sleep
from pyiso import LOGGER
from pytz import AmbiguousTimeError
import ssl
import certifi


try:
    from urllib2 import urlopen
except ImportError:
    from urllib.request import urlopen  # Changed from urllib2 for python3.x

# named tuple for time period interval labels
IntervalChoices = namedtuple('IntervalChoices', ['hourly', 'fivemin', 'tenmin', 'fifteenmin', 'na', 'dam'])

# list of fuel choices
FUEL_CHOICES = ['biogas', 'biomass', 'coal', 'geo', 'hydro',
                'natgas', 'nonwind', 'nuclear', 'oil', 'other',
                'refuse', 'renewable', 'smhydro', 'solar', 'solarpv',
                'solarth', 'thermal', 'wind', 'fossil', 'dual']


class BaseClient(object):
    """
    Base class for scraper/parser clients.
    """
    # choices for market and frequency interval labels
    MARKET_CHOICES = IntervalChoices(hourly='RTHR', fivemin='RT5M', tenmin='RT5M', fifteenmin='RTPD', na='RT5M', dam='DAHR')
    FREQUENCY_CHOICES = IntervalChoices(hourly='1hr', fivemin='5m', tenmin='10m', fifteenmin='15m', na='n/a', dam='1hr')

    # timezone
    TZ_NAME = 'UTC'

    # name
    NAME = ''

    TIMEOUT_SECONDS = 20

    def __init__(self, timeout_seconds=20):
        # will hold query options
        self.options = {}

        # connection timeout
        self.timeout_seconds = timeout_seconds

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        """
        Scrape and parse generation fuel mix data.

        :param bool latest: If True, only get the generation mix at the one most recent available time point.
           Available for all regions.
        :param bool yesterday: If True, get the generation mix for every time point yesterday.
           Not available for all regions.
        :param datetime start_at: If the datetime is naive, it is assummed to be in the timezone of the Balancing Authority. The timestamp of all returned data points will be greater than or equal to this value.
           If using, must provide both ``start_at`` and ``end_at`` parameters.
           Not available for all regions.
        :param datetime end_at: If the datetime is naive, it is assummed to be in the timezone of the Balancing Authority. The timestamp of all returned data points will be less than or equal to this value.
           If using, must provide both ``start_at`` and ``end_at`` parameters.
           Not available for all regions.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, fuel_name, gen_MW]``.
           Timestamps are in UTC.
        :rtype: list

        """
        raise NotImplementedError('Derived classes must implement the get_generation method.')

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        """
        Scrape and parse load data.

        :param bool latest: If True, only get the load at the one most recent available time point.
           Available for all regions.
        :param bool yesterday: If True, get the load for every time point yesterday.
           Not available for all regions.
        :param datetime start_at: If the datetime is naive, it is assummed to be in the timezone of the Balancing Authority. The timestamp of all returned data points will be greater than or equal to this value.
           If using, must provide both ``start_at`` and ``end_at`` parameters.
           Not available for all regions.
        :param datetime end_at: If the datetime is naive, it is assummed to be in the timezone of the Balancing Authority. The timestamp of all returned data points will be less than or equal to this value.
           If using, must provide both ``start_at`` and ``end_at`` parameters.
           Not available for all regions.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, load_MW]``.
           Timestamps are in UTC.
        :rtype: list

        """
        raise NotImplementedError('Derived classes must implement the get_load method.')

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        """
        Scrape and parse import/export data.
        Value is net export (export - import), can be positive or negative.

        :param bool latest: If True, only get the trade at the one most recent available time point.
           Available for all regions.
        :param bool yesterday: If True, get the trade for every time point yesterday.
           Not available for all regions.
        :param datetime start_at: If the datetime is naive, it is assummed to be in the timezone of the Balancing Authority. The timestamp of all returned data points will be greater than or equal to this value.
           If using, must provide both ``start_at`` and ``end_at`` parameters.
           Not available for all regions.
        :param datetime end_at: If the datetime is naive, it is assummed to be in the timezone of the Balancing Authority. The timestamp of all returned data points will be less than or equal to this value.
           If using, must provide both ``start_at`` and ``end_at`` parameters.
           Not available for all regions.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, net_exp_MW]``.
           Timestamps are in UTC.
        :rtype: list

        """
        raise NotImplementedError('Derived classes must implement the get_trade method.')

    def get_lmp(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        """
        Scrape and parse location marginal price data.
        To request a specific LMP node, include kwarg `node_id`.

        :param bool latest: If True, only get LMP at the one most recent available time point.
           Available for all regions.
        :param bool yesterday: If True, get LMP for every time point yesterday.
           Not available for all regions.
        :param datetime start_at: If the datetime is naive, it is assummed to be in the timezone of the Balancing Authority. The timestamp of all returned data points will be greater than or equal to this value.
           If using, must provide both ``start_at`` and ``end_at`` parameters.
           Not available for all regions.
        :param datetime end_at: If the datetime is naive, it is assummed to be in the timezone of the Balancing Authority. The timestamp of all returned data points will be less than or equal to this value.
           If using, must provide both ``start_at`` and ``end_at`` parameters.
           Not available for all regions.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, lmp, lmp_type]``.
           Timestamps are in UTC.
        :rtype: list

        """
        raise NotImplementedError('Derived classes must implement the get_lmp method.')

    def handle_options(self, **kwargs):
        """
        Process and store keyword argument options.
        """
        self.options = kwargs

        # check start_at and end_at args
        if self.options.get('start_at', None) and self.options.get('end_at', None):
            assert self.options['start_at'] < self.options['end_at']
            self.options['start_at'] = self.utcify(self.options['start_at'])
            self.options['end_at'] = self.utcify(self.options['end_at'])
            self.options['sliceable'] = True
            self.options['latest'] = False

            # force forecast to be True if end_at is in the future
            if self.options['end_at'] > pytz.utc.localize(datetime.utcnow()):
                self.options['forecast'] = True
            else:
                self.options['forecast'] = False

        # set start_at and end_at for yesterday in local time
        elif self.options.get('yesterday', None):
            local_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone(self.TZ_NAME))
            self.options['end_at'] = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
            self.options['start_at'] = self.options['end_at'] - timedelta(days=1)
            self.options['sliceable'] = True
            self.options['latest'] = False
            self.options['forecast'] = False

        # set start_at and end_at for today+tomorrow in local time
        elif self.options.get('forecast', None):
            local_now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone(self.TZ_NAME))
            self.options['start_at'] = local_now.replace(microsecond=0)
            self.options['end_at'] = self.options['start_at'] + timedelta(days=2)
            self.options['sliceable'] = True
            self.options['latest'] = False
            self.options['forecast'] = True

        else:
            self.options['sliceable'] = False
            self.options['forecast'] = False

    def utcify(self, local_ts_str, tz_name=None, is_dst=None):
        """
        Convert a datetime or datetime string to UTC.

        Uses the default behavior of dateutil.parser.parse to convert the string to a datetime object.

        :param string local_ts: The local datetime to be converted.
        :param string tz_name: If local_ts is naive, it is assumed to be in timezone tz. If tz is not provided, the client's default timezone is used.
        :param bool is_dst: If provided, explicitly set daylight savings time as True or False.
        :return: Datetime in UTC.
        :rtype: datetime
        """
        # set up tz
        if tz_name is None:
            tz = pytz.timezone(self.TZ_NAME)
        else:
            tz = pytz.timezone(tz_name)

        # parse
        try:
            local_ts = dateutil_parse(local_ts_str)
        except (AttributeError, TypeError):  # already parsed
            local_ts = local_ts_str

        # localize
        if local_ts.tzinfo is None:  # unaware
            if is_dst is None:
                aware_local_ts = tz.localize(local_ts)
            else:
                aware_local_ts = tz.localize(local_ts, is_dst=is_dst)
        else:  # already aware
            aware_local_ts = local_ts

        # convert to utc
        aware_utc_ts = aware_local_ts.astimezone(pytz.utc)

        # return
        return aware_utc_ts

    def parse_row(self, row, delimiter=',', datetime_col=None, drop_vals=None):
        raw_vals = row.split(delimiter)
        if datetime_col is not None:
            raw_vals[datetime_col] = self.utcify(raw_vals[datetime_col])

        if drop_vals is not None:
            cleaned_vals = [val for val in raw_vals if val not in drop_vals]
        else:
            cleaned_vals = raw_vals

        return cleaned_vals

    def fetch_xls(self, url):
        # follow http://stackoverflow.com/questions/27835619/ssl-certificate-verify-failed-error
        context = ssl.create_default_context(cafile=certifi.where())
        socket = urlopen(url, context=context)
        xd = pd.ExcelFile(socket)
        return xd

    def request(self, url, mode='get', retry_sec=5, retries_remaining=5, **kwargs):
        """
        Get or post to a URL with the provided kwargs.
        Returns the response, or None if an error was encountered.
        If the mode is not 'get' or 'post', raises ValueError.
        """
        # check args
        allowed_modes = ['get', 'post']
        if mode not in allowed_modes:
            raise ValueError('Invalid request mode %s' % mode)

        # check for session
        try:
            session = getattr(self, 'session')
        except AttributeError:
            self.session = requests.Session()
            session = self.session

        # carry out request
        try:
            response = getattr(session, mode)(url, verify=True,
                                              timeout=self.timeout_seconds,
                                              **kwargs)
        # except requests.exceptions.ChunkedEncodingError as e:
        #     # JSON incomplete or not found
        #     msg = '%s: chunked encoding error for %s, %s:\n%s' % (self.NAME, url, kwargs, e)
        #     LOGGER.error(msg)
        #     return None
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            # eg max retries exceeded
            msg = '%s: connection error for %s, %s:\n%s' % (self.NAME, url, kwargs, e)
            LOGGER.error(msg)
            return None
        # except requests.exceptions.RequestException:
        #     msg = '%s: request exception for %s, %s:\n%s' % (self.NAME, url, kwargs, e)
        #     LOGGER.error(msg)
        #     return None

        if response.status_code == 200:
            # success
            LOGGER.debug('%s: request success for %s, %s with cache hit %s' % (self.NAME, url, kwargs, getattr(response, 'from_cache', None)))

        elif response.status_code == 429:
            if retries_remaining > 0:
                # retry on throttle
                LOGGER.warn('%s: retrying in %d seconds (%d retries remaining), throttled for %s, %s' % (self.NAME, retry_sec, retries_remaining, url, kwargs))
                sleep(retry_sec)
                retries_remaining -= 1
                return self.request(url, mode=mode,
                                    retry_sec=retry_sec*2, retries_remaining=retries_remaining,
                                    **kwargs)
            else:
                # exhausted retries
                LOGGER.warn('%s: exhausted retries for %s, %s' % (self.NAME, url, kwargs))
                return None

        else:
            # non-throttle error
            LOGGER.error('%s: request failure with code %s for %s, %s' % (self.NAME, response.status_code, url, kwargs))

        return response

    def unzip(self, content):
        """
        Unzip encoded data.
        Returns the unzipped content as an array of strings, each representing one file's content
        or returns None if an error was encountered.
        ***Previous behavior: Only returned the content from the first file***
        """
        # create zip file
        try:
            filecontent = BytesIO(content)
        except TypeError:
            filecontent = StringIO(content)

        try:
            # have zipfile
            z = zipfile.ZipFile(filecontent)
        except zipfile.BadZipfile:
            LOGGER.error('%s: unzip failure for content:\n%s' % (self.NAME, content))
            return None

        # have unzipped content
        unzipped = [z.read(thisfile) for thisfile in z.namelist()]
        z.close()

        # return
        return unzipped

    def parse_to_df(self, filelike, mode='csv', header_names=None, sheet_names=None, **kwargs):
        """
        Parse a delimited or excel file from the provided content and return a DataFrame.

        Any extra kwargs are passed to the appropriate pandas parser;
        read the pandas docs for details.
        Recommended kwargs: skiprows, parse_cols, header.

        :param filelike: string-like or filelike object containing formatted data
        :paramtype: string or file
        :param string mode: Choose from 'csv' or 'xls'. Default 'csv'.
            If 'csv', kwargs are passed to pandas.read_csv.
        :param list header_names: List of strings to use as column names.
            If provided, this will override the header extracted by pandas.
        :param list sheet_names: List of strings for excel sheet names to read.
            Default is to concatenate all sheets.
        """
        # check mode
        allowed_modes = ['csv', 'xls']
        if mode not in allowed_modes:
            raise ValueError('Invalid mode %s' % mode)

        # do csv/tsv
        if mode == 'csv':
            # convert string to filelike if needed
            try:
                filelike.closed
            except AttributeError:  # string, unicode, etc
                try:
                    filelike = BytesIO(filelike)  # This was changed from StringIO for Python 3.x
                except TypeError:
                    filelike = StringIO(filelike)

            # read csv
            df = pd.read_csv(filelike, **kwargs)

        # do xls
        elif mode == 'xls':
            # parse_dates is not implemented for excel, so pop it off
            if 'parse_dates' in kwargs:
                parse_dates = kwargs.pop('parse_dates')
            else:
                parse_dates = False

            pieces = []
            for sheet in sheet_names:
                pieces.append(filelike.parse(sheet, **kwargs))
            df = pd.concat(pieces)

            # parse date index

            df.index = pd.to_datetime(df.index, infer_datetime_format=True, errors='coerce')

        # set names
        if header_names is not None:
            df.columns = header_names

        # drop na
        df = df.dropna()

        return df

    def utcify_index(self, local_index, tz_name=None, tz_col=None):
        """
        Convert a DateTimeIndex to UTC.

        :param DateTimeIndex local_index: The local DateTimeIndex to be converted.
        :param string tz_name: If local_ts is naive, it is assumed to be in timezone tz.
            If tz is not provided, the client's default timezone is used.
        :return: DatetimeIndex in UTC.
        :rtype: DatetimeIndex
        """
        # set up tz
        if tz_name is None:
            tz_name = self.TZ_NAME

        # use tz col if given
        if tz_col is not None:
            # it seems like we shouldn't have to iterate, but all the smart ways aren't working
            aware_utc_list = []
            for i in range(len(local_index)):
                try:
                    aware_local_ts = pytz.timezone(tz_col[i]).localize(local_index[i])
                except pytz.UnknownTimeZoneError:
                    # fall back to local ts
                    aware_local_ts = pytz.timezone(tz_name).localize(local_index[i])

                # utcify
                aware_utc_ts = self.utcify(aware_local_ts)
                aware_utc_list.append(aware_utc_ts)

            # indexify
            aware_utc_index = pd.DatetimeIndex(aware_utc_list)

        else:
            # localize
            try:
                aware_local_index = local_index.tz_localize(tz_name)
            except AmbiguousTimeError as e:
                LOGGER.debug(e)
                aware_local_index = local_index.tz_localize(tz_name, ambiguous='infer')
            except TypeError as e:
                # already aware
                LOGGER.debug(e)
                aware_local_index = local_index

            # convert to utc
            aware_utc_index = aware_local_index.tz_convert('UTC')

        # return
        return aware_utc_index

    def slice_times(self, df, options=None):
        if options is None:
            options = self.options

        if len(df) == 0:
            # if empty, end here
            return df

        if options.get('latest', None):
            start_at = df.iloc[-1].name
            end_at = start_at
        else:
            try:
                start_at = options['start_at']
                end_at = options['end_at']
            except KeyError:
                raise ValueError('Slicing by time requires start_at and end_at')

        # sort before truncate eliminates DST KeyError
        sorteddf = df.sort_index()
        sliced = sorteddf.truncate(before=start_at, after=end_at)

        # return
        return sliced

    def unpivot(self, df):
        return df.stack().reset_index(level=1)

    def serialize(self, df, header, extras={}):
        data = []

        for row in df.itertuples():
            dp = dict(zip(header, list(row)))
            dp.update(extras)
            data.append(dp)

        return data

    def serialize_faster(self, df, extras={}, drop_index=False):
        """DF is a DataFrame with DateTimeIndex and columns fuel_type and gen_MW (or load_mW).
        Index and columns are already properly named."""
        df = df.reset_index(drop=drop_index)
        for key in extras:
            df[key] = extras[key]
        return df.to_dict(orient='records')

    def local_now(self):
        """Returns a tz-aware datetime equal to the current moment, in the local timezone"""
        return pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone(self.TZ_NAME))

    def dates(self):
        """Returns a list of dates in local time"""
        # set up storage
        dates = []

        # if latest, use date in local time
        if self.options['latest']:
            local_now = self.local_now()
            if local_now.date() != (local_now - timedelta(minutes=30)).date():
                dates.append((local_now - timedelta(minutes=30)).date())
            dates.append(local_now.date())

        # if start and end, use all dates in range
        elif self.options['start_at'] and self.options['end_at']:
            local_start = self.options['start_at'].astimezone(pytz.timezone(self.TZ_NAME))
            local_end = self.options['end_at'].astimezone(pytz.timezone(self.TZ_NAME))
            this_date = local_start.date()
            while this_date <= local_end.date():
                dates.append(this_date)
                this_date += timedelta(days=1)

        # have to have some sort of dates
        else:
            raise ValueError(
                'Either latest must be True, or start_at and end_at must both be provided.')

        # return
        return dates
