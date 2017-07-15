try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
from datetime import datetime
import pytz
from pyiso.base import BaseClient
from pandas import read_csv
from pandas import DataFrame
import warnings


class AESOClient(BaseClient):
    NAME = 'AESO'
    REPORT_URL = 'http://ets.aeso.ca/ets_web/ip/Market/Reports/CSDReportServlet?contentType=csv'

    fuels = {
        'COAL': 'coal',
        'GAS': 'natgas',
        'HYDRO': 'hydro',
        'OTHER': 'other',
        'WIND': 'wind'
    }

    TZ_NAME = 'Canada/Mountain'

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        if latest:
            return self._get_latest_report(request_type='generation')
        else:
            warnings.warn(message='The AESO client only supports latest=True for retrieving generation fuel mix data.',
                          category=UserWarning)
            return None

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_lmp(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def _get_latest_report(self, request_type):
        response = self.request(url=self.REPORT_URL)
        response_io = StringIO(response.content)
        response_df = read_csv(response_io, names=['label', 'mc', 'tng', 'dcr'], skiprows=1)
        if request_type == 'generation':
            return self._parse_generation(latest_df=response_df)
        else:
            raise RuntimeError('Unknown request type: ' + request_type)

    def _parse_generation(self, latest_df):
        """
        :param DataFrame latest_df: The latest electricity market report, parsed as a dataframe.
        :return:
        """
        local_dt = self._parse_local_time_from_latest_report(latest_df=latest_df)
        fuels_df = latest_df.loc[latest_df['label'].isin(self.fuels.keys())]

        generation_df = list([])
        for idx, row in fuels_df.iterrows():
            # ``[ba_name, timestamp, freq, market, fuel_name, gen_MW]``
            generation_df.append({
                'ba_name': self.NAME,
                'timestamp': self.utcify(local_dt),
                'freq': self.FREQUENCY_CHOICES.fivemin,  # Actually it's every two minutes, but whatever :)
                'market': self.MARKET_CHOICES.fivemin,
                'fuel_name': row.label,
                'gen_MW': float(row.tng)
            })

        return generation_df

    @staticmethod
    def _parse_local_time_from_latest_report(latest_df):
        """
        :param DataFrame latest_df: The latest electricity market report, parsed as a dataframe.
        :return: The local datetime that the latest electricity market report was published.
        :rtype datetime
        """
        last_update_prefix = 'Last Update : '
        for lbl in latest_df.label:
            if lbl.startswith(last_update_prefix):
                local_date_str = lbl.lstrip(last_update_prefix)
                break
        local_dt = datetime.strptime(local_date_str, '%b %d, %Y %H:%M')
        return local_dt
