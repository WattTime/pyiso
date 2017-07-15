import warnings
from datetime import datetime
from io import BytesIO

from pandas import DataFrame
from pandas import read_csv

from pyiso.base import BaseClient


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
        if latest:
            return self._get_latest_report(request_type='trade')
        else:
            warnings.warn(message='The AESO client only supports latest=True for retrieving net export data.',
                          category=UserWarning)
            return None

    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_lmp(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def _get_latest_report(self, request_type):
        response = self.request(url=self.REPORT_URL)
        response_body = BytesIO(response.content)
        response_df = read_csv(response_body, names=['label', 'col1', 'col2', 'col3'], skiprows=1)
        if request_type == 'generation':
            return self._parse_generation(latest_df=response_df)
        elif request_type == 'trade':
            return self._parse_trade(latest_df=response_df)
        else:
            raise RuntimeError('Unknown request type: ' + request_type)

    def _parse_generation(self, latest_df):
        """
        Parse fuel mix of electricity generation data from the latest AESO electricity market report.
        :param DataFrame latest_df: The latest electricity market report, parsed as a dataframe.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, fuel_name, gen_MW]``.
           Timestamps are in UTC.
        :rtype: list
        """
        local_dt = self._parse_local_time_from_latest_report(latest_df=latest_df)
        fuels_df = latest_df.loc[latest_df['label'].isin(self.fuels.keys())]
        fuels_df.rename(columns={'col1': 'mc', 'col2': 'tng', 'col3': 'dcr'}, inplace=True)

        generation_df = list([])
        for idx, row in fuels_df.iterrows():
            generation_df.append({
                'ba_name': self.NAME,
                'timestamp': self.utcify(local_dt),
                'freq': self.FREQUENCY_CHOICES.fivemin,  # Actually it's every two minutes, but whatever :)
                'market': self.MARKET_CHOICES.fivemin,
                'fuel_name': row.label,
                'gen_MW': float(row.tng)
            })

        return generation_df

    def _parse_trade(self, latest_df):
        """
        Parse net export data from the latest AESO electricity market report.
        :param DataFrame latest_df: The latest electricity market report, parsed as a dataframe.
        :return: List of dicts, each with keys ``[ba_name, timestamp, freq, market, net_exp_MW]``.
           Timestamps are in UTC.
        :rtype: list
        """
        local_dt = self._parse_local_time_from_latest_report(latest_df=latest_df)
        net_export_df = latest_df.loc[latest_df['label'].isin(['British Columbia', 'Montana', 'Saskatchewan'])]
        net_export_df.rename(columns={'col1': 'actual_flow'}, inplace=True)

        total_net_export = 0
        for idx, row in net_export_df.iterrows():
            total_net_export += float(row.actual_flow)

        load_df = [{
            'ba_name': self.NAME,
            'timestamp': self.utcify(local_dt),
            'freq': self.FREQUENCY_CHOICES.fivemin,  # Actually it's every two minutes, but whatever :)
            'market': self.MARKET_CHOICES.fivemin,
            'net_exp_MW': total_net_export
        }]
        return load_df

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
