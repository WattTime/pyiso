from pyiso.base import BaseClient


class IESOClient(BaseClient):
    NAME = 'IESO'
    TZ_NAME = 'America/Toronto'

    base_url = 'http://reports.ieso.ca/public/'

    # In Ontario,
    fuels = {
        'NUCLEAR': 'nuclear',
        'GAS': 'natgas',
        'HYDRO': 'hydro',
        'WIND': 'wind',
        'SOLAR': 'solarpv',
        'BIOFUEL': 'biomass'
    }

    def get_generation(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass


    def get_load(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass

    def get_trade(self, latest=False, yesterday=False, start_at=False, end_at=False, **kwargs):
        pass
