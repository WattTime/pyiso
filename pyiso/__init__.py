import imp
import os.path
from os import environ
import logging

__version__ = '0.3.16'


# ERROR = 40, WARNING = 30, INFO = 20, DEBUG = 10
LOG_LEVEL = int(environ.get('LOG_LEVEL', logging.INFO))


# logger: create here to only add the handler once!
LOGGER = logging.getLogger(__name__)
handler = logging.StreamHandler()
LOGGER.addHandler(handler)
LOGGER.setLevel(LOG_LEVEL)

# BALANCING_AUTHORITIES = {
#     'AEC': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'AECI': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'AESO': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'AVA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'AZPS': {'class': 'SVERIClient', 'module': 'sveri', },
#     'AZPS-EIA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'BANC': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'BCTC': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'BPA': {'class': 'BPAClient', 'module': 'bpa'},
#     'BPAT': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'CAISO': {'class': 'CAISOClient', 'module': 'caiso'},
#     'CAISO-EIA': {'class': 'EIAClient', 'module': 'caiso'},
#     'CFE': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'CHPD': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'CISO': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'CPLE': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'CPLW': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'DEAA': {'class': 'SVERIClient', 'module': 'sveri'},
#     'DEAA-EIA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'DOPD': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'DUK': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'EEI': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'ELE': {'class': 'SVERIClient', 'module': 'sveri'},
#     'EPE': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'ERCO': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'ERCOT': {'class': 'ERCOTClient', 'module': 'ercot'},
#     'EU': {'class': 'EUClient', 'module': 'eu'},
#     'FMPP': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'FPC': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'FPL': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'GCPD': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'GRID': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'GRIF': {'class': 'SVERIClient', 'module': 'sveri'},
#     'GRIF-EIA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'GRMA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'GVL': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'GWA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'HGMA': {'class': 'SVERIClient', 'module': 'sveri'},
#     'HGMA-EIA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'HQT': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'HST': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'IESO': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'IID': {'class': 'SVERIClient', 'module': 'sveri'},
#     'IID-EIA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'IPCO': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'ISNE': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'ISONE': {'class': 'ISONEClient', 'module': 'isone'},
#     'JEA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'LDWP': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'LGEE': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'MHEB': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'MISO': {'class': 'MISOClient', 'module': 'miso'},
#     'MISO-EIA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'NBSO': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'NEVP': {'class': 'NVEnergyClient', 'module': 'nvenergy'},
#     'NEVP-EIA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'NSB': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'NWMT': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'NYIS': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'NYISO': {'class': 'NYISOClient', 'module': 'nyiso'},
#     'OVEC': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'PACE': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'PACW': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'PGE': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'PJM': {'class': 'PJMClient', 'module': 'pjm'},
#     'PJM-EIA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'PNM': {'class': 'SVERIClient', 'module': 'sveri'},
#     'PNM-EIA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'PSCO': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'PSEI': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'SC': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'SCEG': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'SCL': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'SEC': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'SEPA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'SOCO': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'SPA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'SPC': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'SPP': {'class': 'SPPClient', 'module': 'spp'},
#     'SPPC': {'class': 'NVEnergyClient', 'module': 'nvenergy'},
#     'SRP': {'class': 'SVERIClient', 'module': 'sveri'},
#     'SRP-EIA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'SWPP': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'TAL': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'TEC': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'TEPC': {'class': 'SVERIClient', 'module': 'sveri'},
#     'TEPC-EIA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'TIDC': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'TPWR': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'TVA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'WACM': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'WALC': {'class': 'SVERIClient', 'module': 'sveri'},
#     'WALC-EIA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'WAUW': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'WWA': {'class': 'EIAClient', 'module': 'eia_esod'},
#     'YAD': {'class': 'EIAClient', 'module': 'eia_esod'}
# }

# restored version
BALANCING_AUTHORITIES = {
    'AZPS': {'class': 'SVERIClient', 'module': 'sveri'},
    'BPA': {'class': 'BPAClient', 'module': 'bpa'},
    'CAISO': {'class': 'CAISOClient', 'module': 'caiso'},
    'DEAA': {'class': 'SVERIClient', 'module': 'sveri'},
    'EIA': {'class': 'EIAClient', 'module': 'eia_esod'},
    'ELE': {'class': 'SVERIClient', 'module': 'sveri'},
    'ERCOT': {'class': 'ERCOTClient', 'module': 'ercot'},
    'EU': {'class': 'EUClient', 'module': 'eu'},
    'GRIF': {'class': 'SVERIClient', 'module': 'sveri'},
    'HGMA': {'class': 'SVERIClient', 'module': 'sveri'},
    'IID': {'class': 'SVERIClient', 'module': 'sveri'},
    'ISONE': {'class': 'ISONEClient', 'module': 'isone'},
    'MISO': {'class': 'MISOClient', 'module': 'miso'},
    'NEVP': {'class': 'NVEnergyClient', 'module': 'nvenergy'},
    'NYISO': {'class': 'NYISOClient', 'module': 'nyiso'},
    'PJM': {'class': 'PJMClient', 'module': 'pjm'},
    'PNM': {'class': 'SVERIClient', 'module': 'sveri'},
    'SPP': {'class': 'SPPClient', 'module': 'spp'},
    'SPPC': {'class': 'NVEnergyClient', 'module': 'nvenergy'},
    'SRP': {'class': 'SVERIClient', 'module': 'sveri'},
    'TEPC': {'class': 'SVERIClient', 'module': 'sveri'},
    'WALC': {'class': 'SVERIClient', 'module': 'sveri'},
}


def client_factory(client_name, **kwargs):
    """Return a client for an external data set"""
    # set up
    dir_name = os.path.dirname(os.path.abspath(__file__))
    error_msg = 'No client found for name %s' % client_name
    client_key = client_name.upper()

    # find client
    try:
        client_vals = BALANCING_AUTHORITIES[client_key]
        module_name = client_vals['module']

        class_name = client_vals['class']
    except KeyError:
        raise ValueError(error_msg)

    # find module
    try:
        fp, pathname, description = imp.find_module(module_name, [dir_name])
    except ImportError:
        raise ValueError(error_msg)

    # load
    try:
        mod = imp.load_module(module_name, fp, pathname, description)
    finally:
        # Since we may exit via an exception, close fp explicitly.
        if fp:
            fp.close()

    # instantiate class
    try:
        client_inst = getattr(mod, class_name)(**kwargs)
    except AttributeError:
        raise ValueError(error_msg)

    # set name
    client_inst.NAME = client_name

    return client_inst
