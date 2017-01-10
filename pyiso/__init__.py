import imp
import os.path
from os import environ
import logging

__version__ = '0.3.15'


# ERROR = 40, WARNING = 30, INFO = 20, DEBUG = 10
LOG_LEVEL = int(environ.get('LOG_LEVEL', logging.INFO))


# logger: create here to only add the handler once!
LOGGER = logging.getLogger(__name__)
handler = logging.StreamHandler()
LOGGER.addHandler(handler)
LOGGER.setLevel(LOG_LEVEL)


# BALANCING_AUTHORITIES = {
#     'AZPS': {'module': 'sveri', 'class': 'SVERIClient'},
#     'BPA': {'module': 'bpa', 'class': 'BPAClient'},
#     'CAISO': {'module': 'caiso', 'class': 'CAISOClient'},
#     'DEAA': {'module': 'sveri', 'class': 'SVERIClient'},
#     'ELE': {'module': 'sveri', 'class': 'SVERIClient'},
#     'ERCOT': {'module': 'ercot', 'class': 'ERCOTClient'},
#     'HGMA': {'module': 'sveri', 'class': 'SVERIClient'},
#     'IID': {'module': 'sveri', 'class': 'SVERIClient'},
#     'ISONE': {'module': 'isone', 'class': 'ISONEClient'},
#     'GRIF': {'module': 'sveri', 'class': 'SVERIClient'},
#     'MISO': {'module': 'miso', 'class': 'MISOClient'},
#     'NEVP': {'module': 'nvenergy', 'class': 'NVEnergyClient'},
#     'NYISO': {'module': 'nyiso', 'class': 'NYISOClient'},
#     'PJM': {'module': 'pjm', 'class': 'PJMClient'},
#     'PNM': {'module': 'sveri', 'class': 'SVERIClient'},
#     'SPPC': {'module': 'nvenergy', 'class': 'NVEnergyClient'},
#     'SPP': {'module': 'spp', 'class': 'SPPClient'},
#     'SRP': {'module': 'sveri', 'class': 'SVERIClient'},
#     'TEPC': {'module': 'sveri', 'class': 'SVERIClient'},
#     'WALC': {'module': 'sveri', 'class': 'SVERIClient'},
#     'EU': {'module': 'eu', 'class': 'EUClient'},
# }

BALANCING_AUTHORITIES = {
    'AEC': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'AECI': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'AESO': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'AVA': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'AZPS': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'BANC': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'BCTC': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'BPA': {'class': 'BPAClient', 'module': 'bpa'},
    'BPAT': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'CAISO': {'class': 'CAISOClient', 'module': 'caiso'},
    'CFE': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'CHPD': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'CISO': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'CPLE': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'CPLW': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'DEAA': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'DOPD': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'DUK': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'EEI': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'ELE': {'class': 'SVERIClient', 'module': 'sveri'},
    'EPE': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'ERCO': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'ERCOT': {'class': 'ERCOTClient', 'module': 'ercot'},
    'EU': {'class': 'EUClient', 'module': 'eu'},
    'FMPP': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'FPC': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'FPL': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'GCPD': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'GRID': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'GRIF': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'GRMA': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'GVL': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'GWA': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'HGMA': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'HQT': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'HST': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'IESO': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'IID': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'IPCO': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'ISNE': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'ISONE': {'class': 'ISONEClient', 'module': 'isone'},
    'JEA': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'LDWP': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'LGEE': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'MHEB': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'MISO': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'NBSO': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'NEVP': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'NSB': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'NWMT': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'NYIS': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'NYISO': {'class': 'NYISOClient', 'module': 'nyiso'},
    'OVEC': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'PACE': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'PACW': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'PGE': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'PJM': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'PNM': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'PSCO': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'PSEI': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'SC': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'SCEG': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'SCL': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'SEC': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'SEPA': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'SOCO': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'SPA': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'SPC': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'SPP': {'class': 'SPPClient', 'module': 'spp'},
    'SPPC': {'class': 'NVEnergyClient', 'module': 'nvenergy'},
    'SRP': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'SWPP': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'TAL': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'TEC': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'TEPC': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'TIDC': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'TPWR': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'TVA': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'WACM': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'WALC': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'WAUW': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'WWA': {'class': 'EIACLIENT', 'module': 'eia_esod'},
    'YAD': {'class': 'EIACLIENT', 'module': 'eia_esod'}
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
