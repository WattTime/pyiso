
from pyiso import client_factory, BALANCING_AUTHORITIES
from pyiso.base import BaseClient
import pandas as pd

def test_nyiso():
    c = client_factory(client_name='NYISO')

    # get data
    # start=, end=, | latest
    data = c.get_load(start_at='2016-05-01', end_at='2016-05-03',integrated_1h=True, freq='hourly')
    df=pd.DataFrame(data)


def test_get_nyiso_fst_load():

    c = client_factory('NYISO')
    data = c.get_load(latest=True, forecast=True, freq='hourly')
    df = pd.DataFrame(data)

    assert df.shape[1] > 10
  