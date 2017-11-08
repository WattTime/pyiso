Usage
=====

There are two main ways to use pyiso: via the client objects, or via celery tasks.
The client approach is preferred for scripted data analysis.
The task approach enables asynchronous or periodic data collection
and is in use at the `WattTime Impact API <http://api.watttime.org/>`_.

Clients
-------

.. py:currentmodule:: pyiso.base

First, create a client using the ``client_factory(ba_name)`` function.
``ba_name`` should be taken from this list of abbreviated names for available balancing authorities
listed on the :doc:`intro` page.
For example::

   >>> from pyiso import client_factory
   >>> isone = client_factory('ISONE')


Requests made to external data sources will automatically time out after 20 seconds.
To change this value, add a keyword argument in the constructor::

   >>> isone = client_factory('ISONE', timeout_seconds=60)


Each client returned by ``client_factory`` is derived from :py:class:`BaseClient` and provides one or more of the following methods (see also :doc:`options`):

.. automethod:: BaseClient.get_generation
   :noindex:

.. automethod:: BaseClient.get_load
   :noindex:

.. automethod:: BaseClient.get_trade
   :noindex:

The lists returned by clients are conveniently structured for import into other data structures like :py:class:`pandas.DataFrame`::

   >>> import pandas as pd
   >>> data = isone.get_generation(latest=True)
   >>> df = pd.DataFrame(data)
   >>> print df
     ba_name freq fuel_name  gen_MW market                  timestamp
   0   ISONE  n/a      coal  1170.0   RT5M  2014-03-29 20:40:27+00:00
   1   ISONE  n/a     hydro   813.8   RT5M  2014-03-29 20:40:27+00:00
   2   ISONE  n/a    natgas  4815.7   RT5M  2014-03-29 20:40:27+00:00
   3   ISONE  n/a   nuclear  4618.8   RT5M  2014-03-29 20:40:27+00:00
   4   ISONE  n/a    biogas    29.5   RT5M  2014-03-29 20:40:27+00:00
   5   ISONE  n/a    refuse   428.6   RT5M  2014-03-29 20:40:27+00:00
   6   ISONE  n/a      wind    85.8   RT5M  2014-03-29 20:40:27+00:00
   7   ISONE  n/a   biomass   434.3   RT5M  2014-03-29 20:40:27+00:00

Happy data analysis!


Tasks
-----

If you have a `celery <http://www.celeryproject.org/>`_ environment set up, you can use the tasks provided in the :py:mod:`pyiso.tasks` module.
There is one task for each of the client's ``get_*`` methods that implements a thin wrapper around that method.
The call signatures match those of the corresponding client methods, except that the ``ba_name`` is a required first argument.
For example, to get the latest ISONE generation mix data every 10 minutes,
add this to your `celerybeat schedule <http://docs.celeryproject.org/en/latest/userguide/periodic-tasks.html#crontab-schedules>`_::

   CELERYBEAT_SCHEDULE = {
       'get-isone-genmix-latest' : {
           'task': 'pyiso.tasks.get_generation',
           'schedule': crontab(minute='*/10'),
           'args': ['ISONE'],
           'kwargs': {'latest': True},
       }
   }

In practice, you will want to chain these tasks with something that captures and processes their output.
