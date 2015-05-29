Introduction
=============

Pyiso provides Python client libraries for ISO power grid data sources.
It powers the WattTime Impact API
(`code <https://github.com/WattTime/watttime-grid-api>`_,
`live <http://api.watttime.org/>`_).

What's an ISO?
---------------

Electricity markets are operated by "balancing authorities,"
which manage supply and demand for a given service area.
The bigger balancing authorities, called
Independent Services Operators and Regional Transmission Organizations
(`ISOs/RTOs <http://www.isorto.org/>`_, or simply ISOs),
together cover about 2/3 of US electricity consumers.
The US ISO-like entities are:

* BPA: Bonneville Power Administration (Pacific Northwest) 
* CAISO: California ISO
* ERCOT: Texas
* ISONE: ISO New England
* MISO: Midcontinent ISO
* NYISO: New York ISO
* PJM: Mid-Atlantic
* SPP: Southwest Power Pool (central Midwest/Plains)

ISOs are required to provide real-time data about electricity market operations,
but choose to do so in a wide variety of unstandardized, inconvenient formats.

What's included
----------------

Pyiso makes it easier to collect data from ISOs by providing a uniform Python interface
to each ISO's data streams.
See the :doc:`usage` page for instructions on how to get started.
