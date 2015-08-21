Introduction
=============

Pyiso provides Python client libraries for ISO and other power grid data sources.
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

ISOs are required to provide real-time data about electricity market operations,
but choose to do so in a wide variety of unstandardized, inconvenient formats.
Some smaller balancing authorities provide data too.

What's included
----------------

Pyiso makes it easier to collect data from ISOs and other balancing authorities
by providing a uniform Python interface to each data stream.
See the :doc:`usage` page for instructions on how to get started.

Specifically, here are the included balancing authorities and their respective data sources:

============================= ======================================== ============
balancing authority abbbrev.  balancing authority name/region          data source
============================= ======================================== ============
    AZPS                      Arizona Public Service                   SVERI
    BPA                       Bonneville Power Administration (Pac NW) BPA
    CAISO                     California ISO                           CAISO
    DEAA                      DECA Arlington Valley (AZ)               SVERI
    ELE                       El Paso Electric                         SVERI
    ERCOT                     Texas                                    ERCOT
    HGMA                      Harquahala Generation Maricopa Arizona   SVERI
    IID                       Imperial Irrigation District (CA)        SVERI
    ISONE                     ISO New England                          ISONE
    GRIF                      Griffith Energy (AZ)	                   SVERI
    MISO                      Midcontinent ISO                         MISO
    NEVP                      Nevada Power                             NVEnergy
    NYISO                     New York ISO                             NYISO
    PJM                       Mid-Atlantic                             PJM
    PNM                       Public Service Co New Mexico             SVERI
    SPPC                      Sierra Pacific Power (NV)                NVEnergy
    SRP                       Salt River Project (AZ)                  SVERI
    TEPC                      Tuscon Electric Power Co                 SVERI
    WALC                      WAPA Desert Southwest (NV, AZ)           SVERI
============================= ======================================== ============
