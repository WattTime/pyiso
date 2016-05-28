Introduction
=============

Pyiso provides Python client libraries for ISO and other power grid data sources.
It powers the `WattTime Impact API <https://github.com/WattTime/watttime-grid-api>`_,
among other things.

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
balancing authority abbrev.    balancing authority name/region          data source
============================= ======================================== ============
    BPA                       Bonneville Power Administration (Pac NW) BPA
    CAISO                     California ISO                           CAISO
    ERCOT                     Texas                                    ERCOT
    EU                        European Union                           ENTSO
    ISONE                     ISO New England                          ISONE
    MISO                      Midcontinent ISO                         MISO
    NEVP                      Nevada Power                             NVEnergy
    NYISO                     New York ISO                             NYISO
    PJM                       Mid-Atlantic                             PJM
    SPPC                      Sierra Pacific Power (NV)                NVEnergy
============================= ======================================== ============


For European data, you also need to specify a "control area". The available control areas are:

===================== ========================================
control area abbrev.   control area country/provider    
===================== ========================================
AL                      Albania
AT                      Austria
BA                      Bosnia and Herzegovina
BE                      Belgium
BG                      Bulgaria
CH                      Switzerland
CY                      Cyprus
CZ                      Czech Republic
DE(50HzT)               Germany (50 HzT)
DE(Amprion)             Germany (Amprion)
DE(TenneT GER)          Germany (TenneT)
DE(TransnetBW)          Germany (Transnet)
DK                      Denmark
EE                      Estonia
ES                      Spain
FI                      Finland
FR                      France
GR                      Greece
HR                      Croatia
HU                      Hungary
IE                      Ireland
IT                      Italy
LT                      Lithuania
LU                      Luxembourg
LV                      Latvia
MD                      Moldavia
ME                      Montenegro
MK                      Macedonia
MT                      Malta
NIE                     UK (NIE)
NL                      Netherlands
NO                      Norway
National Grid           UK (National Grid)
PL                      Poland
PL-CZ                   Czech Republic/Poland
PT                      Portugal
RO                      Romania
RS                      Serbia
RU                      Russia
RU-KGD                  Russia (KGD)
SE                      Sweden
SI                      Slovenia
SK                      Slovakia
TR                      Turkey
UA                      Ukraine
UA-WEPS                 Ukraine (WEPS)
===================== ========================================
