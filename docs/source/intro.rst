Introduction
============

Pyiso provides Python client libraries for ISO and other power grid data sources.
It powers the `WattTime Impact API <https://api.watttime.org/>`_,
among other things.

What's an ISO?
--------------

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
---------------

Pyiso makes it easier to collect data from ISOs and other balancing authorities
by providing a uniform Python interface to each data stream.
See the :doc:`usage` page for instructions on how to get started.

Specifically, here are the included balancing authorities and their respective data sources:

Note: Some balancing authorities offer data directly and through the EIA client.

=========================== ======================================== ============
balancing authority abbrev. balancing authority name/region          data source
=========================== ======================================== ============
AESO                        Alberta Elec. System Operator (Canada)   AESO
AZPS                        Arizona Public Service                   SVERI
BCH                         British Columbia Hydro (Canada)          BCH
BPA                         Bonneville Power Administration (Pac NW) BPA
CAISO                       California ISO                           CAISO
DEAA                        DECA Arlington Valley (AZ)               SVERI
ELE                         El Paso Electric                         SVERI
ERCOT                       Texas                                    ERCOT
EU                          European Union                           ENTSO
GRIF                        Griffith Energy (AZ)                     SVERI
HGMA                        Harquahala Generation Maricopa Arizona   SVERI
IESO                        Ontario (Canada)                         IESO
IID                         Imperial Irrigation District (CA)        SVERI
ISONE                       ISO New England                          ISONE
MISO                        Midcontinent ISO                         MISO
NBP                         New Brunswick Power (Canada)             NBPower
NLH                         Newfoundland and Labrador Hydro (Canada) NLHydro
NSP                         Nova Scotia Power (Canada)               NSPower
NEVP                        Nevada Power                             NVEnergy
NYISO                       New York ISO                             NYISO
PEI                         Price Edward Island (Canada)             PEI
PJM                         Mid-Atlantic                             PJM
PNM                         Public Service Co New Mexico             SVERI
SASK                        Saskatchewan Power (Canada)              SaskPower
SPPC                        Sierra Pacific Power (NV)                NVEnergy
SRP                         Salt River Project (AZ)                  SVERI
TEPC                        Tuscon Electric Power Co                 SVERI
WALC                        WAPA Desert Southwest (NV, AZ)           SVERI
YUKON                       Yukon Energy (Canada)                    YUKON
=========================== ======================================== ============

The following BAs are available through the EIA client.

============================= ======================================== ============
balancing authority abbrev.    balancing authority name/region          data source
============================= ======================================== ============
      AEC                       PowerSouth Energy Cooperative            EIA
      AECI                      Associated Electric Cooperative, Inc.    EIA
      AESO                      Alberta Electric System Operator         EIA
      AVA                       Avista Corporation                       EIA
      AZPS                      Arizona Public Service- EIA data         EIA
      BANC                      Bal Authority of Northern California     EIA
      BCTC                      British Columbia Transmission Corp       EIA
      BPAT                      Bonneville Power Admin- EIA data         EIA
      CAISO                     California ISO- EIA data                 EIA
      CFE                       Comision Federal de Electricidad         EIA
      CHPD                      Pub Utility Dist 1 of Chelan County      EIA
      CISO                      California Independent System Operator   EIA
      CPLE                      Duke Energy Progress East                EIA
      CPLW                      Duke Energy Progress West                EIA
      DEAA                      DECA Arlington Valley (AZ)- EIA data     EIA
      DOPD                      PUD No. 1 of Douglas County              EIA
      DUK                       Duke Energy Carolinas                    EIA
      EEI                       Electric Energy, Inc                     EIA
      EPE                       El Paso Electric - EIA data              EIA
      ERCO                      Texas- EIA data                          EIA
      FMPP                      Florida Municipal Power Pool             EIA
      FPC                       Duke Energy Florida                      EIA
      FPL                       Florida Power and Light Co.              EIA
      GCPD                      PUD of Grant County, Washington          EIA
      GRID                      Gridforce Energy Management              EIA
      GRIF                      Griffith Energy (AZ) - EIA data          EIA
      GRMA                      Gila River Power                         EIA
      GVL                       Gainesville Regional Utilities           EIA
      GWA                       NaturEner Power Watch                    EIA
      HGMA                      Harquahala Gen Maricopa Az - EIA         EIA
      HQT                       Hydro-Quebec TransEnergie                EIA
      HST                       City of Homestead                        EIA
      IESO                      Ontario IESO                             EIA
      IID                       Imperial Irrigation District- EIA        EIA
      IPCO                      Idaho Power Company                      EIA
      ISNE                      ISO New England - EIA data               EIA
      JEA                       JEA Jacksonville, Fl                     EIA
      LDWP                      Los Angeles Dept of Water and Power      EIA
      LGEE                      Louisville Gas & Electric/KY Utilities   EIA
      MHEB                      Manitoba Hydro                           EIA
      MISO                      Midcontinent ISO - EIA data              EIA
      NBSO                      New Brunswick System Operator            EIA
      NEVP                      Nevada Power - EIA data                  EIA
      NSB                       New Smyrna Beach UC                      EIA
      NWMT                      NorthWestern Corporation                 EIA
      NYIS                      New York ISO - EIA data                  EIA
      OVEC                      Ohio Valley Electric Corporation         EIA
      PACE                      PacifiCorp East                          EIA
      PACW                      PacifiCorp West                          EIA
      PGE                       Portland General Electric Co             EIA
      PJM                       Mid-Atlantic - EIA data                  EIA
      PNM                       Public Service Co New Mexico- EIA        EIA
      PSCO                      Public Service Company of Colorado       EIA
      PSEI                      Puget Sound Energy                       EIA
      SC                        South Carolina Public Service Auth       EIA
      SCEG                      South Carolina Electric and Gas          EIA
      SCL                       Seattle City Light                       EIA
      SEC                       Seminole Electric Cooperative            EIA
      SEPA                      Southeastern Power Admin                 EIA
      SOCO                      Southern Company Services                EIA
      SPA                       Southwestern Power Admin                 EIA
      SPC                       Saskatchewan Power Corporation           EIA
      SRP                       Salt River Project (AZ) - EIA data       EIA
      SWPP                      Southwest Power Pool                     EIA
      TAL                       City of Tallahassee                      EIA
      TEC                       Tampa Electric Company                   EIA
      TEPC                      Tuscon Electric Power Co                 EIA
      TIDC                      Turdock Irrigation District              EIA
      TPWR                      City of Tacoma DPU                       EIA
      TVA                       Tennessee Valley Authority               EIA
      WACM                      Western Area Power Admin- Rocky Mtn      EIA
      WALC                      WAPA Desert Southwest (NV, AZ)-EIA       EIA
      WAUW                      Western Area Power Admin- Great Plains   EIA
      WWA                       NaturEner Wind Watch                     EIA
      YAD                       Alcoa Power Generation- Yadkin           EIA
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
