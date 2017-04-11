pyiso
============

[![Build Status](https://travis-ci.org/WattTime/pyiso.svg?branch=master)](https://travis-ci.org/WattTime/pyiso)
[![Coverage Status](https://coveralls.io/repos/WattTime/pyiso/badge.png?branch=master)](https://coveralls.io/r/WattTime/pyiso?branch=master)
[![PyPI version](https://badge.fury.io/py/pyiso.svg)](https://badge.fury.io/py/pyiso)

pyiso provides Python client libraries for [ISO](https://www.epsa.org/industry/primer/?fa=rto) and other power grid data sources.
It powers the WattTime API (https://api.watttime.org/), among other things.

Documentation: https://pyiso.readthedocs.io/

User group: https://groups.google.com/forum/#!forum/pyiso-users


Changelog
---------
* 0.3.18: Fix bug with PJM date parsing
* 0.3.17: Fix bug with `Black Liquor` fuel type for PJM
* 0.3.16: Implement ISONE get_morningreport and get_sevendayforecast
* 0.3.15: Minor bugfixes to CAISO get_generation. 
* 0.3.14: Minor bugfixes to ISONE, PJM, and ERCOT. 
* 0.3.13: Major feature: generation mix in PJM (RTHR market only). Minor change: SSL handling in BPA.
* 0.3.12: Bugfix: fixed EU authentication, thanks @frgtn!
* 0.3.11: Changes: `timeout_seconds` kwarg to client constructor; do not remember options from one `get_*` call to the next.
* 0.3.10: Changes: Historical DAHR LMP data in NYISO is not available using `market='DAHR'`; error is raised when trying to access historical RT5M LMP data in PJM.
* 0.3.9: Fixes breaking error with BeautifulSoup. Minor fixes: closes issues #79, #84.
* 0.3.8: Minor feature: Historical NYISO LMP data available farther into the past.
* 0.3.7: Change: For CAISO historical generation, defaults to DAHR market instead of RTHR if no market is provided.
* 0.3.6: Change: If `forecast=True` is requested without specifying `start_at` or `end_at`, `start_at` will default to the current time; previously it defaulted to midnight in the ISO's local time. Bugfixes: times outside the `start_at`-`end_at` range are no longer returned for ISONE generation and load, CAISO DAHR generation.
* 0.3.5: Minor feature: all tasks can accept strings for `start_at` and `end_at` kwargs.
* 0.3.2: Minor feature: `get_lmp` task. Minor bugfixes: safer handling of response errors for load (BPA, ERCOT, MISO, NVEnergy, PJM) and generation (BPA, CAISO, ERCOT, ISONE, NYISO); clean up LMP tests.
* 0.3.1: Minor changes for PJM real-time load data: fall back to OASIS if Data Snapshot is down, round time down to nearest 5 min period. Major feature: SVERI back up.
* 0.3.0: Major features: Add LMP to all ISOs, license change. Please contact us for alternative licenses. Bugfixes: SVERI has a new URL. Minor features: CAISO has 15-minute RTPD market.
* 0.2.23: Major fix: ERCOT real-time data format changed, this release is updated to match the new format. Minor fixes to excel date handling with pandas 0.18, and MISO forecast.
* 0.2.22: Feature: LMP in NYISO, thanks @ecalifornica! Bug fixes for DST transition.
* 0.2.21: Major feature: generation mix in NYISO. Bug fix: time zone handling in NYISO.
* 0.2.18: Minor change: enforce pandas version 0.17 or higher.
* 0.2.17: Minor change: Limit retries in `base.request`, and increase time between retries.
* 0.2.16: Major fix: PJM deprecated the data source that was used in previous releases. This release uses a new data source that has load and tie flows, but not wind. So PJM generation mix has been deprecated for the moment--hopefully it will return in a future release.
* 0.2.15: Minor changes: enforce pandas 0.16.2 and change NYISO index labelling to fix NYISO regression in some environments.
* 0.2.14: Major features: forecast load in ERCOT, MISO, NYISO, PJM; forecast genmix in MISO; forecast trade in MISO. Minor changes: fixed DST bug in BPA, refactored several to better use pandas.
* 0.2.13: Minor bugfix: Better able to find recent data in NVEnergy.
* 0.2.12: Major features: EU support, support for throttling in CAISO. Minor upgrades: Improve docs, dedup logging messages.
* 0.2.11: Minor bugfixes. Also, made a backward-incompatible change to the data structure that's returned from `get_ancillary_services` in CAISO.
* 0.2.10: Fixed bug in CAISO LMP DAM.
* 0.2.9: Added load and generation mix for SVERI (AZPS, DEAA, ELE, HGMA, IID, GRIF, PNM, SRP, TEPC, WALC)
* 0.2.8: Added lmp in ISONE. Also, made a backward-incompatible change to the data structure that's returned from `get_lmp` in CAISO.
* 0.2.7: Added load and trade in Nevada Energy (NEVP and SPPC)
* 0.2.1: Added load (real-time 5-minute and hourly forecast) in ISONE
* 0.2.0: Maintained Python 2.7 support and added Python 3.4! Thanks @emunsing
