Options
=======

Not all date range options are available for all methods in all regions.
Here's what's available now:

======================== ========== =================================== ============== ============
method                   ``latest``   ``start_at`` and ``end_at`` pair   ``yesterday`` forecast ok
======================== ========== =================================== ============== ============
``BPA.get_generation``    yes         yes                                no            no
``BPA.get_load``          yes         yes                                no            no
``CAISO.get_generation``  yes         yes                                yes           yes
``CAISO.get_load``        yes         yes                                yes           yes
``CAISO.get_trade``       yes         yes                                yes           yes
``CAISO.get_lmp``         yes         yes                                yes           yes
``EIA.get_generation``    yes         yes                                yes           no
``EIA.get_load``          yes         yes                                yes           yes
``EIA.get_trade``         yes         yes                                yes           no
``ERCOT.get_generation``  yes         no                                 no            no
``ERCOT.get_lmp``         yes         yes                                no            yes
``ERCOT.get_load``        yes         yes                                no            yes
``EU.get_load``           yes         yes                                no            yes
``ISONE.get_generation``  yes         yes                                no            no
``ISONE.get_lmp`` 	      yes         yes                                yes           yes
``ISONE.get_load`` 	      yes         yes                                no            yes
``MISO.get_generation``   yes         yes                                no            yes
``MISO.get_load``         yes         yes                                no            yes
``MISO.get_trade``        no          yes                                no            yes
``MISO.get_lmp``          yes         yes                                no            yes
``NVEnergy.get_load``     yes         yes                                no            yes
``NYISO.get_generation``  yes         yes                                no            no
``NYISO.get_load``        yes         yes                                no            yes
``NYISO.get_lmp``         yes         yes                                no            yes
``NYISO.get_trade``       yes         yes                                no            no
``PJM.get_generation``    yes         no                                 no            no
``PJM.get_load``          yes         yes                                no            yes
``PJM.get_trade``         yes         no                                 no            no
``PJM.get_lmp``           yes         yes                                no            no
``SASK.get_generation``   no          no                                 no            no
``SASK.get_load``         yes         no                                 no            no
``SASK.get_trade``        no          no                                 no            no
``SASK.get_lmp``          no          no                                 no            no
``SVERI.get_generation``  yes         yes                                no            no
``SVERI.get_load``        yes         yes                                no            no
======================== ========== =================================== ============== ============
