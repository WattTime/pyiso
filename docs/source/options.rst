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
``ERCOT.get_generation``  yes         yes                                no            no
``ERCOT.get_load``        yes         no                                 no            no
``ISONE.get_generation``  yes         yes                                no            no
``ISONE.get_lmp`` 	      yes         yes                                no            no
``ISONE.get_load`` 	      yes         yes                                no            yes
``MISO.get_generation``   yes         no                                 no            no
``NVEnergy.get_load``     yes         yes                                no            yes
``NYISO.get_generation``  yes         yes                                no            no
``NYISO.get_load``        yes         yes                                no            no
``NYISO.get_trade``       yes         yes                                no            no
``PJM.get_generation``    yes         no                                 no            no
``PJM.get_load``          yes         no                                 no            no
``SVERI.get_generation``  yes         yes                                no            no
``SVERI.get_load``        yes         yes                                no            no
======================== ========== =================================== ============== ============
