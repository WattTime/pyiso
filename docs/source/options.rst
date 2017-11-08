Options
=======

Not all date range options are available for all methods in all regions.
Here's what's available now:

======================== ========== =================================== ============== ============
method                   ``latest``   ``start_at`` and ``end_at`` pair   ``yesterday`` forecast ok
======================== ========== =================================== ============== ============
``AESO.get_generation``   yes         no                                 no            no
``AESO.get_load``         yes         yes                                yes           yes
``AESO.get_trade``        yes         no                                 no            no
``AESO.get_lmp``          no          no                                 no            no
``BCH.get_generation``    no          no                                 no            no
``BCH.get_load``          no          no                                 no            no
``BCH.get_trade``         yes         yes                                yes           no
``BPA.get_generation``    yes         yes                                no            no
``BPA.get_load``          yes         yes                                no            no
``CAISO.get_generation``  yes         yes                                yes           yes
``CAISO.get_load``        yes         yes                                yes           yes
``CAISO.get_trade``       yes         yes                                yes           yes
``EIA.get_generation``    yes         yes                                yes           no
``EIA.get_load``          yes         yes                                yes           yes
``EIA.get_trade``         yes         yes                                yes           no
``ERCOT.get_generation``  yes         no                                 no            no
``ERCOT.get_load``        yes         yes                                no            yes
``EU.get_generation``     yes         yes                                yes           no
``EU.get_load``           yes         yes                                no            yes
``IESO.get_generation``   yes         yes                                yes           yes
``IESO.get_load``         yes         yes                                yes           yes
``IESO.get_trade``        yes         yes                                yes           yes
``ISONE.get_generation``  yes         yes                                no            no
``ISONE.get_load`` 	      yes         yes                                no            yes
``MISO.get_generation``   yes         yes                                no            yes
``MISO.get_load``         yes         yes                                no            yes
``MISO.get_trade``        no          yes                                no            yes
``MISO.get_lmp``          yes         yes                                no            yes
``NLH.get_generation``    no          no                                 no            no
``NLH.get_load``          yes         no                                 no            no
``NLH.get_trade``         no          no                                 no            no
``NPB.get_generation``    no          no                                 no            no
``NPB.get_load``          yes         yes                                no            yes
``NPB.get_trade``         yes         no                                 no            no
``NSP.get_generation``    yes         yes                                no            no
``NSP.get_load``          yes         yes                                no            yes
``NSP.get_trade``         no          no                                 no            no
``NVEnergy.get_load``     yes         yes                                no            yes
``NYISO.get_generation``  yes         yes                                no            no
``NYISO.get_load``        yes         yes                                no            yes
``NYISO.get_trade``       yes         yes                                no            no
``PEI.get_generation``    yes         no                                 no            no
``PEI.get_load``          yes         no                                 no            no
``PEI.get_trade``         no          no                                 no            no
``PJM.get_generation``    yes         no                                 no            no
``PJM.get_load``          yes         yes                                no            yes
``PJM.get_trade``         yes         no                                 no            no
``SASK.get_generation``   no          no                                 no            no
``SASK.get_load``         yes         no                                 no            no
``SASK.get_trade``        no          no                                 no            no
``SVERI.get_generation``  yes         yes                                no            no
``SVERI.get_load``        yes         yes                                no            no
``YUKON.get_generation``  yes         yes                                no            no
``YUKON.get_load``        yes         yes                                no            no
``YUKON.get_trade``       n/a         n/a                                n/a           n/a
======================== ========== =================================== ============== ============
