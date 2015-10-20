Installation
============

Install
-------

Pyiso is available on `PyPI <https://pypi.python.org/pypi?name=pyiso&:action=display>`_
and on `GitHub <https://github.com/WattTime/pyiso>`_.

For users, the easiest way to get pyiso is with pip::

   pip install pyiso

For developers, you can get the source from `GitHub <https://github.com/WattTime/pyiso.git>`_
or `PyPI <https://pypi.python.org/packages/source/p/pyiso/pyiso-0.1.tar.gz>`_, then::

   cd pyiso
   python setup.py install

Pyiso depends on pandas so be prepared for a large install.

Windows Users: If you are unable to setup pyiso due to issues with installing or using numpy, a dependent package of pyiso, try installing a precompiled version of numpy found here: http://www.lfd.uci.edu/~gohlke/pythonlibs/


Accounts
--------

ISONE and the EU each require a username and password to collect data.
You can register for an ISONE account here (http://www.iso-ne.com/participate/applications-status-changes/access-software-systems#data-feeds) and an EU ENTSOe account here (https://transparency.entsoe.eu/).

Then, set your username and password as environment variables::

	export ISONE_USERNAME=myusername1
	export ISONE_PASSWORD=mysecret1
    export ENTSOe_USERNAME=myusername2
    export ENTSOe_PASSWORD=mypassword2

All other ISOs allow unauthenticated users to collect data, so no other credentials are needed.


Uninstall
---------

To uninstall::

   pip uninstall pyiso
