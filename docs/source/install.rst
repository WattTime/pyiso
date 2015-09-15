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

ISONE requires a username and password to use their API to collect data.
To use the ISONE client, you must register an account with them
by following the instructions here:
http://www.iso-ne.com/participate/applications-status-changes/access-software-systems#data-feeds

Then, set your username and password as environment variables::

	export ISONE_USERNAME=myusername
	export ISONE_PASSWORD=mysecret

The EU requires a registered user to download machine readable files.  Register for an account
at https://transparency.entsoe.eu/

Then set your username and password as environment variables:

        export ENTSOe_USERNAME=myusername
        export ENTSOe_PASSWORD=mypassword

All other ISOs allow unauthenticated users to collect data, so no other credentials are needed.


Uninstall
---------

To uninstall::

   pip uninstall pyiso
