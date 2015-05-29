Contributing
=============

Right now, pyiso only has interfaces for collecting a small subset of the interesting electricity data that the ISOs provide.
You can help by adding more!
Please get in touch with Anna <anna@watttime.org> if you have questions about any of this.

For developers
---------------

Check out the `project wiki <https://github.com/WattTime/pyiso/wiki>`_ for developer-ready lists of links to other data sources,
and choose one that you'd like to add.

When you're ready to get started:

* fork the `repo <https://github.com/WattTime/pyiso>`_
* install in development mode: ``python setup.py develop``
* run the tests: ``python setup.py test``
* add tests to the :py:mod:`tests` directory and code to the :py:mod:`pyiso` directory, following the conventions that you see in the existing code
* send a pull request

For data users
---------------

Know of a data source that you think pyiso should include?
Please add it to the `project wiki <https://github.com/WattTime/pyiso/wiki>`_.
Ideas of new balancing authorities (anywhere in the world)
and of new data streams from ISOs we already support
are both very welcome.
