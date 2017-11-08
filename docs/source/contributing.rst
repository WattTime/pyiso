Contributing
============

Right now, pyiso only has interfaces for collecting a small subset of the interesting electricity data that the ISOs provide.
You can help by adding more!
Please create an issue on `github <https://github.com/WattTime/pyiso/issues>`_
if you have questions about any of this.


For developers
--------------

When you're ready to get started coding:

* fork the `repo <https://github.com/WattTime/pyiso>`_
* install in development mode: ``python setup.py develop``
* run the tests: ``python setup.py test`` (or ``python setup.py test -s tests.test_some_file.TestSomeClass.test_some_method`` to run a specific subset of the tests)
* add tests to the :py:mod:`tests` directory and code to the :py:mod:`pyiso` directory, following the conventions that you see in the existing code
* add docs to the `docs/source` directory
* add a note to the Upcoming Changes section in `README.md` on a separate line
* send a pull request
* sign the CLA at https://www.clahub.com/agreements/WattTime/pyiso (see below)


For data users
--------------

Found a bug, or know of a data source that you think pyiso should include?
Please add an issue to `github <https://github.com/WattTime/pyiso/issues>`_.
Ideas of new balancing authorities (anywhere in the world)
and of new data streams from ISOs we already support are both very welcome.


For project admins
------------------

Before making a release, check that these are true in the master branch of the GitHub repo:

* the changelog in `README.md` includes all changes since the last release
* test coverage is good and the tests pass locally and on Travis
* changes are reflected in the docs in `docs/source`
* the version number is upgraded in `pyiso/__init__.py`

To make a release, run these commands (replacing 0.x.y with the correct version number):

.. code-block:: bash

   git checkout master
   git pull origin master
   git tag v0.x.y
   git push origin master --tags
   python setup.py sdist upload

Releasing via twine:

.. code-block:: bash

   python setup.py sdist
   twine upload dist/pyiso-VERSION.tar.gz

Legal things
------------

Because we use pyiso as the base for our other software products, we ask that contributors sign the following Contributor License Agreement.  If you have any questions, or concerns, please drop us a line on Github.


You and WattTime, Corp, a california non-profit corporation, hereby accept and agree to the following terms and conditions:

    Your "Contributions" means all of your past, present and future contributions of object code, source code and documentation to pyiso however submitted to pyiso, excluding any submissions that are conspicuously marked or otherwise designated in writing by You as "Not a Contribution."

    You hereby grant to the WattTime, Corp a non-exclusive, irrevocable, worldwide, no-charge, transferable copyright license to use, execute, prepare derivative works of, and distribute (internally and externally, in object code and, if included in your Contributions, source code form) your Contributions. Except for the rights granted to the WattTime, Corp in this paragraph, You reserve all right, title and interest in and to your Contributions.

    You represent that you are legally entitled to grant the above license. If your employer(s) have rights to intellectual property that you create, you represent that you have received permission to make the Contibutions on behalf of that employer, or that your employer has waived such rights for your Contributions to pyiso.

    You represent that, except as disclosed in your Contribution submission(s), each of your Contributions is your original creation. You represent that your Contribution submissions(s) included complete details of any license or other restriction (including, but not limited to, related patents and trademarks) associated with any part of your Contributions)(including a copy of any applicable license agreement). You agree to notify WattTime, Corp of any facts or circumstances of which you become aware that would make Your representations in the Agreement inaccurate in any respect.

    You are not expected to provide support for your Contributions, except to the extent you desire to provide support. Your may provide support for free, for a fee, or not at all. Your Contributions are provided as-is, with all faults, defects and errors, and without any warranty of any kind (either express or implied) including, without limitation, any implied warranty of merchantability and fitness for a particular purpose and any warranty of non-infringement.

    To get started, <a href="https://www.clahub.com/agreements/WattTime/pyiso">sign the Contributor License Agreement</a>.
