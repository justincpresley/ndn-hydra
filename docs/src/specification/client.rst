Client
======

The Client is very forgiving. Running ``python3 ./client/main.py`` will tell you what options
are available. Furthermore, running ``python3 ./client/main.py <function>`` with a appropriate
function out of the list {insert,delete,fetch,query} will tell you exactly what you need.

Insertion
---------

Inserts a local file given by the path within a hydra repo associated with the following Name.

Assuming this is ran being in the root directory, the following is a template on ways to run
the client with insert.

.. code-block:: bash

    python3 ./client/main.py insert -r <repo-prefix> -f <file-name> -p <path>

For example:

.. code-block:: bash

    python3 ./client/main.py insert -r /hydra -f /home/a.txt -p ./client/files/10kb.txt

Deletion
--------

Deletes a file within a hydra repo associated with the following Name.

Assuming this is ran being in the root directory, the following is a template on ways to run
the client with delete.

.. code-block:: bash

    python3 ./client/main.py delete -r <repo-prefix> -f <file-name>

For example:

.. code-block:: bash

    python3 ./client/main.py delete -r /hydra -f /home/a.txt

Queries
-------

Queries a hydra repo to find out how, where, and what information it holds. Queries reach only one node
that responds by looking into it's Global View.

Assuming this is ran being in the root directory, the following is a template on ways to run
the client with a query.

.. code-block:: bash

    python3 ./client/main.py query -r <repo-prefix> -q <query>

For example:

.. code-block:: bash

    python3 ./client/main.py query -r /hydra -q /files

Types implemented so far:
    ``* /files``
    ``* /sids``
    ``* /file/<Name>`` where Name is a name associated with a file.
    ``* /prefix/<Prefix>`` where Prefix is a prefix belonging to an unknown number of file names.

Fetching
--------

Fetches a file from a hydra repo.

Assuming this is ran being in the root directory, the following is a template on ways to run
the client with a fetch.

.. code-block:: bash

    python3 ./client/main.py fetch -r <repo-prefix> -f <file-name> -p <path>

For example:

.. code-block:: bash

    python3 ./client/main.py fetch -r /hydra -f /home/a.txt -p ./client/output/sample.txt