Client
======

The Client is very forgiving. Running ``python3 ./examples/client.py`` will tell you what options
are available. Furthermore, running ``python3 ./examples/client.py <function>`` with a appropriate
function out of the list {insert,delete,fetch,query} will tell you exactly what you need/what is missing.

Information
-----------

Both the Client and Repo have [-h] for help on how to run and [-v] for getting the current version.

Insertion
---------

Inserts a local file given by the path within a hydra repo associated with the following Name.

Assuming this is ran being in the root directory, the following is a template on ways to run
the client with insert.

.. code-block:: bash

    python3 ./examples/client.py insert -r <repo-prefix> -f <file-name> -p <path>

For example:

.. code-block:: bash

    python3 ./examples/client.py insert -r /hydra -f /home/a.txt -p ./examples/files/10kb.txt

Deletion
--------

Deletes a file within a hydra repo associated with the following Name.

Assuming this is ran being in the root directory, the following is a template on ways to run
the client with delete.

.. code-block:: bash

    python3 ./examples/client.py delete -r <repo-prefix> -f <file-name>

For example:

.. code-block:: bash

    python3 ./examples/client.py delete -r /hydra -f /home/a.txt

Queries
-------

Queries a hydra repo to find out how, where, and what information it holds. Queries reach only one node
that responds by looking into it's Global View.

Assuming this is ran being in the root directory, the following is a template on ways to run
the client with a query.

.. code-block:: bash

    python3 ./examples/client.py query -r <repo-prefix> -q <query> [-s <sessionid>]

For example:

.. code-block:: bash

    python3 ./examples/client.py query -r /hydra -q /files

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

    python3 ./examples/client.py fetch -r <repo-prefix> -f <file-name> [-p <path>]

For example:

.. code-block:: bash

    python3 ./examples/client.py fetch -r /hydra -f /home/a.txt -p ./examples/output/sample.txt