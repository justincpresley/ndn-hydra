Examples
========

For the basic example, we will be having all nodes fall under one NFD.
This means we can eliminate routing for a quick run.

**1. Edit NFD config to see things happen**

.. code-block:: bash

    cs_max_packets 0

**2. Start / Restart NFD**

**3. Configure Repo Prefix Strategy**

.. code-block:: bash

    $ nfdc strategy set /<repo-prefix>/group /localhost/nfd/strategy/multicast/%FD%03

**4. Run the following on 4 or more terminals.**

.. code-block:: bash

    $ cd ndn-hydra # inside root directory
    $ python3 ./hydra/main/main.py -rp <repo-prefix> -n <node-name> -s <sid>

- *repo_prefix* : The registered-multicast group prefix for all under repo. All should be ran with the same prefix. (example: /hydra)
- *node_name* : A unique, per node, name. Remains constant through restarts. (example: node1)
- *sid* : A session id. Always unique, create new on restart. (example: 2c4f)

**5. On a seperate terminal, run all client interactions**

Client needs to also be in the root directory :literal:`cd ndn-hydra`.
Running :literal:`python3 ./client/main.py` will help you see all choices you have.

* Insertion

.. code-block:: bash

    $ python3 ./client/main.py insert -r <repo_prefix> -f /home/a.txt -p ./client/files/test.txt

* Query

.. code-block:: bash

    $ python3 ./client/main.py query -r <repo_prefix> -q /files

* Fetch

.. code-block:: bash

    $ python3 ./client/main.py fetch -r <repo_prefix> -f /home/a.txt ./client/files/output.txt

* Deletion

.. code-block:: bash

    $ python3 ./client/main.py delete -r <repo_prefix> -f /home/a.txt