Background Tasks
================

Background tasks are persistent tasks that run alongside the
librarian web server. They have access to the same database,
but they need not even run in the same container, and are
hence extremely flexible and scalable.

It is typical to run the background tasks in a separate
thread when running the librarian server (this is handled
automatically by the ``librarian-server-start`` script).
Most installations of the librarian will find that this is
more than suitable for their needs. For more complex scenarios,
where multithreaded web servers are required, may find it
useful to run the background tasks separately with the
``librarian-background-only`` script.

The background tasks are defined in the separate
``librarian_background`` package, and are configured
using a json file pointed to by ``$LIBRARIAN_BACKGROUND_CONFIG``.

Each background task is configured using a small json
object. The following two configurations are required,
for example, for a SneakerNet transfer:

.. code-block::json
    {
      "create_local_clone": [
          {
              "task_name": "Local cloner",
              "soft_timeout": "00:30:00",
              "every": "01:00:00",
              "age_in_days": 7,
              "clone_from": "store",
              "clone_to": ["clone"],
              "files_per_run": 256,
          }
      ],
      "recieve_clone": [
          {
              "task_name": "Clone receiver",
              "soft_timeout": "00:30:00",
              "every": "01:00:00",
              "files_per_run": 256,
          }
      ]
    }

Background Task Types
---------------------

All background tasks have the following shared configuration variables:

- ``task_name``: A human-readable name for the task.
- ``soft_timeout``: The maximum amount of time the task should run before being killed.
  This is, as stated, a soft time-out, meaning that all tasks will check this time-out
  (typically) after every file iteration. This is a string that can be parsed by the
  ``dateutil`` library to a ``datetime.timedelta`` object (e.g. HH:MM:SS)
- ``every``: The frequency at which the task should run. This is a string that can be
  parsed by the ``dateutil`` library to a ``datetime.timedelta`` object (e.g. HH:MM:SS)

The following background tasks are available:

- ``check_integrity``: Check the integrity of the files in the librarian. This runs
  through all files uploaded recently and checks their integrity against the local
  MD5sum that was generated at ingest. This is really useful in cases where you are not
  frequently accessing or transfering the files (as the integrity check is done
  on transfers anyway). This task is configured with the following additional
  parameters:

  * ``age_in_days``: The number of days back to check for files to verify (integer).
  * ``store_name``: The store to check the integrity of (string).
- ``create_local_clone``: Create a local clone of the files in the librarian. This
  task is configured with the following additional parameters:

  * ``age_in_days``: The number of days back to check for files to clone (ineger).
  * ``clone_from``: The store to clone from (string).
  * ``clone_to``: The stores to clone to (a list of strings); only one copy is created
    across all of these stores.
  * ``files_per_run``: The number of files to clone in each run (integer). This
    works in concert with the soft timeout flag to make sure that the system keeps
    moving.
  * ``disable_store_on_full``: A boolean flag that determines what happens when
    one of the stores is full. If true, the destination store is disabled once
    it is full. This is useful in cases where you have multiple destination stores
    (e.g. multiple SneakerNet drives) that you are progressively filling up.
- ``send_clone``: Send a clone of the files in the librarian to a destination librarian.
  This generates tasks in a queue that are picked up by other background tasks for
  the actual egress. This task is configured with the following additional parameters:

  * ``destination_librarian``: The name of the destination librarian in the internal
    database (string)
  * ``age_in_days``: The number of days back to check for files to send (integer).
  * ``store_preference``: A preference for the store to send from (string). This is
    useful in the case where you have SneakerNet drives attached, as it will prefer
    the main store instead of the SneakerNet drives for copies.
  * ``send_batch_size``: The sizes of file batches to send (interger).
  * ``warn_disabled_timer``: The number of days to wait before warning about a disabled
    store (integer).
- ``receive_clone``: Receive a clone of the files in the librarian from a source librarian.
  This ingests copies of files from the staging area to the store area.
  This task is configured with the following additional parameters:

  * ``deletion_policy``: Whether new files can be deleted from the store or not.
    Can be one of ``DISALLOWED`` or ``ALLOWED`` (string).
  * ``files_per_run``: The number of files to receive in each run (integer).
- ``consume_queue``: Consume the queue of files to send to a destination librarian. This
  task must be enabled if you would like to send files to a destination using the
  ``send_clone`` task.
- ``check_consumed_queue``: Check the consumed queue of files to send to a destination librarian.
  This task must be enabled if you would like to send files to a destination using the
  ``send_clone`` task.
- ``incoming_transfer_hypervisor``: A hypervisor for incoming transfers; if transfers have
  aged out we check on their status by calling up the upstream librarian and making
  appropriate changes to the status. It is recommended to run this on any librarian that is
  recieving incoming transfers as things can age out for many reasons.
  This task is configured with the following additional parameters:

  * ``age_in_days``: The number of days back to check for files to transfer (integer).
- ``outgoing_transfer_hypervisor``: A hypervisor for outgoing transfers; if transfers have
  aged out we check on their status by calling up the downstream librarian and making
  appropriate changes to the status. It is recommended to run this on any librarian that is
  sending outgoing transfers as things can age out for many reasons.
  This task is configured with the following additional parameters:
  
  * ``age_in_days``: The number of days back to check for files to transfer (integer).
- ``duplicate_remote_instance_hypervisor``: A hypervisor that looks for duplicate remote
  instance rows int he table and removes one. This ensures database integrity and that
  the count of remote instances per librarian and store corresponds to the number of files
  on that librarian.
- ``rolling_deletion``: A task to delete data that was ingested into the librarian
  more than ``age_in_days`` ago. Note that this does not delete them from the entire network,
  and has specific tools to ensure other copies exist in the network elsewhere:

  * ``store_name``: The store to delete instances from
  * ``age_in_days``: The number of days old data needs to be to be considered for deletion
  * ``number_of_remote_copies``: The number of copies in the rest of the network (which are
    validated using checksumming) that must be kept before deleting a local instance.
  * ``verifiy_downstream_checksums``: Whether to make sure all downstream checksums that were
    computed on request match the underlying data before deletion (True).
  * ``mark_unavailable``: Whether to mark instances as unavailable (True) or actually remove the
    rows in the table (False). Default True.
  * ``force_deletion``: Whether to ignore the legacy DeletionPolicy parameter (True).
- ``corruption_fixer``: A task that reaches out to upstream librarians to ask for new copies of
  corrupt files in the table. These corrupt files can be found by the ``check_integrity`` task
  or when upstreams validate files during the deletion process.


Background Task Configuration Examples
--------------------------------------

Below, we provide some examples of background task configurations for various
scenarios.

SneakerNet Transfer (Source)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following configuration will create a local clone of the files in the librarian
every hour, and will clone files that are up to 7 days old. The clone will be created
in the ``clone`` store, and will clone from the ``store`` store. The system will clone
256 files per run, and will disable the ``clone`` store if it is full.

.. code-block:: json

  {
    "create_local_clone": [
      {
        "task_name": "Local cloner",
        "soft_timeout": "00:30:00",
        "every": "01:00:00",
        "age_in_days": 7,
        "clone_from": "store",
        "clone_to": ["clone"],
        "files_per_run": 256,
        "disable_store_on_full": true
      }
    ]
  }

Inter-Librarian Transfer (Source)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following configuration will send a clone of the files in the librarian to a
destination librarian every hour, and will send files that are up to 7 days old.
The system will send 128 files per batch, and will prefer to send from the ``store``
store. The destination librarian is called ``destination``.

.. code-block:: json

  {
    "send_clone": [
      {
        "task_name": "Clone sender",
        "soft_timeout": "00:30:00",
        "every": "01:00:00",
        "age_in_days": 7,
        "store_preference": "store",
        "send_batch_size": 128,
        "destination_librarian": "destination"
      }
    ],
    "consume_queue": [
      {
        "task_name": "Queue consumer",
        "soft_timeout": "00:30:00",
        "every": "01:00:00"
      }
    ],
    "check_consumed_queue": [
      {
        "task_name": "Queue checker",
        "soft_timeout": "00:30:00",
        "every": "01:00:00"
      }
    ],
    "outgoing_transfer_hypervisor": [
      {
        "task_name": "Outgoing transfer hypervisor",
        "soft_timeout": "00:30:00",
        "every": "01:00:00",
        "age_in_days": 2
      }
    ],
    "duplicate_remote_instance_hypervisor": [
      {
        "task_name": "Duplicate RI hypervisor",
        "soft_timeout": "00:30:00",
        "every": "24:00:00"
      }
    ],
    "rolling_deletion": [
      {
        "task_name": "Storage Recovery",
        "soft_timeout": "00:30:00",
        "every": "24:00:00",
        "store_name": "store",
        "number_of_remote_copies": 2
      }
    ]
  }
    

Inter-Librarian Transfer (Destination)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following configuration will receive a clone of the files in the librarian from a
source librarian every hour, and this can be via SneakerNet or via the network.
The system will receive 1024 files per batch, and will not allow new files to be
deleted from the store.

.. code-block:: json

  {
    "receive_clone": [
      {
        "task_name": "Clone receiver",
        "soft_timeout": "00:30:00",
        "every": "01:00:00",
        "deletion_policy": "DISALLOWED",
        "files_per_run": 1024
      }
    ],
    "incoming_transfer_hypervisor": [
      {
        "task_name": "Incoming transfer hypervisor",
        "soft_timeout": "00:30:00",
        "every": "01:00:00",
        "age_in_days": 2
      }
    ],
    "corruption_fixer": [
      {
        "task_name": "Corruption fixer",
        "soft_timeout": "00:30:00",
        "every": "24:00:00"
      }
    ]
  }
