"""
A (very) dangerous task that you may have to use. This task will delete files that
are older than a certain age, subject to some (optional) constraints:

a) The file must have $N$ remote instances available throughout the network
b) The checksums of those files must match the original checksum
"""

from .task import Task


class RollingDeletion(Task):
    """
    A background task that deletes _instances_ (not files!) that are older than
    a certain age.
    """

    pass
