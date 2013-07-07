#!/usr/bin/python
import sys
from btrfsbackup.server import server_io, StandardStorageDriver

import sys

server_io(
        StandardStorageDriver(sys.argv[1]),
        sys.stdin,
        sys.stdout
)

