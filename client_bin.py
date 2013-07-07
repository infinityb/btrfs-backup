#!/usr/bin/python

import sys, subprocess
from btrfsbackup.client import client_io, StandardStorageDriver

subproc = subprocess.Popen(
        ['python', './server_bin.py', '/tmp/backups'],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
)

storage_driver = StandardStorageDriver(
    '/btrfs/home', '/btrfs/home.arc'
)

client_io(storage_driver, subproc)
