#!/usr/bin/python
import sys, subprocess

head = lambda arry: (arry[0], arry[1:])
cmdline = list(sys.argv)

if len(cmdline) < 2:
    raise Exception("Invalid invocation")

exe_name, cmdline = head(cmdline)

if cmdline[0] == "--server":
    from btrfsbackup.server import server_io, StandardStorageDriver
    server_io(
        StandardStorageDriver(cmdline[1]),
        sys.stdin,
        sys.stdout
    )
else:
    from btrfsbackup.client import client_io, StandardStorageDriver
    subproc = subprocess.Popen(
        cmdline, # ['python', './server.py', '/tmp/backups'],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    storage_driver = StandardStorageDriver(
        '/btrfs/home', '/btrfs/home.arc'
    )
    client_io(storage_driver, subproc)

