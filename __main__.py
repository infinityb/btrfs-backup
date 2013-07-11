#!/usr/bin/python
import sys
import subprocess


head = lambda arry: (arry[0], arry[1:])
cmdline = list(sys.argv)

if len(cmdline) < 2:
    raise Exception("Invalid invocation")


def _server(backup_root):
    from btrfsbackup.server import server_io, StandardStorageDriver
    server_io(
        StandardStorageDriver(backup_root),
        sys.stdin,
        sys.stdout
    )


def _client(subvolume_path, local_repo, *args):
    from btrfsbackup.client import client_io, StandardStorageDriver
    from btrfsbackup.graphanalyze import MonthWeekDayHourTree
    subproc = subprocess.Popen(
        args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )

    storage_driver = StandardStorageDriver(
        subvolume,
        local_repo
    )
    client_io(storage_driver, MonthWeekDayHourTree, subproc)


_, cmdline = head(cmdline)
if cmdline[0] == "--server":
    _server(cmdline[1])
else:
    args = cmdline
    subvolume, args = head(args)
    local_repo, args = head(args)
    _client(subvolume, local_repo, *args)
