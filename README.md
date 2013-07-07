What is this thing?
------------------------
`btrfs-backup` is a network-capable `btrfs send` file management system.
`btrfs-backup` manages a set of snapshots on a client and a graph of snapshot
files on a server. This allows you to define an algorithm to choose which of
the local snapshots to delta against based on the properties of graph provided
by the server.  For example, you could make a full backup monthly, an
incremental of depth 1 weekly, an incremental of depth 2 daily and an
incremental of depth 3 hourly.


Example
------------------------

    TARGET_SUBVOLUME=/btrfs/home
    SUBVOLUME_SNAPSHOT_DIR=/btrfs/home.arc
    btrfs-backup "$TARGET_SUBVOLUME" "$SUBVOLUME_SNAPSHOT_DIR" \
        ssh sell@10.0.1.2 \
        btrfs-backup --server /mnt/btrpool1/backups/chiaki_home

Please note we are not using a pipe between btrfs-backup and ssh.
btrfs-backup will create a subprocess with whatever arguments you
pass after the first two.  The only requirement is that it conforms
to the interface that `btrfs-backup --server` exposes.


Requirements
------------------------
* A recent version of Python 2.x
* Google's Protocol Buffers library for Python.  This can be found in the `python-protobuf` package in Debian-based distributions.


TODO
------------------------
* Use argparse
* Implement some nice parent selection algorithms since none exist right now.
