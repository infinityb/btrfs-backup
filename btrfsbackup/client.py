#!/usr/bin/python
import os
import subprocess
from datetime import datetime
from functools import partial
from contextlib import contextmanager

from .reliable_rw import yield_pieces, yield_pieces_output_manager
from . import wire_pb2
from . import common
from .graphanalyze import BackupDirectedAcyclicGraph

# data = [
#     ('2013-07-01', '2013-07-02'),
#     ('2013-07-01', '2013-07-03'),
#     ('2013-07-01', '2013-07-04'),
#     ('2013-07-01', '2013-07-05'),
#     ('FULL', '2013-07-01')
# ]
# local_nodes = ['2013-07-01', '2013-07-05']
# local_nodes = [
#     x for x in os.listdir('/btrfs/home.arc')
#     if x != 'use_for_incr'
# ]
# graph = BackupDirectedAcyclicGraph(data, local_nodes)


class NonZeroReturn(Exception):
    pass


class StorageDriver(object):
    def get_local_nodes(self):
        raise NotImplementedError
        return [""]

    def node_to_filename(self, node):
        raise NotImplementedError
        return ""

    def generate_node_name(self):
        raise NotImplementedError
        return ""

    @contextmanager
    def get_snapstream(self, from_node, to_node, keep_node=False):
        raise NotImplementedError
        # yield OutputStream


class StandardStorageDriver(StorageDriver):
    def __init__(self, target_subvol, node_root):
        self._target_subvol = target_subvol
        self._node_root = node_root

    def get_local_nodes(self):
        return [
            x for x in os.listdir(self._node_root)
            if os.path.isdir(os.path.join(self._node_root, x))
        ]

    def node_to_filename(self, node):
        return os.path.join(self._node_root, node)

    def generate_node_name(self):
        return datetime.now().isoformat()

    @contextmanager
    def get_snapstream(self, from_node, to_node, keep_node=False):
        retval = subprocess.Popen([
            'btrfs', 'subvolume', 'snapshot', '-r',
            self._target_subvol,
            self.node_to_filename(to_node)
        ]).wait()
        if 0 != retval:
            raise NonZeroReturn("btrfs command failure", retval)
        try:
            args = [
                'btrfs', 'send', '-p',
                self.node_to_filename(from_node),
                self.node_to_filename(to_node)
            ]
            print ' '.join(args)
            subproc = subprocess.Popen(args, stdout=subprocess.PIPE)
            yield subproc
            subproc.stdout.close()
            retval = subproc.wait()
            if 0 != retval:
                raise NonZeroReturn("btrfs command failure", retval)
        except:
            try:
                subproc.kill()
            except OSError:
                pass
            raise
        finally:
            subproc.stdout.close()
            if not keep_node:
                retval = subprocess.Popen([
                    'btrfs', 'subvolume', 'delete',
                    self.node_to_filename(to_node)
                ]).wait()
                if 0 != retval:
                    raise NonZeroReturn("btrfs command failure", retval)


def client_io(storage_driver, subprocess):
    write_framed = partial(
        common.write_framed,
        '!I',
        subprocess.stdin
    )
    read_framed = partial(
        common.read_framed,
        '!I',
        subprocess.stdout
    )

    # load graph
    subprocess.stdin.write(common.magic_number)
    graph = wire_pb2.Graph()
    graph.ParseFromString(read_framed())

    # select parent and make edge (parent, current)
    local_nodes = storage_driver.get_local_nodes()
    best_parent = BackupDirectedAcyclicGraph(graph, local_nodes).best_parent()

    newshot_node = storage_driver.generate_node_name()

    edge = wire_pb2.Graph.GraphEdge()
    edge.from_node = best_parent
    edge.to_node = newshot_node
    write_framed(edge.SerializeToString())
    subprocess.stdout.close()

    with yield_pieces_output_manager(subprocess.stdin) as sink:
        with storage_driver.get_snapstream(best_parent, newshot_node) as btrfs_send:
            for piece in yield_pieces(btrfs_send.stdout, with_magic=False):
                sink.write(piece)

    subprocess.stdin.close()
    subprocess.wait()
