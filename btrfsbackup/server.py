#!/usr/bin/python
from __future__ import absolute_import

import os
from functools import partial

from .reliable_rw import transactional_write, yield_input
from . import wire_pb2
from . import common


def reliable_decode():
    pass


class StorageDriver(object):
    def get_edges(self):
        raise NotImplementedError
        return [("from", "to")]

    def generate_filename(self, from_, to_):
        raise NotImplementedError
        return "%(from)s->%(to)s" % {'from': from_, 'to': to_}

    def open_file(self, from_, to_):
        raise NotImplementedError
        return open(os.path.devnull, 'w')


class StandardStorageDriver(StorageDriver):
    def __init__(self, pool_root):
        self._pool_root = pool_root

    def filename_to_edge(self, filename):
        return tuple(filename[0:-6].split('__', 1))

    def get_edges(self):
        filter_condition = lambda x: '__' in x and x.endswith('.btrfs')
        all_files = os.listdir(self._pool_root)
        return map(
            self.filename_to_edge,
            filter(filter_condition, all_files)
        )

    def generate_filename(self, from_, to_):
        return "%s__%s.btrfs" % (from_, to_)

    def generate_fullpath(self, *args):
        return os.path.join(self._pool_root, self.generate_filename(*args))


def server_io(driver, instream, outstream):
    write_framed = partial(common.write_framed, '!I', outstream)
    read_framed = partial(common.read_framed, '!I', instream)

    def _serialize_graph():
        graph = wire_pb2.Graph()
        for from_node, to_node in driver.get_edges():
            edge = graph.edges.add()
            edge.from_node = from_node
            edge.to_node = to_node
        return graph.SerializeToString()

    # validate magic number
    if instream.read(len(common.magic_number)) != common.magic_number:
        raise Exception("Invalid magic number")

    # send graph
    write_framed(_serialize_graph())
    outstream.flush()

    # what edge are we saving?
    edge = wire_pb2.Graph.GraphEdge()
    edge.ParseFromString(read_framed())

    # do the saving
    filename = driver.generate_fullpath(edge.from_node, edge.to_node)
    with transactional_write(filename) as sink:
        for piece in yield_input(instream):
            sink.write(piece)
