#!/usr/bin/python
from datetime import datetime


class PathNotFound(Exception):
    pass


class DirectedGraph(object):
    ROOT_NODE = 'FULL'

    @classmethod
    def make_node_dict(cls, graph):
        out = dict()
        for edge in graph.edges:
            if edge.from_node not in out:
                out[edge.from_node] = []
            out[edge.from_node].append(edge.to_node)
        return out

    @classmethod
    def make_inverted_node_dict(cls, graph):
        out = dict()
        for edge in graph.edges:
            if edge.to_node not in out:
                out[edge.to_node] = []
            out[edge.to_node].append(edge.from_node)
        return out

    def __init__(self, graph):
        self._forward = self.make_node_dict(graph)
        self._reverse = self.make_inverted_node_dict(graph)

    def _height_helper(self, from_subroot):
        children = self._forward.get(from_subroot, [])
        if not children:
            return 0
        return 1 + max(self._height_helper(child) for child in children)

    def height(self):
        return self._height_helper('FULL')

    def node_height(self, node_name):
        if node_name == 'FULL':
            return 0
        return 1 + min(map(self.node_height, self._reverse[node_name]))

    def paths_from(self, source):
        parents = self._reverse.get(source, [])
        for parent in parents:
            for path in self.paths_from(parent):
                yield [source] + path
        yield [source]

    def min_path(self, source, target):
        print "searching: %s->%s" % (source, target)
        try:
            return min(
                (len(path) - 1, path)
                for path in self.paths_from(source)
                if path[-1] == target
            )[1]
        except ValueError:
            raise PathNotFound


class BackupDirectedGraph(DirectedGraph):
    MAX_HEIGHT = 3
    MAX_CHILDREN_FOR_DEPTH = {
        1: 4,
        2: 8,
        3: 24
    }

    @classmethod
    def generate_node_name(cls):
        return datetime.now().isoformat()

    @classmethod
    def _serialize_node_name(cls, node_datetime):
        return node_datetime.iso8601()

    @classmethod
    def _parse_node_name(cls, node_name):
        return datetime.strptime(node_name, "%Y-%m-%dT%H:%M:%S")

    def __init__(self, graph, local_nodes):
        super(BackupDirectedGraph, self).__init__(graph)
        self._local_nodes = local_nodes
        self._available_parents = set(self._local_nodes) & set(self._reverse)

    def clean_local_nodes(self, storage_driver):
        for node in sorted(self._local_nodes, reverse=True):
            if self.MAX_CHILDREN <= len(self._forward.get(node, [])):
                storage_driver.delete_node(node)
                self._local_nodes.remove(node)

    def best_parent(self):
        return sorted(self._available_parents, reverse=True)[0]
        # find most recent backup who meets the following conditions:
        #    whose depth is less than MAX_HEIGHT
        #    who has fewer than the maximum allowed children
        # If none are found, return the root.
        for node in sorted(self._local_nodes, reverse=True):
            depth = len(self.min_path(node, self.ROOT_NODE)) - 1
            assert depth > 0
            child_count = len(self._forward.get(node, []))
            child_max = self.MAX_CHILDREN_FOR_DEPTH[depth]
            if child_count < child_max and depth < self.MAX_HEIGHT:
                return node
        return self.ROOT_NODE


def _same_month(left, right):
    return left.year == right.year and left.month == right.month


def _same_week(left, right):
    return (
        _same_month(left, right) and
        left.isocalendar()[1] == right.isocalendar()[1]
    )


def _same_day(left, right):
    return (
        _same_week(left, right) and
        left.day == right.day
    )


class MonthWeekDayHourTree(DirectedGraph):
    # FULL
    # +- monthly
    # |  +- weekly
    # |  |  +- daily
    # |  |  |  +- hourly
    # |  |  |  +- hourly
    # |  |  |
    # |  |  ...
    # |  ...
    # +- monthly
    # ...
    HOURLY_DEPTH = 4
    DAILY_DEPTH = 3
    WEEKLY_DEPTH = 2
    MONTHLY_DEPTH = 1

    ALLOW_CHILD = {
        0: lambda left, right: True,
        1: _same_month,
        2: _same_week,
        3: _same_day,
        4: lambda left, right: False
    }

    @classmethod
    def generate_node_name(cls):
        return datetime.now().isoformat()

    @classmethod
    def _serialize_node_name(cls, node_datetime):
        return node_datetime.isoformat()

    @classmethod
    def _parse_node_name(cls, node_name):
        return datetime.strptime(node_name, "%Y-%m-%dT%H:%M:%S.%f")

    def __init__(self, graph, local_nodes):
        super(MonthWeekDayHourTree, self).__init__(graph)
        # self._to_nodes = set(self._reverse)
        # self._from_nodes = set(self._forward)
        self._local_nodes = set(local_nodes)
        self._available_parents = set(self._reverse) & self._local_nodes
        self._nodes_by_height = {}
        for parent in self._available_parents:
            height = self.node_height(parent)
            if height not in self._nodes_by_height:
                self._nodes_by_height[height] = []
            self._nodes_by_height[height].append(parent)

    def clean_local_nodes(self, storage_driver):
        now = datetime.now()
        keep = set()
        for node in self._local_nodes:
            try:
                node_obj = self._parse_node_name(node)
            except ValueError:
                continue
            if self.MONTHLY_DEPTH in self._nodes_by_height:
                if node in self._nodes_by_height[self.MONTHLY_DEPTH]:
                    if _same_month(node_obj, now):
                        keep.add(node)
            if self.WEEKLY_DEPTH in self._nodes_by_height:
                if node in self._nodes_by_height[self.WEEKLY_DEPTH]:
                    if _same_week(node_obj, now):
                        keep.add(node)
            if self.DAILY_DEPTH in self._nodes_by_height:
                if node in self._nodes_by_height[self.DAILY_DEPTH]:
                    if _same_week(node_obj, now):
                        keep.add(node)
        to_delete = list(self._local_nodes - keep)
        if to_delete:
            storage_driver.delete_node(to_delete)

    def _scan_for_parent(self, now):
        for depth in [3, 2, 1]:
            if depth in self._nodes_by_height:
                candidates = list()
                for parent in self._nodes_by_height[depth]:
                    try:
                        parent_obj = self._parse_node_name(parent)
                    except ValueError:
                        pass
                    else:
                        if self.ALLOW_CHILD[depth](parent_obj, now):
                            candidates.append((parent_obj, parent))
                if candidates:
                    return max(candidates)[1]
        return None

    def best_parent(self):
        return self._scan_for_parent(datetime.now())
