#!/usr/bin/python


def delete_local_node(node_name):
    print "delete %r" % node_name


class PathNotFound(Exception):
    pass


class BackupDirectedAcyclicGraph(object):
    ROOT_NODE = 'FULL'
    MAX_HEIGHT = 3
    MAX_CHILDREN_FOR_DEPTH = {
        1: 4,
        2: 8,
        3: 24
    }

    def __init__(self, records, local_nodes):
        self._forward = self.make_node_dict(records)
        self._reverse = self.make_inverted_node_dict(records)
        self._local_nodes = local_nodes

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

    def _height_helper(self, from_subroot):
        children = self._forward.get(from_subroot, [])
        if not children:
            return 0
        return 1 + max(self._height_helper(child) for child in children)

    def height(self):
        return self._height_helper('FULL')

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

    def clean_local_nodes(self):
        for node in sorted(self._local_nodes, reverse=True):
            if self.MAX_CHILDREN <= len(self._forward.get(node, [])):
                delete_local_node(node)
                self._local_nodes.remove(node)

    def best_parent(self):
        return sorted(set(self._local_nodes) & set(self._reverse), reverse=True)[0]
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
