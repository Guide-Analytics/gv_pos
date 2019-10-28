##

# Graph 2.0
# 'graph_extract'
# Gide Inc. 2019

# This is a static class file and will not be documented

##


class GraphExtract:

    def lower_case(self, graph_data):

        '''

        :param graph_data:
        :return:
        '''

        graph_lower_case = graph_data.map(lambda nodes: nodes.lower())

        return graph_lower_case

    def convert_to_bin(self, graph_data):

        '''

        :param graph_data:
        :return:
        '''

        graph_bin = graph_data.map(lambda nodes: [self.bin_conversion(node) for node in nodes])

        return graph_bin

    def bin_conversion(self, data):

        '''

        :param graph_data:
        :return:
        '''

        res = ''
        for s in data:
            res = res.join(format(ord(s), 'b'))

        return res


    def graph_extract(self, graph_data):

        '''

        :param graph_data:
        :return:
        '''
        graph_data = graph_data.map(lambda nodes: nodes.split(", "))

        return graph_data

    def num_edges(self, graph_data):

        '''

        :purpose: count edges by splitting the direction "\t" from the initial serve node to the other servers nodes
        in the same adjacency list. Then count the size of that list (subtract 1) to get the edges
        :param graph_data:
        :return: tuple (num_nodes, num_edges)
        '''
        graph_edges = graph_data.map(lambda nodes: len(nodes.split(",")) - 1)\
                                .reduce(lambda x, y: x + y)

        return graph_edges

    def num_nodes(self, graph_data):

        '''

        :param graph_data: RDD
        :return graph_res: dict
        '''

        graph_nodes = graph_data.flatMap(lambda nodes: nodes.split(",")).distinct().count()

        return graph_nodes

    def out_counts(self, graph_data):

        '''

        :param graph_data: RDD
        :return graph_res: dict
        '''

        graph_res = graph_data.map(lambda nodes: (nodes[0], len(nodes[1]) if nodes != [] else 0))

        return graph_res

    def out_deg_set(self, graph_data):

        '''

        :param graph_data: RDD
        :return graph_set: RDD (set)
        '''

        graph_set = graph_data.map(lambda nodes: (nodes[0], [node for node in nodes[1]]))

        return graph_set

    def in_counts(self, graph_data):

        '''


        :param graph_data: RDD
        :return graph_res: dict
        '''

        graph_res = graph_data.map(lambda nodes: nodes.split(","))\
                              .map(lambda nodes: (nodes[0], nodes[1:] if nodes != [] else []))

        graph_res = graph_res.map(lambda nodes: [(i, nodes[0]) for i in nodes[1]])\
                             .flatMap(lambda x: x).groupByKey()

        graph_res = graph_res.map(lambda x: (x[0], len(list(x[1]))))

        return graph_res

    def in_deg_set(self, graph_data):

        '''

        :param graph_data:
        :return:
        '''

        graph_set = graph_data.map(lambda nodes: (nodes[0], nodes[1] if nodes != [] else []))\
                              .map(lambda nodes: [(i, nodes[0]) for i in nodes[1]]).flatMap(lambda x: x).groupByKey()\
                              .map(lambda x: (x[0], [i for i in list(x[1])]))

        return graph_set
