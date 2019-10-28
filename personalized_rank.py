##

# Graph 2.0
# 'personalized_rank'
# Gide Inc. 2019

##

"""

One node in the graph is designated as the source node. Personalize page rank is performed with respect to that
source node.

Personalized page rank is initialized by assigning all probability mass to the source node and none to the other
nodes. In contrast, ordinary page rank is initialized by giving all nodes the same probability mass.
Whenever personalized page rank makes a random jump, it jumps back to the source node. In contrast, ordinary page rank
may jump to any node.

In personalized page rank, all probability mass lost dandling nodes is put back into the source nodes

"""

from graph_extract import GraphExtract


class PersonalizedRank:

    def __init__(self, graph_data):

        self.graph_data = graph_data

    def weight_calculation(self, graph_data, num_iter, pR, jump_fac):

        """

        :param graph_data:
        :param num_iter:
        :param pR:
        :param jump_fac:
        :return:
        """

        for i in range(num_iter):
            # First iteration: store initial page rank from before to pRLast (saves information)
            # Future iterations: store any updated page rank info to pRLast
            pRLast = pR.collectAsMap()

            pR = graph_data.map(lambda nodes: (nodes[0], 0))

        return None

    def personalized_page_rank(self, sc_node_id, num_iter, jump_fac):

        """

        :param sc_node_id:
        :param num_iter:
        :param jump_fac:
        :return:
        """

        ge = GraphExtract()
        graph_data = self.graph_data

        # Graph linkages: each starting node connects directly to the nodes in [1:]
        graph_data = ge.graph_extract(graph_data)

        graph_data = graph_data.map(lambda nodes: (nodes[0], nodes[1:] if nodes != [] else []))

        # C(m) --> outDegrees: dict of key (page m) and number of links on the key m
        # outDegrees determine dangling nodes as well. If the value is 0, the node is considered dangling
        out_set = ge.out_deg_set(graph_data).cache()
        out_set_count = ge.out_counts(graph_data).cache()

        # returns as a dictionary of outdegree keys and the list of the outdegree values
        # returns as a dictionary of outdegree keys and the size of the outdegree values
        out_set_dict = out_set.collectAsMap()
        out_set_count_dict = out_set_count.collectAsMap()

        #
        in_set = ge.in_deg_set(graph_data).cache()
        in_set_count = ge.in_counts(graph_data)

        pR = graph_data.map(lambda nodes: (nodes[0], 1 if sc_node_id == nodes[0] else 0))

        alpha = jump_fac
        neg_alpha = (1 - alpha)

        for index in range(num_iter):
            '''
            
            '''

            pRLast = pR.collectAsMap()
            pR = graph_data.map(lambda nodes: (nodes[0], 0))

            # Group in-degrees information (with values as list of server nodes in degree to the keys) into pR
            sumFactor = pR.leftOuterJoin(in_set)

            # Calculate using information from previous page rank information and using the group in-deg info. to sum
            # up P(m) / C(m), where m is the element in the in-degree set (got joined into the RDD), P(m) is the prob
            # mass of each server node m, and C(m) is the out-degree size of m.

            # If the key is source node, alpha + (1 - alpha) * sum of all the info for P(M) / C(m)
            # If key is not source node, (1 - alpha) * sum of all the info for P(m) / C(m)

            pn = sumFactor.map(lambda x: (x[0], (x[1][0], [] if x[1][1] is None else x[1][1]))) \
                .map(lambda x: (x[0], alpha + neg_alpha * sum([pRLast[m] / out_set_count_dict[m]
                                                               for m in x[1][1]]) if x[0] == sc_node_id else
            neg_alpha * sum([pRLast[m] / out_set_count_dict[m] for m in x[1][1]])))

            # Looking into dangling nodes weight transfer to source node
            pnDict = pn.collectAsMap()

            # Store any information from out-degrees (with values as list of server nodes out
            # degree from the keys) into the sumFactorResult we obtained
            pR = pn.leftOuterJoin(out_set).cache()

            # Find any dangling nodes that is adjacent to the source node and move weight values into the source node.
            # At the same, the dangling nodes will have weight 0
            pRResult = pR.map(lambda x: (x[0], (x[1][0] + sum([pnDict[i] if out_set_dict[i] == 0
                                                               else 0 for i in x[1][1]]) if x[0] == sc_node_id else
                                                x[1][0], x[1][1]))) \
                .map(lambda x: (x[0], 0 if x[0] in out_set_dict[sc_node_id] and x[1][1] == [] else x[1][0]))

            pR = pRResult

        resulting_page_rank = pR.sortBy(lambda x: x[1], False)
        return resulting_page_rank

    def personalized_page_rank_niter(self, sc_node_id, jump_fac):

        return
