import itertools
from queue import Queue

#import findspark

#findspark.init("/usr/local/spark", edit_rc=True)

from pyspark.context import SparkContext

import pdb
import time
import sys

thresholder = int(sys.argv[1])
input_file_name = sys.argv[2]
output_file_betweenness = sys.argv[3]
output_file_community = sys.argv[4]

sc = SparkContext("local[*]", "Elnaz App")

rdd_csv = sc.textFile(input_file_name)


def user_pairs(x):  # make pair of all users for each bussiness
    business = x[0]
    users = x[1]
    result = []
    pairs = itertools.combinations(users, 2)
    pairs = pairs
    for i in pairs:
        i = sorted(i)
        result.append(((i[0], i[1]), [business]))
    return result


def find_edges(s1, all_edges):  # find all the connected user to the user
    result = []
    for item in all_edges:
        if (item[0] == s1):
            result.append(item[1])
        elif (item[1] == s1):
            result.append(item[0])

    return result


def get_betweenness(starrting_node, edges, number_nodes): # calculate betwenness
    q = Queue(maxsize=number_nodes)
    visited_nodes = []
    levels_of_nodes = {}
    parents_node = {}
    weights = {}

    q.put(starrting_node)
    visited_nodes.append(starrting_node)
    levels_of_nodes[starrting_node] = 0
    weights[starrting_node] = 1

    while (q.empty() != True): # as long as the queue is not empty
        node = q.get()         # get first node which is starting node
        children = edges[node]  # find all the user that connected to that user

        for i in children:        # for each users
            if (i not in visited_nodes):    # if child is not vizited
                q.put(i)                     # put that child to que
                parents_node[i] = [node]      # and child i has parent node that get from que
                weights[i] = weights[node]     # child has same weights as its parent

                visited_nodes.append(i)       # count child as vizited node
                levels_of_nodes[i] = levels_of_nodes[node] + 1   # go to next level, increase the level by one
            else:
                if (i != starrting_node):            # if child is vizited and it is not the starting node
                    parents_node[i].append(node)     # the parents of that child is node that  get from que
                    if (levels_of_nodes[node] == levels_of_nodes[i] - 1):  #  decrease level by one
                        weights[i] += weights[node]  # and the weight of that child would be inctease by the weights of the new parent

    order_node = []
    count = 0
    for i in visited_nodes:    # go through all vizited nodes
        order_node.append((i, count))
        count = count + 1
    reverse_order_node = sorted(order_node, key=(lambda x: x[1]), reverse=True)  # reverse the order of the node because we
    # start from leves to calculate the betwenness
    new_reverse_order = []
    nodes_values = {}
    for i in reverse_order_node:
        new_reverse_order.append(i[0])
        nodes_values[i[0]] = 1   # give value one to all nodes

    betweenness_values = {}

    for j in new_reverse_order:
        if (j != starrting_node):
            final_weight = 0
            for i in parents_node[j]: # find all parents of the node j
                if (levels_of_nodes[i] == levels_of_nodes[j] - 1):
                    final_weight += weights[i]

            for i in parents_node[j]:
                if (levels_of_nodes[i] == levels_of_nodes[j] - 1):
                    start = j
                    end = i

                    if start < end:
                        pair = tuple((start, end))
                    else:
                        pair = tuple((end, start))

                    if (pair not in betweenness_values.keys()):
                        betweenness_values[pair] = float(nodes_values[start] * weights[end] / final_weight)
                    else:
                        betweenness_values[pair] += float(nodes_values[start] * weights[end] / final_weight)

                    nodes_values[end] += float(nodes_values[start] * weights[end] / final_weight)

    betweenness_list = []
    for key, value in betweenness_values.items():
        temp = [key, value]
        betweenness_list.append(temp)

    return betweenness_list


def run_bfs(root_node, adjacent_vertices, n_vertices):
    visited = []
    edges = []

    q = Queue(maxsize=n_vertices)

    q.put(root_node)
    visited.append(root_node)

    while (q.empty() != True):
        node = q.get()
        children = adjacent_vertices[node]

        for i in children:
            if (i not in visited):
                q.put(i)
                visited.append(i)

            pair = sorted((node, i))
            if (pair not in edges):
                edges.append(pair)

    return (visited, edges)


def remove_subgraph(remainder_graph, component):  # finding the sungragh and remove it from whole gragh
    component_vertices = component[0]



    for item in component_vertices:
        del remainder_graph[item]


    for i in remainder_graph.keys():
        adj_list = remainder_graph[i]
        for item in component_vertices:
            if (item in adj_list):
                adj_list.remove(i[1])
        remainder_graph[i] = adj_list

    return remainder_graph


# basicly we run bfs for each node and find all the connected nodes to that node and we remove it from whole gragh,
# if there is no node let, it means they are all connected

def get_connected_nodes(adjacent_vertices):

    connected_nodes = []
    remainder_graph = adjacent_vertices

    while (any(remainder_graph) == True):
        vertices = []

        for key, value in remainder_graph.items():
            vertices.append(key)

        vertices = list(set(vertices))

        root = vertices[0]
        subgraph = run_bfs(root, adjacent_vertices, len(vertices))
        connected_nodes.append(subgraph)
        remainder_graph = remove_subgraph(remainder_graph, subgraph)

    return connected_nodes


def check_modularity(adjacent_vertices, connected_nodes, m):
    modularity = 0
    for item in connected_nodes:
        item_vertices = item[0]

        for i in item_vertices:
            for j in item_vertices:
                matrixij = 0
                adj_list = adjacent_vertices[str(i)]
                if (j in adj_list):
                    matrixij = 1

                expected_edgei = len(adjacent_vertices[i])
                expected_edgej = len(adjacent_vertices[j])

                modularity += matrixij - (expected_edgei * expected_edgej) / (2 * m)

    modularity = modularity / (2 * m)
    return modularity


def build_node_matrix(connected_nodes):
    result = {}
    for item in connected_nodes:

        item_edges = item[1]

        for i in item_edges:
            if (i[0] in result.keys()):
                result[i[0]].append(i[1])
            else:
                result[i[0]] = [i[1]]

            if (i[1] in result.keys()):
                result[i[1]].append(i[0])
            else:
                result[i[1]] = [i[0]]

    return result


def remove_edge(adjacency_matrix, edge_with_highest_betwenness):
    # we have the edge that has highest betwenness , we find it in the dictionary
    # we find the nodes of that edge and we make sure that we remove it in dictonary, I mean we make sure that when we remove the egde
    # thoes node are not connected any more
    if (edge_with_highest_betwenness[0] in adjacency_matrix.keys()):
        nodes = adjacency_matrix[edge_with_highest_betwenness[0]]
        if (edge_with_highest_betwenness[1] in nodes):
            nodes.remove(edge_with_highest_betwenness[1])

    if (edge_with_highest_betwenness[1] in adjacency_matrix.keys()):
        nodes = adjacency_matrix[edge_with_highest_betwenness[1]]
        if (edge_with_highest_betwenness[0] in nodes):
            nodes.remove(edge_with_highest_betwenness[0])
    return adjacency_matrix


dataHeader = rdd_csv.first()
data = rdd_csv.filter(lambda x: x != dataHeader).map(lambda x: x.split(','))
# reading data line by line and split elemets in each line by comma

business_users = data.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x, y: x + y)
# read user as key and bussines as value. and reduce it by user
pair_user = business_users.flatMap(lambda x: user_pairs(x)).reduceByKey(lambda x, y: x + y).filter(
    lambda x: len(x[1]) >= thresholder).map(lambda x: x[0])

 # make a pair for all the bussines for each user

all_vertices = pair_user.flatMap(lambda x: [(x[0]), (x[1])]).distinct()
# get all users
number_vertices = len(all_vertices.collect())
# number of the users

all_edges = pair_user.map(lambda x: (x[0], x[1]))
# each pair that we made count as edge that has two users
number_all_edges = len(all_edges.collect())
# number of edges
list_edges = all_edges.collect()

adjacent_vertices = all_vertices.map(lambda x: (x, find_edges(x, list_edges))).collectAsMap()
# we make a dictionary that shows connected users to each user

betweenness_each_edge = all_vertices.flatMap(lambda x: get_betweenness(x, adjacent_vertices, number_vertices))

# find the betwenness of all the edges
betweenness_each_edge = betweenness_each_edge.reduceByKey(lambda x, y: (x + y)).map(lambda x: (x[0], float(x[1] / 2)))

betweenness_rdd = betweenness_each_edge.sortByKey(ascending=True).map(lambda x: (x[1], x[0])).sortByKey(ascending=False).map(lambda x: (x[1], x[0]))
# sort the betweenness
edge_with_highest_betwenness = betweenness_rdd.take(1)[0][0]
# take the edge that has highest betwenness

adjacency_matrix = adjacent_vertices.copy()
# we make a hard copy of the dictionary

connected_nodes = get_connected_nodes(adjacency_matrix)

# with this function we find all the users that connected to each user

modularity = check_modularity(adjacent_vertices, connected_nodes, number_all_edges)
# we calculate the modularity every time we remove the edge that has highest betweenness


adjacency_matrix = adjacent_vertices.copy()

highest_modularity = -1
# modularity is between -1 and 1
clusters = []
count = 0
while (1):
    adjacency_matrix = remove_edge(adjacency_matrix, edge_with_highest_betwenness)
    # remove the edge that has highest betweenness

    connected_nodes = get_connected_nodes(adjacency_matrix)
    # after removing we check all the users that connected to each user

    modularity = check_modularity(adjacent_vertices, connected_nodes, number_all_edges)
    # calculate the modularity

    adjacency_matrix = build_node_matrix(connected_nodes)

    temp = []
    for i in adjacency_matrix.keys():
        temp.append(i)
    temp = list(set(temp))
    v_rdd = sc.parallelize(temp)
    betweenness_temp = v_rdd.flatMap(lambda x: get_betweenness(x, adjacency_matrix, number_vertices)) \
        .reduceByKey(lambda x, y: (x + y)).map(lambda x: (x[0], float(x[1] / 2))).sortByKey().map(
        lambda x: (x[1], x[0])).sortByKey(ascending=False).map(lambda x: (x[1], x[0]))
    edge_with_highest_betwenness = betweenness_temp.take(1)[0][0]

    if (modularity >= highest_modularity):  # as long as the modularity that we calculate is better that the prevoius
        # we replace the modulaity with the one we calculate it
        highest_modularity = modularity
        clusters = connected_nodes

    count += 1
    if (count == 60):  # i repeat the whole process 60 time
        break

sorted_clusters = []
for i in clusters:
    item = sorted(i[0])
    sorted_clusters.append((item, len(item)))

sorted_clusters.sort()
sorted_clusters.sort(key=lambda x: x[1])

text = open(output_file_betweenness, "w")
if betweenness_rdd:
    for e in betweenness_rdd.collect():
        text.write("(")
        text.write("'")
        text.write(str(e[0][0]))
        text.write("'")
        text.write(", ")
        text.write("'")
        text.write(str(e[0][1]))
        text.write("'")
        text.write(")")
        text.write(", ")
        text.write(str(e[1]))
        text.write("\n")
text.close()

text = open(output_file_community, 'w')

for i in sorted_clusters:
    s = str(i[0]).replace("[", "").replace("]", "")
    text.write(s)
    text.write("\n")
text.close()
