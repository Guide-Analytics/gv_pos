

from graph_extract import GraphExtract
from personalized_rank import PersonalizedRank
from dataCSV import write_data, spark, sc, path_graph_output
from file_func import *


def initiate(source_node, iterations = 10, jump_fac = 0.5):

    """

    :param source_node:
    :param iterations:
    :param jump_fac:
    :return:
    """

    # Initiate Graph Extract (Testing purposes)

    result_pr = []
    try:
        ge = GraphExtract()
        graph_data = sc.textFile(path_graph_output)
        graph_data = ge.lower_case(graph_data).cache()

        # Initiate Personalized Rank (compute personalized rank system)
        pr = PersonalizedRank(graph_data)
        result_pr = pr.personalized_page_rank(source_node, iterations, jump_fac)
        result_pr = result_pr.groupByKey().map(lambda x: (x[0], sum(x[1]))).sortBy(lambda x: x[1], False)
        return result_pr.take(20)
    except:
        print('Keyword not found! Passed!')
        pass

    return


def keyword_pair(rdd):

    """

    :param rdd:
    :return:
    """

    lst_rdd = sc.parallelize(rdd)
    lst_rdd = lst_rdd.filter(lambda node: node != None)
    lst_rdd = lst_rdd.map(lambda tup: [tup[0], tup[1:]])

    lst_rdd = lst_rdd.map(lambda nodes: [(nodes[0][0], i[0]) for i in nodes[1] if i[1] != 0.0])
    lst_rdd_new = lst_rdd.flatMap(lambda keys: keys)

    lst_rdd_new = lst_rdd_new.filter(lambda node: len(node[1]) >= 3)

    df = spark.createDataFrame(lst_rdd_new, ['Keywords', 'Pairs'])
    df = df.select('Keywords', 'Pairs').coalesce(1)
    df.write.format('csv').mode('overwrite').option("header", "true").save("output")
    # schema = StructType([StructField('Keywords', StringType(), True), StructField("Pairs", StringType(), False)])
    # df = sqlContext.createDataFrame(rdd, schema)


def main():

    keyword_pair_list = []
    try:
        write_data()
        check_file()
    except FileNotFoundError:
        exit(2)

    keywords = read_file()
    for word in keywords:
        result = initiate(source_node=word)
        keyword_pair_list.append(result)

    keyword_rdd = sc.parallelize(keyword_pair_list)
    keyword_pair(keyword_pair_list)

    sc.stop()
    spark.stop()


if __name__ == '__main__':
    main()