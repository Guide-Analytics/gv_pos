from punc_removal import PunctuationRemoval
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import split, udf, regexp_replace
from pyspark import SparkContext, SparkConf
from string_analysis import build_graph_rdd, print_graph, noun_adj_rule_table, verb_rule_table, graph_table
import psutil
import random

sc = SparkContext(appName="Google Weight Analysis 1", master="local[*]", conf=SparkConf().set('spark.ui.port', random.randrange(4000, 5000)))
#sqlContext = SQLContext(sc)

spark = SparkSession.builder.appName("Google Review Analysis 2").master("local[*]")\
                            .config('spark.ui.port', random.randrange(4000, 5000)).getOrCreate()

path_graph_output = 'graph_output/graph.txt'


def read_csv(spark, pathR = 'GOOGLE_REVIEWS.csv'):

    #
    # :param spark:
    # :param pathR:
    # :return:
    #

    rev_data_raw = spark.read.csv(pathR, mode="DROPMALFORMED", header='true', sep=',', inferSchema=True,
                                  multiLine=True) ## adding argument quote = '"' and escape = '"' may
    ## be necessary ... for now, not in this case

    rev_data_raw = rev_data_raw.withColumn('Review Text', regexp_replace('Review Text', '"', ''))
    rev_data = rev_data_raw.toDF("GOOGLE_REVIEWS ID", "Business Rating", "Business Reviews", "Source URL",
                                 "Business Name", "Author Name", "Local Guide", "Review Text", "Review Rating",
                                 "Review Date", "Author URL")

    rev_data.createOrReplaceTempView("rev_data")
    return rev_data


def filter_data(spark):

    rev_data = read_csv(spark)
    data = PunctuationRemoval()

    udf_punc = udf(data.remove_nuke, StringType())
    new_data = rev_data.withColumn('crtext', udf_punc('Review Text'))
    return new_data


def data_initiate():

    new_data = filter_data(spark)
    new_data = new_data.select('crtext')

    return new_data.rdd.map(lambda x: x.crtext)


def write_data():

    cleaned_graph = data_initiate().collect()
    build_graph_rdd(cleaned_graph, "canadian tire", noun_adj_rule_table, graph_table)
    build_graph_rdd(cleaned_graph, "canadian tire", verb_rule_table, graph_table)
    print_graph(path_graph_output, graph_table)

