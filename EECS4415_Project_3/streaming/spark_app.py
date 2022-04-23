import json
import sys
import requests
from pyspark import SparkContext
from pyspark.sql.functions import current_timestamp, to_timestamp
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession


def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SparkSession(spark_context)
    return globals()['sqlContextSingletonInstance']


def send_df_to_dashboard(df):
    url = 'http://webapp:5000/updateData'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)


def process_rdd(time, rdd):
    pass
    print("-----------Language Count-----------------------")
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(
            lambda w: Row(language_name=w[0][0], full_name=w[0][1], count=w[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql(
            "SELECT language_name, COUNT(*) AS language_count FROM results GROUP BY language_name ORDER BY 2 ")
        new_results_df.show()
        send_df_to_dashboard(new_results_df)
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


## avg star
def process_rdd2(time, rdd):
    pass
    print("-----------AVG STAR-----------------------")
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(
            lambda w: Row(language_name=w[0][0], full_name=w[0][1], starCount=w[0][3]))
        # full_name = w[0][1],
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results_2")
        new_results_df = sql_context.sql(
            "SELECT language_name, ROUND(AVG(starCount),2) AS avgCount FROM results_2 GROUP BY language_name ORDER BY 2 ")
        new_results_df.show()
        send_df_to_dashboard(new_results_df)
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


## top 10 word
def process_rdd3(time, rdd):
    pass
    print("-----------AVG STAR-----------------------")
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(
            lambda w: Row(language_name=w[0][0], full_name=w[0][1], starCount=w[0][3]))
        # full_name = w[0][1],
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results_2")
        new_results_df = sql_context.sql(
            "SELECT language_name, AVG(starCount) AS avgCount FROM results_2 GROUP BY language_name ORDER BY 2 ")
        new_results_df.show()
        send_df_to_dashboard(new_results_df)
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


#collected repositories with changes pushed during the last 60 seconds
def process_rdd4(time, rdd):
    pass
    print("-----------changes pushed during the last 60 seconds-------------")
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(
            lambda w: Row(language_name=w[0][0], full_name=w[0][1],pushed_at=w[0][2], starCount=w[0][3], count=w[1]))
        # full_name = w[0][1],
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql(
            "SELECT language_name, full_name, pushed_at, starCount, count FROM results ORDER BY count ")
        a_new_one = new_results_df.withColumn("timestamp", current_timestamp().cast("long") - to_timestamp("pushed_at").cast("long"))
        a_new_one.show()
        send_df_to_dashboard(new_results_df)
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)



if __name__ == "__main__":
    DATA_SOURCE_IP = "data-source"
    DATA_SOURCE_PORT = 9999
    sc = SparkContext(appName="Project -3 Github Analysis")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 60)
    ssc.checkpoint("checkpoint_project3")
    data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT)

    words = data.map(lambda x: json.loads(x))
    values = words.map(
        lambda s: ((s['language'], s['full_name'], s["pushed_at"], s["stargazers_count"], s["description"]), 1))
    lang_count = values.reduceByKey(lambda a, b: a)
    # python_repos_desc = values.filter(lambda x: x[0][1] == "Python")
    # python_repos_desc = python_repos_desc.map(lambda y:((y[0][1], y[0][0], y[0][4]),1))
    # different_py_desc = python_repos_desc.updateStateByKey(aggregate_count)
    # different_py_desc = different_py_desc.filter(lambda desc: desc.strip() != 'None')
    # different_py_desc = different_py_desc.filter(lambda desc: desc.strip() != None)
    # different_py_desc = different_py_desc.filter(lambda desc: desc.strip() != "")
    # flat_python_description = different_py_desc.flatMap(lambda desc: desc.strip().split(" "))
    # flat_python_description = flat_python_description.filter(lambda s: len(s.strip()) > 0)
    lang_count.pprint()
    aggregated_counts = lang_count.updateStateByKey(aggregate_count)
    aggregated_counts.foreachRDD(process_rdd)
    aggregated_counts.foreachRDD(process_rdd2)
    #aggregated_counts.foreachRDD(process_rdd3)
    #flat_python_description.foreachRDD(process_rdd4)

    ssc.start()
    ssc.awaitTermination()
