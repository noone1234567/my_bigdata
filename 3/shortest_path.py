from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys

conf = SparkConf()
conf.set("spark.ui.port", "4322")

spark = SparkSession.builder.config(conf=conf).appName("Pagerank").getOrCreate()

start_node = int(sys.argv[1]) #12
end_node = int(sys.argv[2]) #34
data_path = sys.argv[3] #'/datasets/twitter/twitter_sample_small.tsv'
ans_path = sys.argv[4] #'hw3_output'

ddl_schema = """
    user_Id INT,
    follower_Id INT
"""
graph_data = spark.read\
          .schema(ddl_schema)\
          .format("csv")\
          .option("sep", "\t")\
          .load(data_path)


from pyspark.sql.functions import col
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import explode


max_path_length = 100

# Создаем DataFrame с начальным узлом
current_paths = graph_data.filter(graph_data["follower_Id"] == start_node).withColumnRenamed("follower_Id", "follower0_Id").withColumnRenamed("user_Id", 'user0_Id')
current_paths = current_paths.select("follower0_Id", "user0_Id")
current_paths.show(5)
# Повторяем итерации до достижения максимальной длины пути
i=0
while (i <= max_path_length) and (current_paths.filter(col(f"user{i}_Id") == end_node).count() == 0):
    # Обновляем текущие пути, добавляя следующие вершины
    # Обновляем текущие пути, добавляя следующие вершины
    current_paths.select(f"user{i}_Id").show(5)
   # graph_data = graph_data.withColumnRenamed("follower_Id",f"user{i}_Id")
    current_paths = current_paths.join(graph_data, current_paths[f"user{i}_Id"] == graph_data["follower_Id"], how="inner") \
        .withColumnRenamed("user_Id", f"user{i+1}_Id").drop("follower_Id")
    #last = current_paths[f"user{i+1}_Id"]
    #current_paths = current_paths.drop(f"user{i+1}_Id").withColumn(f"user{i+1}_Id", last)
    current_paths.show(5)
    #graph_data = graph_data.filter(~graph_data["follower_Id"].isin(current_paths[f"follower{i+1}_Id"])).select("user_Id", "follower_Id")
    i += 1
# Собираем путь от конечного узла к начальному
shortest_path = current_paths.filter(col(f"user{i}_Id") == end_node)

shortest_path.write.mode('overwrite').option("sep", ",").option("header", "false").csv(ans_path)
