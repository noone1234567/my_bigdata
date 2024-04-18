import sys
from pyspark.sql import SparkSession

import os
import sys

from pyspark import SparkConf

conf = SparkConf()
conf.set("spark.ui.port", "14099")

if len(sys.argv) != 3:
    print("Usage: train.py <input_path> <model_path>", file=sys.stderr)
    sys.exit(-1)

input_path = sys.argv[1]
model_path = sys.argv[2]

# Инициализация SparkSession
spark = SparkSession.builder.config(conf=conf).appName("Spark next part").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
from model import pipeline
# Загрузка данных
data = spark.read.json(input_path).limit(5000)
data = data.na.fill('0', subset=['vote'])
data = data.na.fill('empty', subset=['reviewText'])
data = data.na.fill('empty', subset=['summary'])
    
# Обучение модели
model = pipeline.fit(data)

# Сохранение модели
model.write().overwrite().save(model_path)

spark.stop()
