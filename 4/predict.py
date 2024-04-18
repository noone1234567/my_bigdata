import sys
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel


if len(sys.argv) != 4:
    print("Usage: predict.py <model_path> <input_path> <output_path>", file=sys.stderr)
    sys.exit(-1)

model_path = sys.argv[1]
input_path = sys.argv[2]
output_path = sys.argv[3]

# Инициализация SparkSession
import os
import sys

conf = SparkConf()
conf.set("spark.ui.port", "14099")

spark = SparkSession.builder.config(conf=conf).appName("Predict").getOrCreate()


# Загрузка модели
model = PipelineModel.load(model_path)

# Загрузка тестовых данных
data = spark.read.json(input_path)

# Предсказание
predictions = model.transform(data)

# Сохранение предсказаний
predictions.select("id", "prediction").write.mode("overwrite").json(output_path)

spark.stop()
