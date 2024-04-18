from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.regression import LinearRegression

# Создаем трансформеры
tokenizer = Tokenizer(inputCol="reviewText", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
lr = LinearRegression(labelCol="overall", featuresCol="features", elasticNetParam=0.5,regParam=0.1)

# Создаем пайплайн
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])
