import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("3")\
        .master("yarn")\
        .config("spark.executor.memory", "4g") \
        .config("'spark.executor.cores", "4")\
        .config("'spark.driver.cores", "4")\
        .config("spark.ui.port", "4051")\
        .getOrCreate()

#проверка

# events = spark.read.json(path = "/user/master/data/events/date=2022-05-25")

# events.show(10)