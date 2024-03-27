from pyspark.sql import SparkSession

class SparkSessionBuilder:
    @staticmethod
    def build(app_name="SparkSessionBuilder", spark_config=None):
        if spark_config is not None:
            return SparkSession.builder.master('local') \
                .config(spark_config[0], spark_config[1]) \
                .appName(app_name) \
                .getOrCreate()
        else:
            return SparkSession.builder \
                .appName(app_name) \
                .getOrCreate()

