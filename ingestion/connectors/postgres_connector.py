from ingestion.utils.utils import assert_dataframe_equal

class PostgresConnector:
    def __init__(self, spark_session,url, user, password):
        self.spark_session = spark_session
        self.url = url
        self.user = user
        self.password = password


    def extract(self,table_name):
        """
        Writes a DataFrame to a PostgreSQL database using JDBC.

        Args:
            df (pyspark.sql.DataFrame): The DataFrame to be written.
            table_name (str): The name of the target table in the database.
        """
        df = self.spark_session.read.format("jdbc") \
            .option("url", self.url) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", table_name) \
            .option("user", self.user) \
            .option("password", self.password) \
            .load()
        return df

    def load(self, df, table_name):
        """
        Writes a DataFrame to a PostgreSQL database using JDBC.

        Args:
            df (pyspark.sql.DataFrame): The DataFrame to be written.
            table_name (str): The name of the target table in the database.
        """
        df.write.format("jdbc") \
            .option("url", self.url) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", table_name) \
            .option("user", self.user) \
            .option("password", self.password) \
            .save()


 