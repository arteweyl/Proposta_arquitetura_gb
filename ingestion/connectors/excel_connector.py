import pandas as pd

class ExcelLocalConnector:
    """Excel Connector to Spark, as we don't deal directly with xlsx files in spark, we using pandas as bridge"""
    def __init__(self,spark_session):
        self.spark_session = spark_session

    def extract(self,file_path):
        
        pandas_df = pd.read_excel(file_path)
        
        # Convert the pandas DataFrame to a Spark DataFrame
        spark_df = self.spark_session.createDataFrame(pandas_df)
        
        return spark_df

    def load(self, df, output_file_path):
        # Convert Spark DataFrame to pandas DataFrame
        pandas_df = df.toPandas()

        # Write pandas DataFrame to Excel file
        pandas_df.to_excel(output_file_path, index=False)

    def close(self):
        self.spark.stop()