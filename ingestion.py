from ingestion.create_session import SparkSessionBuilder
from ingestion.connectors.excel_connector import ExcelLocalConnector
from ingestion.connectors.bucket_connector import BucketConnector
from ingestion.utils.utils import assert_dataframe_equal, read_sql_file
from ingestion.connectors.postgres_connector import PostgresConnector
from dotenv import load_dotenv
import os

load_dotenv("ingestion/.env")
url = os.getenv("URL")
pg_user = os.getenv("POSTGRES_USER")
pg_pw = os.getenv("POSTGRES_PASSWORD")

spark_config = ("spark.jars.packages", "org.postgresql:postgresql:42.5.4")
spark = SparkSessionBuilder.build("test_case_2", spark_config=spark_config)
postgres_conn = PostgresConnector(spark, url, pg_user, pg_pw)
bucket_seed_conn = BucketConnector("gbtech_test_case2_seeds", spark)

bucket_seed_conn.load_from_local(
    "/home/arteweyl/trabalho/testes/Boticario/Case2_GBTech/seeds"
)

df_base_2017 = bucket_seed_conn.read_excel_from_bucket("Base_2017.xlsx")
df_base_2019 = bucket_seed_conn.read_excel_from_bucket("Base_2019.xlsx")
df_base_2018 = bucket_seed_conn.read_excel_from_bucket("Base_2018.xlsx")

#assert if the dataframes are equal. this step i made because was sent to me two wrong files 
assert_dataframe_equal(df_base_2017,df_base_2018,'base_2017','base_2018')
assert_dataframe_equal(df_base_2017,df_base_2019,'base_2017','base_2019')
assert_dataframe_equal(df_base_2018,df_base_2019,'base_2017','base_2018')

df_consolidated = df_base_2017.union(df_base_2018)

postgres_conn.load(df_base_2018,'base_2017')
postgres_conn.load(df_base_2019,'base_2019')
postgres_conn.load(df_consolidated,'base_vendas')

#creating the first four tables requested
 
df = postgres_conn.extract('base_vendas')
df.createOrReplaceTempView("vendas")

query_sales_by_year_and_month = read_sql_file('SQL/sales_by_year_and_month.sql')
query_sales_by_brand_and_line = read_sql_file('SQL/sales_by_brand_and_line.sql')
query_sales_by_brand_and_month = read_sql_file('SQL/sales_by_brand_and_month.sql')
query_sales_by_brand_and_month = read_sql_file('SQL/sales_by_brand_and_month.sql')

# Execute the SQL statement
consolidated_df_by_year_and_month = spark.sql(query_sales_by_year_and_month)
consolidated_df_by_brand_and_line = spark.sql(query_sales_by_brand_and_line)
consolidated_df_by_brand_and_month = spark.sql(query_sales_by_brand_and_month)
consolidated_df_by_line_and_month = spark.sql(query_sales_by_brand_and_month)

#create the views
consolidated_df_by_year_and_month.createOrReplaceTempView("consolidado_mes_ano")

#Load new tables to postgres
postgres_conn.load(consolidated_df_by_year_and_month,'consolidado_mes_ano')
postgres_conn.load(consolidated_df_by_brand_and_line,'consolidado_marca_linha')
postgres_conn.load(consolidated_df_by_brand_and_month,'consolidado_marca_mes')
postgres_conn.load(consolidated_df_by_line_and_month,'consolidado_linha_mes')

# Show the result

consolidated_df_by_year_and_month.show()
consolidated_df_by_brand_and_line.show()
consolidated_df_by_brand_and_month.show()
consolidated_df_by_line_and_month.show()


# postgres_conn.load(consolidated_df,'vendas_por_mes')




