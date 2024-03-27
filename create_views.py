import psycopg2
from sqlalchemy import create_engine, text
from dotenv import dotenv_values
from ingestion.utils.utils import assert_dataframe_equal, read_sql_file

constants = dotenv_values("ingestion/.env")
url = constants.get("URL")
pg_user = constants.get("POSTGRES_USER")
pg_pw = constants.get("POSTGRES_PASSWORD")


# Replace with your actual database connection details
db_url = f"postgresql://{pg_user}:{pg_pw}@35.184.59.44:5432/postgres"

engine = create_engine(db_url)

view_query = """SELECT "MARCA", "LINHA", SUM("QTD_VENDA") AS "QTD_VENDA" FROM base_vendas GROUP BY "MARCA","LINHA" ORDER BY "MARCA", "LINHA"
"""


with engine.connect() as conn:
    conn.execute(text(view_query))
    conn.commit()