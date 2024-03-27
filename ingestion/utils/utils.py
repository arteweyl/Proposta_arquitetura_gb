from dotenv import dotenv_values


def pick_postgres_info(dotenv_path='ingestion/.env'):
    constants = dotenv_values(dotenv_path)
    url = constants.get('URL')
    pg_user = constants.get('POSTGRES_USER')
    pg_pw = constants.get('POSTGRES_PASSWORD')
    return url,pg_user,pg_pw


def assert_dataframe_equal(df1, df2, df1_name='df1_name', df2_name='df2_name'):
    equal_dataframes = df1.exceptAll(df2).count() == 0

    if equal_dataframes:
        print(f"{df1_name} and {df2_name} are equals")
    else:
        print(f"{df1_name} and {df2_name} are not equals")
    return equal_dataframes

class ReadSeedFilesError(Exception):
    code='002'


def read_sql_file(filepath):
    try:
        with open(filepath, "r") as sql:
            sql_file = sql.read()
        return sql_file
    except ReadSQLFileError as err:
        raise err
