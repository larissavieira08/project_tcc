from pandas.io import sql as psql


def insert_values_dimension(dataframe, engine, table_name, index, if_exists):
    try:
        dataframe.to_sql(table_name,
                         con=engine,
                         index=index,
                         if_exists=if_exists)
        return True

    except Exception as error:
        print(error)
        return False


def update_values_dimension(dataframe, engine, table_name, index, if_exists):
    try:
        dataframe.to_sql(table_name,
                         con=engine,
                         index=index,
                         if_exists=if_exists)
        return True

    except Exception as error:
        print(error)
        return False


def read_table(create_table_query, conn):
    return psql.read_sql(create_table_query, conn)


def create_table(create_table_query, conn):
    cursor = conn.cursor()
    cursor.execute(create_table_query)
    conn.commit()


def execute_query(create_query, conn):
    cursor = conn.cursor()
    cursor.execute(create_query)
    conn.commit()
