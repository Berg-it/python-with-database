#  Copyright (c) 2020. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.

import pandas as pd
import psycopg2
import numpy as np



def connect_to_db():
    db_hostname = "localhost"
    db_port = "5432"
    db_username = "postgres"
    db_name = "python-test"
    db_password = "postgresql"

    try:
        print('Connecte to database')
        connection = psycopg2.connect(host=db_hostname, dbname=db_name, user=db_username, password=db_password, port=db_port)
        print('connection.status', connection.status)
        print('connection.server_version', connection.server_version)

        return connection

    except Exception as e:
        print('Cannot connect to the PostgreSQL database !')
        print(e)
        raise e

def read_csv_file():
    data_frame = pd.read_csv("../test_amine.csv", encoding='utf8', sep=";")
    final_data_frame = data_frame.replace(np.nan, '', regex=True)
    return final_data_frame

def sample_insert_query(df):
    # create (col1,col2,...)
    columns = ",".join(df)
    # create VALUES('%s', '%s",...) one '%s' per column
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
    # INSERT INTO table_name (Fund_ID,Asset_Manager,Contact) VALUES(%s,%s,%s)
    query = "INSERT INTO {} ({}) {}".format('example_table', columns, values)
    return query

try:
    connection = connect_to_db()
    cursor = connection.cursor()
    df = read_csv_file()

    # Get column names
    df_columns = list(df.columns)
    query = sample_insert_query(df)
    first_5_line = df.head(10)
    index = 0;
    for row in first_5_line.values:
        cursor.execute(query, row)
        if index % 5 == 0:
            connection.commit()
            index = 0
        index = index + 1
    if index != 0:
        connection.commit()



except Exception as e:
    print("Exception")
    print(e)
finally:
    connection.close()
    print("---")


