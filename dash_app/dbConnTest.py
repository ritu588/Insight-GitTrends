# test DB connectivity
import os
import pandas as pds
import psycopg2

postgres_host = os.environ['POSTGRES_HOST']
postgres_username = os.environ['POSTGRES_USERNAME']
postgres_password = os.environ['POSTGRES_PASSWORD']

if __name__ == "__main__":
    #test db connectivity
    try:
        connection = psycopg2.connect(host = postgres_host,
                                        port = '5432',
                                        user = postgres_username,
                                        password = postgres_password,
                                        dbname = 'insightproject')
        cursor = connection.cursor()
        print('postgres db connect sucess')
    except:
        print('postgres db connect error, please recheck security group and POSTGRES login credentials')

    #test query result
    try:
        select_table = 'pushevent_table'
        cursor.execute("SELECT * FROM {} repo_name WHERE count > 6000".format(select_table))
        data = cursor.fetchall()
        print(data)
    except:
        print("DB error")
