from pyspark.sql import DataFrameWriter
import os

class PostgresConnector(object):
    def __init__(self):
        self.database_name = 'DATABASE_NAME'
        self.hostname = 'HOST_NAME'
        self.url_connect = "jdbc:postgresql://{hostname}:5432/{db}".format(hostname=self.hostname, db=self.database_name)
        self.properties = {"user":"POSTGRES_USER",
                      "password" : "POSTGER_PASS",
                      "driver": "org.postgresql.Driver"
                     }
    def get_writer(self, df):
        return DataFrameWriter(df)

    def write(self, df, table, md):
        my_writer = self.get_writer(df)
        df.write.jdbc(url=self.url_connect,table= table,mode=md,properties=self.properties)
