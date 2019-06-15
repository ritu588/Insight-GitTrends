#version on Spark Master that is working
import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pgConnector

def initSession():
    ss = SparkSession.builder.appName("TestSpark").getOrCreate()
    return ss

def initDbConnection():
    conn = pgConnector.PostgresConnector()
    return conn


def processPushEvent(df):
    try:
        df = df[df.type.isin('PushEvent')]
    except:
        print('extracting rows with PushEvent Failed')
    try:
        df = df.withColumn('event', df['type']).withColumn('create_time', df['created_at']).withColumn('repo_name', df['repo']['name']).withColumn('user_name', df['actor']['login'])
    except:
        print('extracting Pushevent columns Failed')
    try:
        df = df.withColumn('event', df['type']).withColumn('create_time', df['created_at']).withColumn('repo_name', df['repo']['name']).withColumn('user_name', df['actor']['login'])
    except:
        print('extracting Pushevent columns Failed')
    try:
        df = df.select('event', 'create_time', 'repo_name', 'user_name')
    except:
        print('selecting PushEvent cols Failed')
    print("$$$$$$$$$$$$$$$$$$$", df.head())
    return df

def processWatchEvent(df):
    try:
        df = df[df.type.isin('WatchEvent')]
    except:
        print('extracting rows with WatchEvent Failed')
    try:
        df=df.withColumn('event', df['type']).withColumn('create_time', df['created_at']).withColumn('repo_name', df['repo']['name'])
    except:
        print('extracting WatchEvent columns Failed')
    try:
        df = df.select('event', 'create_time', 'repo_name')
    except:
        print('selecting WatchEvent cols Failed')
    print("*************", df.head())
    return df

def processStarEvent(df):
    try:
        df = df[df.type.isin('StarEvent')]
    except:
        print('extracting rows with StarEvent Failed')
    try:
        df=df.withColumn('event', df['type']).withColumn('create_time', df['created_at']).withColumn('repo_name', df['repo']['name'])
    except:
        print('extracting StarEvent columns Failed')
    try:
        df = df.select('event', 'create_time', 'repo_name')
    except:
        print('selecting StarEvent cols Failed')
    print("&&&&&&&&", df.head())
    return df

def writeUsersToPostgres(df, table, mode):
    conn.write(df, table, mode)

if __name__=="__main__":
    ss = initSession()
    conn = initDbConnection()
    hour = 2
    df = ss.read.json("s3a://ritu-insight-project/2019-05-01-{}.json".format(hour))
    #df = ss.read.json("/Users/ritu/Downloads/2019-01-15-15.json")
    df_push = processPushEvent(df)
    df_users = df_push.drop('event')
    #writeUsersToPostgres(df_users, "repo_users", "overwrite")
    writeUsersToPostgres(df_users, "repo_users", "append")

    df_push = df_push.drop('user_name')
    df_watch = processWatchEvent(df)
    df_star = processStarEvent(df)
    writeUsersToPostgres(df_push, "repo_events", "append")
    writeUsersToPostgres(df_watch, "repo_events", "append")
    writeUsersToPostgres(df_star, "repo_events", "append")
    ss.stop()
