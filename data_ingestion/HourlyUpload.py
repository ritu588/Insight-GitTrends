#This file is for managing the cleaning of hourly data
import sys
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql import functions as func
from pyspark.sql.functions import lit, to_date, col, when
import pgConnector
from datetime import datetime

user_schema_list = [
            ('repo_name', 'STRING'),
            ('user_name', 'STRING'),
            ('count', 'INT'),
        ]
user_schema = ", ".join(["{} {}".format(col, type) for col, type in user_schema_list])
event_schema_list = [
            ('events', 'STRING'),
            ('repo_name', 'STRING'),
            ('count', 'INT'),
        ]
event_schema = ", ".join(["{} {}".format(col, type) for col, type in event_schema_list])

input_path = 's3a://bucket/'

def initSession():
    ss = SparkSession.builder.appName("HourlyUpload")\
    .config("spark.sql.codegen.wholeStage", "true").getOrCreate()
    return ss

def initDbConnection():
    conn = pgConnector.PostgresConnector()
    return conn

def getHour(filename):
    return int(fileName.split('.')[-2].split('-')[-1])

# At the end of the day all cleaned hourly files will be read and data will be
# grouped together
def processDayEnd(df, type, fileName, numCount):
        filestoread=fileName.split('.')[-2][:-3]
        df_read = ss.read.csv(type + filestoread + "*.csv", header=True, schema=event_schema)
        df_read = df_read.withColumn('count', df_read["count"].cast(IntegerType()))
        df_combine = df.union(df_read)
        df_combine = df_combine.groupBy("event", "repo_name").agg(func.sum("count"))
        df_combine = df_combine.withColumnRenamed("sum(count)", "count")
        df_combine = df_combine.withColumn('create_date', lit(filestoread))
        df_combine = df_combine.withColumn('create_date', df_combine["create_date"].cast(DateType()))
        df_combine = df_combine.sort("count", ascending=False).limit(50)
        writeUsersToPostgres(df_combine, type+"_table", "append")
        return df_combine

# extract the repo info from pull request event
def processPullRequestEvent(df, fileName):
    try:
        df = df[df.type.isin('PullRequestEvent')]
    except:
        print('extracting rows with PullRequestEvent Failed')
    try:
        df = df.withColumn('repo_name', df['repo']['name']).withColumn('repo', df['payload']['pull_request']['head']['repo'])
    except:
        print('extracting repo_name, payload PullRequestEvent columns Failed')
    try:
        df = df.select('repo_name', 'repo')
        df = df.na.drop()
    except:
        print('selecting PullRequestEvent cols Failed for repo_name, repo')
    try:
        df = df.withColumn('language', df['repo']['language']).withColumn('open_issues_count', df['repo']['open_issues_count'])
    except:
        print("error extracting language, open_issues_count")
    try:
        df = df.withColumn('forks', df['repo']['forks']).withColumn('has_wiki', df['repo']['has_wiki'])
    except:
        print("error extracting forks, has_wiki")
    try:
        df = df.withColumn('stars_count', df['repo']['stargazers_count']).withColumn('watchers', df['repo']['watchers_count'])
    except:
        print('extracting stars, watchers Failed')
    df.na.drop()
    try:
        df = df.select('repo_name', 'language', 'open_issues_count', 'has_wiki', 'forks', 'stars_count', 'watchers')
    except:
        print('selecting PullRequestEvent cols Failed')
    df = df.drop('repo')

    #df = df.where(col("language").isNotNull())

    df_agg_langs = df.select('repo_name', 'language').dropDuplicates().groupby("repo_name").agg(func.collect_list("language").alias("languages"))
    df_agg = df.join(df_agg_langs, on='repo_name', how='inner').drop('language')
    filestr = fileName.split('.')[-2]
    try:
        df_agg.write.json("PullReqEvent" + filestr + ".json")
    except:
        print("file PullReqEvent" + filestr + ".json" + "already exists")
    str = getHour(fileName)
    if str == 23:
        filestoread=fileName.split('.')[-2][:-3]
        df_read = ss.read.json("PullReqEvent" + filestoread + "*.json")
        return df_read



# extract the user information from push event
def processPushEventUsers(df, fileName):
    try:
        df = df[df.type.isin('PushEvent')]
    except:
        print('extracting rows with PushEvent Failed')
    try:
        df = df.withColumn('repo_name', df['repo']['name']).withColumn('user_name', df['actor']['login'])
    except:
        print('extracting Pushevent columns Failed')
    try:
        df = df.select('repo_name', 'user_name')
    except:
        print('selecting PushEvent cols Failed')

    df = df.groupBy('repo_name', 'user_name').count()

    df = df.filter(df['count']>10)
    str = getHour(fileName)
    if str != 23:
        filestr = fileName.split('.')[-2]
        try:
            df.write.csv("PushEventUser" + filestr + ".csv")
        except:
            print("file PushEventUser" + filestr + ".csv" + "already exists")

    else:
        filestoread=fileName.split('.')[-2][:-3]
        df_read = ss.read.csv("PushEventUser" + filestoread + "*.csv", header=True, schema=user_schema)
        df_read = df_read.withColumn('count', df_read["count"].cast(IntegerType()))
        df_combine = df.union(df_read)
        df_combine = df_combine.groupBy("repo_name", "user_name").agg(func.sum("count"))
        df_combine = df_combine.withColumnRenamed("sum(count)", "count")
        df_combine = df_combine.withColumn('create_date', lit(filestoread))
        df_combine = df_combine.withColumn('create_date', df_combine["create_date"].cast(DateType()))
        df_combine=df_combine.sort("count", ascending=False).limit(50)
        writeUsersToPostgres(df_combine, "repo_users", "append")



# extract any event to find out how many events of that kind occured in an hour on each repo_name
#       df - file loaded in DataFrame
#       type - Event type to extracting
#       filename - file that was loaded in df
#       numCount - Count of minimum number of events that should be saved after processing and grouping
def processGenericEvent(df, type, fileName, numCount):
    try:
        df = df[df.type.isin(type)]
    except:
        print('extracting rows with ', type, ' Failed')
    try:
        df=df.withColumn('event', df['type']).withColumn('create_time', df['created_at']).withColumn('repo_name', df['repo']['name'])
    except:
        print('extracting ', type , ' columns Failed')
    try:
        df = df.select('event', 'create_time', 'repo_name')
    except:
        print('selecting ',  type, ' cols Failed')
    df = df.groupBy('event', 'repo_name').count()
    df = df.filter(df['count'] > numCount)
    str = fileName.split('.')[-2].split('-')[-1]
    if int(str) != 23:
        filestr = fileName.split('.')[-2]
        try:
            df.write.csv(type + filestr + ".csv")
        except:
            print("file " + type + filestr + ".csv" + "already exists")
    return df

# Calculate the aggregate score for three types of events, save it
# to database, find repository information for repositories in This
# table and store that in the repo_details table
#  df1, df2, df3 - three event dataframes
#  df4 - repo detals DataFrame
#  fileName - fileName to extract the date information
def processAggregateScore(df1, df2, df3, df4, fileName):
    df1_max = df1.agg({"count": "max"}).collect()[0][0]
    df1 = df1.withColumn("count", col("count")/df1_max)
    df2_max = df2.agg({"count": "max"}).collect()[0][0]
    df2 = df2.withColumn("count", col("count")/df2_max)
    df3_max = df3.agg({"count": "max"}).collect()[0][0]
    df3 = df3.withColumn("count", col("count")/df3_max)
    df_comb = df1.union(df2).union(df3)
    df_result = df_comb.groupBy('repo_name').agg(func.sum("count")/3)
    df_result = df_result.withColumnRenamed('(sum(count) / 3)', 'score')
    date_stored=fileName.split('.')[-2][:-3]
    df_result = df_result.withColumn('create_date', lit(date_stored))
    df_result = df_result.withColumn('create_date', df_result["create_date"].cast(DateType()))
    writeUsersToPostgres(df_result, "AggregateEvent_table", "append")
    df_repo_results = df_result.join(df4, on='repo_name', how='inner').drop('count')
    writeUsersToPostgres(df_repo_results, "repo_details", "append")

def writeUsersToPostgres(df, table, mode):
    conn.write(df, table, mode)


if __name__=="__main__":
    ss = initSession()
    conn = initDbConnection()
    cur_time = datetime.now()
    year = cur_time.year
    month = cur_time.month
    day = cur_time.day
    hour = cur_time.hour-1
    if hour == -1:
        hour = 23
    #make url for current file to upload
    input_path = 's3a://bucket/'


    filepath = '{}-{}-{}/'.format(year, '{:02}'.format(month), '{:02}'.format(day))
    fileName = '{}-{}-{}-{}.json'.format(year, '{:02}'.format(month), '{:02}'.format(day), hour)
    print(input_path+filepath+fileName)
    df = ss.read.json(input_path + filepath + fileName)

    processPushEventUsers(df, fileName)
    df_push = processGenericEvent(df, "PushEvent", fileName, 10)
    df_watch = processGenericEvent(df, "WatchEvent", fileName, 5)
    df_fork = processGenericEvent(df, "ForkEvent", fileName, 4)
    df_pull = processPullRequestEvent(df, fileName)
    #end of the day then aggregate the data and store in database
    if getHour(fileName) == 23:
        df_push_comb = processDayEnd(df_push, "PushEvent", fileName, 10)
        df_watch_comb = processDayEnd(df_watch, "WatchEvent", fileName, 5)
        df_fork_comb = processDayEnd(df_fork, "ForkEvent", fileName, 4)
        processAggregateScore(df_push_comb, df_watch_comb, df_fork_comb, df_pull, fileName)
        os.system("rm -rf PushEvent*.csv")
        os.system("rm -rf WatchEvent*.csv")
        os.system("rm -rf ForkEvent*.csv")
        os.system("rm -rf PushEventUser*.csv")
        os.system("rm -rf PullReqEvent*.json")
    ss.stop()
