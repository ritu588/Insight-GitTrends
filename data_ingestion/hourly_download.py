# This file is for the hourly download of file from github
# Will be used by airflow
import wget
import boto3
from argparse import ArgumentParser
from botocore.exceptions import NoCredentialsError
import gzip
import os
from datetime import datetime

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3')

    try:
        print("uploading to s3 " + s3_file)
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except IOError:
        print("The file was not found")
        return True


cur_time = datetime.now()
year = cur_time.year
month = cur_time.month
day = cur_time.day
hour = cur_time.hour-1
if hour == -1 :
    hour = 23
#make url for current file to download
file_name = "https://data.gharchive.org/{}-{}-{}-{}.json.gz".format(year,'{:02}'.format(month),'{:02}'.format(day),hour)
print("file to download  ..." + file_name)
output_dir = '/home/ubuntu/{}-{}-{}'.format(year, '{:02}'.format(month), '{:02}'.format(day), hour)
upload_filename = '{}-{}-{}-{}.json.gz'.format(year, '{:02}'.format(month), '{:02}'.format(day), hour)

out_filename = '{}-{}-{}-{}.json'.format(year, '{:02}'.format(month), '{:02}'.format(day), hour)
try:
    os.system('wget {} -P {}'.format(file_name, output_dir))
except:
    print("file not found")
try:
    os.system("gunzip {}/{}".format(output_dir, upload_filename))
except:
    print("no file to unzip")
outfile = '{}/{}'.format(output_dir, out_filename)
s3_dir = '{}-{}-{}'.format(year, '{:02}'.format(month), '{:02}'.format(day), hour)+ '/' + out_filename
upload_to_aws(outfile, 'bucket', s3_dir)
try:
    os.system('rm -rf {}'.format(output_dir))
except:
    print("no file to delete")
