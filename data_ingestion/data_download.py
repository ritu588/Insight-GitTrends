#script to download files one month at a time
import wget
import boto3
from argparse import ArgumentParser
from botocore.exceptions import NoCredentialsError
import gzip
import os

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3')

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except IOError:
        print("The file was not found")
        return False

parser = ArgumentParser()
parser.add_argument("-y", "--year", help="Please enter a four digit year.", required=True)
parser.add_argument("-m", "--month", help="Please enter a two digit month.", required=True)
parser.add_argument("-b", "--bucket", help="please enter a string for bucket name", required=True)

args = parser.parse_args()
if len(args.year) != 4:
    raise Exception("Please enter a four digit year. Ex: '2015'")
if len(args.month) != 2:
    raise Exception("Please enter a two digit month between 01 and 12.")
if (len(args.bucket) < 1):
    raise Exception("Please enter a bucket name.")

for i in range(1, 31):
    for j in range(1,24):
        file_name = 'https://data.gharchive.org/{}-{}-{}-{}.json.gz'.format(args.year, args.month, '{:02}'.format(i), j)
        print(file_name)
        output_dir = '{}-{}-{}'.format(args.year, args.month, '{:02}'.format(i), j)
        upload_filename = '{}-{}-{}-{}.json.gz'.format(args.year, args.month, '{:02}'.format(i), j)

        out_filename = '{}-{}-{}-{}.json'.format(args.year, args.month, '{:02}'.format(i), j)
        os.system('wget {} -P {}'.format(file_name, output_dir))
        os.system("gunzip {}/{}".format(output_dir, upload_filename))

        outfile = '{}/{}'.format(output_dir, out_filename)

        upload_to_aws(outfile, args.bucket, out_filename)
