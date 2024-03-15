import argparse
import yaml
import boto3
import glob
import subprocess
import os 
import datetime

#pyspark libraries
from  pyspark.sql import SparkSession,SQLContext
import pyspark.sql.functions as F
from pyspark.sql.types import *

#function for Reading .yaml file
def read_yaml(path):
  with open(path,"r") as file:
     yaml_data = yaml.safe_load(file)
     return yaml_data
  

#Parsing arguments
parser = argparse.ArgumentParser(description="PySpark Application with Table Argument")
parser.add_argument("--table", help="Specify the table name", required=True)
args = parser.parse_args()
table_args = args.table


config = read_yaml("/app/config.yaml")

access_key = config['aws']["aws_access_key_id"]
secret_key = config['aws']["aws_secret_access_key"]
bucket_name = config['s3']["bucket_name"]
s3_key = config['s3pull']["source_file"]
target_table = config['s3pull']["target_table"]
source_sql = config['s3pull']["source_sql"]
source_table = config['s3push']["source_table"]
num_of_partitions = config['s3push']["num_of_partions"]

spark = SparkSession.builder.appName("migration")\
        .getOrCreate()
sql_context = SQLContext(spark)

 # Specify the S3 bucket and CSV file path
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key",access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key",secret_key)
spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialProvider")
    
database_url = config['postgres']["database_url"]
database_properties = {
        "user":config['postgres']["user"],
        "password": config['postgres']["password"],
        "driver": config['postgres']["driver"]
    }

def uplaod_to_s3(file_path,object_key):
    # Create an S3 client
    s3_client = boto3.client('s3',
                             aws_access_key_id=access_key,
                             aws_secret_access_key=secret_key,
                            )
    # Upload the file
    try:
        s3_client.upload_file(file_path,bucket_name,object_key)
        print(f"File uploaded successfully to s3://{bucket_name}/{object_key}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")

        
# providing underscore in place of space in column name
def column_transformation(dataframe):
    columns = dataframe.columns
    columns_with_underscores = [col.replace(" ","_").lower() for col in columns]
    for new_col,old_col in zip(columns_with_underscores,dataframe.columns):
        dataframe = dataframe.withColumnRenamed(old_col,new_col)
    return dataframe

def write_csv_to_pg():
  try:
        s3_url = f"s3a://{bucket_name}/{s3_key}"
        print(s3_url)
        df = spark.read.option("header", "true").csv(s3_url)
        new__df = column_transformation(df)
        sql_context.registerDataFrameAsTable(new__df,"csv_tbl")
        res = spark.sql(source_sql)
        res.write.jdbc(url=database_url, table=target_table, mode="overwrite", properties=database_properties)
        print(f"Data succesfully written to postgress db {target_table}!!!!!!")
    
  except Exception as e:
        print(f"Error while writing into  Postgress: {e}")

def write_pg_to_csv_in_parquet_format():
    df = spark.read.jdbc(url=database_url, table=source_table, properties=database_properties)
    df.repartition(num_of_partitions).write.mode("overwrite").parquet("/app/parquet")

    parquet_files = glob.glob(os.path.join("/app/parquet","*.parquet"))
    # for index,file_path in enumerate(parquet_files,start=1):
    #     orignal_file_name = os.path.basename(file_path)
    #     new_file = f"csv_data_2022_{index}.parquet"
    #     new_path = os.path.join("/app/parquet",new_file)
    #     os.rename(file_path,new_path)

    for file_path in parquet_files:
        current_datetime = datetime.datetime.now().strftime("%Y_%d-%m-%Y_%H:%M:%S")
        print(current_datetime)
        new_file = f"2022_csv_data_{current_datetime}.parquet"
        s3_url = f"s3://{bucket_name}/parquet/"
        # aws_cli = f"aws s3 mv {file_path} {s3_url}"
        # subprocess.run(aws_cli,shell=True,check=True)
        uplaod_to_s3(file_path,new_file)
        print(f"{file_path} to s3 completed!!! ")
      

def main():   
    print("local to S3 bucket")
    uplaod_to_s3("/app/2022_data.csv","arun.csv")

    print("s3 to Postgress") 
    write_csv_to_pg()

    print("reading from postgress")
    write_pg_to_csv_in_parquet_format()

  

if __name__ == "__main__":
    main()
