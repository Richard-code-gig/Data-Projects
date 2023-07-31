from os import path, remove
import glob, tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from airflow.models import Connection
from zipfile import ZipFile

remove_temp_file = []

spark_session = (SparkSession.builder
        .master("local[4]")
        .appName("DataTest")
        .config("spark.driver.extraClassPath", "/opt/airflow/drivers/postgresql-42.6.0.jar")
        .config("spark.executorEnv.PYTHONHASHSEED", "0")
        .getOrCreate()
    )

def get_filename(directory):
    # Get a list of files matching the pattern in the directory
    files = glob.glob(path.join(directory, 'data_*.zip'))
    if not files:
        return None
    # Get the path of the most recent file based on creation time
    latest_file_path = max(files, key=path.getctime)
    return latest_file_path

def write_to_postgres(df, table_name):
    # Get PostgreSQL connection properties from Airflow
    conn = Connection.get_connection_from_secrets('connection_id')
    database = conn.schema
    user_name = conn.login
    password = conn.password

    url = f"jdbc:postgresql://postgres:5432/{database}"

    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", user_name) \
        .option("password", password) \
        .option("rewriteBatchedInserts", "true") \
        .mode("overwrite") \
        .save()
    
    try:
        remove(remove_temp_file[0])
    except FileNotFoundError:
        pass

def unzip_file(directory, filename, spark) -> bool:
    zip_filename = get_filename(directory)
    # Read survey response
    with ZipFile(zip_filename, 'r') as archive:
        content = archive.read(filename).decode(encoding="utf-8")     
    
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as file:
        file.write(content)

    df = spark.read.csv(file.name, inferSchema=True, header=True, multiLine=True, escape='"')
    remove_temp_file.append(file.name)
    return df

def load_queston_to_postgres(directory, filename, table_name, spark):
    # read survey questions
    df = unzip_file(directory, filename, spark)
    filtered = df.where((col("qid")!='QID16')&(col("qid")!='QID12') \
                           &(col("qid")!='QID190')&(col("qid")!='QID61') \
                           &(col("qid")!='QID91')&(col("qid")!='QID121')
                           &(col("qid")!='QID295')&(col("qid")!='QID131')
                           ).select('qname', 'question') 
    
    write_to_postgres(filtered, table_name)
   