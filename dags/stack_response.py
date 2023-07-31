from os import path
import csv, tempfile, shutil, glob
from pyspark.sql.functions import col, count
from pyspark.sql import functions as F, Row
from airflow.models import Connection
import psycopg2
from stack_question import unzip_file

def get_file(directory):
    files = glob.glob(path.join(directory, '*.csv'))
    if not files:
        return None
    # Get the path of the most recent file based on creation time as the df is coalesced
    latest_file_path = max(files, key=path.getctime)
    return latest_file_path

def load_response_to_postgres(directory, filename, spark):
    # read survey response
    df = unzip_file(directory, filename, spark)
    selected_columns = [
        # Demographics
        'Country',
        'Age',
        'Gender',
        'EdLevel',
        # Programming experience
        'YearsCode',
        'YearsCodePro',
        'LanguageHaveWorkedWith',
        'LanguageWantToWorkWith',
        'DatabaseHaveWorkedWith',
        'DatabaseWantToWorkWith',
        'LearnCodeCoursesCert',
        'ProfessionalTech',
        'SOAccount',
        'SOComm',
        # Employment
        'MainBranch',
        'Employment',
        'DevType',
        'WorkExp',
        'RemoteWork',
        'ConvertedCompYearly'
        ]
    df = df.select(selected_columns)

    # For now, I am only interested in professional developers that specify their genders as man or woman
    strings_to_include = ['I used to be a developer', 'I am a developer']

    prof_df = df.filter(F.col('MainBranch').rlike('(^|\s)(' + '|'.join(strings_to_include) + ')(\s|$)'))
    filtered_df = prof_df.filter(F.col('Gender').rlike('(^|\s)(Man|Woman)(\s|$)'))
    # filtered_df = gender_df.withColumn("Age", col("Age").cast("int"))
    temp_output_dir = tempfile.mkdtemp()  # Create a temporary directory
    temp_csv_path = path.join(temp_output_dir, "temp_filtered_data")
    filtered_df.coalesce(1).write.option("header", "true").csv(temp_csv_path)
    return temp_csv_path

# Define the function to write each partition to PostgreSQL
def write_partition(directory, filename, table_name, spark):
    csv_file_path = load_response_to_postgres(directory, filename, spark)
    conn = Connection.get_connection_from_secrets('connection_id')
    database = conn.schema
    user_name = conn.login
    password = conn.password

    # Establish a connection to PostgreSQL
    conn = psycopg2.connect(
        user=user_name,
        password=password,
        host="postgres",  
        port=5432,  
        database=database, 
    )
    cursor = conn.cursor()

    insert_query = f"INSERT INTO {table_name} (Country, Age, Gender, EdLevel, YearsCode, YearsCodePro, " \
                   f"LanguageHaveWorkedWith, LanguageWantToWorkWith, DatabaseHaveWorkedWith, " \
                   f"DatabaseWantToWorkWith, LearnCodeCoursesCert, ProfessionalTech, SOAccount, SOComm, " \
                   f"MainBranch, Employment, DevType, WorkExp, RemoteWork, ConvertedCompYearly) " \
                   f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " \
                   f"ON CONFLICT DO NOTHING"

    with open(get_file(csv_file_path), "r") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            # Extract the values from CSV row
            values = (
                row["Country"], row["Age"], row["Gender"], row["EdLevel"], row["YearsCode"], row["YearsCodePro"],
                row["LanguageHaveWorkedWith"], row["LanguageWantToWorkWith"], row["DatabaseHaveWorkedWith"],
                row["DatabaseWantToWorkWith"], row["LearnCodeCoursesCert"], row["ProfessionalTech"], row["SOAccount"],
                row["SOComm"], row["MainBranch"], row["Employment"], row["DevType"], row["WorkExp"],
                row["RemoteWork"], row["ConvertedCompYearly"]
            )

            cursor.execute(insert_query, values)

    conn.commit()
    cursor.close()
    conn.close()
    shutil.rmtree(csv_file_path)