This is a repository for an end-to-end data analytics project for Data Sudo students that I'm teaching.
We will be ingesting data from disparate sources using Python, Airflow, Kafka, Pyspark, EC2, EMR and PowerBI for the complete data transformation and visualisation.

The repository will grow in complexity as we progress but I will strive to make all our codes and manifest files reusable.

Follow the instruction at `https://airflow.apache.org/docs/apache-airflow/2.0.1/start/docker.html` to see the list of files that are needed to be created or (get a docker compose.yaml template).

Ensure you have Docker application running.

Create a project directory and clone this repository postgres_data_lake branch.

Set the following env variables in a .env file in the project root folder:
`AIRFLOW_UID
_AIRFLOW_WWW_USER_USERNAME
_AIRFLOW_WWW_USER_PASSWORD
POSTGRES_USER
POSTGRES_PASSWORD
POSTGRES_DB
MINIO_ROOT_USER
MINIO_ROOT_PASSWORD`.

Build the docker immage by running `docker build -t extended_airflow:latest .`

Run `docker-compose up -d` to create and initialise a postgresql database with the connections.

Login to postgresql using DBeaver or PgAdmin or any other tool on localhost:5432.

create a database that `_AIRFLOW_WWW_USER_USERNAME` has all permissions on.

create survey_responses table:
`CREATE TABLE public.survey_responses (
    response_id SERIAL PRIMARY KEY,
    Country VARCHAR(100),
    Age VARCHAR(50),
    Gender VARCHAR(10),
    EdLevel VARCHAR(100),
    YearsCode VARCHAR(100),
    YearsCodePro VARCHAR(100),
    LanguageHaveWorkedWith TEXT,
    LanguageWantToWorkWith TEXT,
    DatabaseHaveWorkedWith TEXT,
    DatabaseWantToWorkWith TEXT,
    LearnCodeCoursesCert TEXT,
    ProfessionalTech TEXT,
    SOAccount VARCHAR(100),
    SOComm VARCHAR(100),
    MainBranch TEXT,
    Employment TEXT,
    DevType TEXT,
    WorkExp TEXT,
    RemoteWork VARCHAR(100),
    ConvertedCompYearly VARCHAR(100)
);
GRANT INSERT, UPDATE, DELETE ON survey_responses TO _AIRFLOW_WWW_USER_USERNAME;`

Login to airflow on localhost:8081 and set up a postgres connection on the Admin tab (the host is the container name `postgres`) \
and name the connection `connection_id`.

On the DAG view trigger the `load_postgres_script_dag` to load the Stackoverflow survey question to an automatically created table `load_question_to_postgres` and the survey response to the table created above `survey_responses`.

Verify that the load is successful.

We will be back to use this postgresql tables for our analysis.
