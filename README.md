This is a reuseable repository for an end-to-end data analytics project for Data Sudo students that I'm teaching.
We will be ingesting data from disparate sources using Python, Airflow, Kafka, Pyspark, EC2, EMR and PowerBI for the complete data transformation and visualisation.

The repository will grow in complexity as we progress but I will strive to make all our codes and manifest files reusable.

Follow the instruction at `https://airflow.apache.org/docs/apache-airflow/2.0.1/start/docker.html` to get a docker compose.yaml template.

Customise the template to use a user-defined network bridge (networkBridge) instead of the default bridge.

Change the executor from `SequentialExecutor` to `LocalExecutor`

Configure the compose.yaml to allow airflow use Postgres as backend

Add environmental vairables to the .env file to improve security

Configure the pg_isready health check and the postgres backend to use the newly defined env variables

Configure a compose.minio.yaml file from the run instructure at https://min.io/docs/minio/container/index.html.
This will serve as a replacement for AWS S3 during the testing stage

Run `docker-compose -f compose.yaml -f compose.minio.yaml up -d` to bring up the set up

Log in to minio to create a bucket, access and secret keys

Log in to Airflow and create a connection to minio from the Admin menu using the keys gotten above and minio service name as endpoint in json format in the `extra` tab.

The airflow compose.yaml file is already configured to allow airflow use remote logging. This should be commented out from the beginning until the bucket and connections are live.

You can modify the Dockerfile to add additional requirements and add a build context to the compose.yaml file

Run `docker-compose -f compose.yaml -f compose.minio.yaml up -d` to apply the changes.

We will be reusing this template and configuration for multiple data end-to-end projects. I will add additional customisation requirements as needed.

A dag file (utils.py) is added to test the set up.

Log in and verify everything is working as expoected.

We will be back with more dag files to ingest streaming data from Sensor, Financial market, Transport and other public APIs for this project.
