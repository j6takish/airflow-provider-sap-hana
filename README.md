# Airflow SAP HANA Provider
This packages enables Airflow to connect to SAP HANA using the official SAP Database cursor [hdbcli](https://pypi.org/project/hdbcli/).
This allows you to use the built-in Airflow operators and database hook methods, including methods that interact with
SQLAlchemy, via [sqlalchemy-hana](https://github.com/SAP/sqlalchemy-hana).

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Installing airflow-provider-sap-hana](#installing-airflow-provider-sap-hana)
- [Setting Up the Airflow/HANA Express Environment](#setting-up-the-airflowhana-express-environment)
  - [Initializing Airflow](#initializing-airflow)
  - [Starting HANA Express](#starting-hana-express)
  - [Start the Remaining Services](#start-the-remaining-services)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Installing airflow-provider-sap-hana
~~~
pip install airflow-provider-sap-hana
~~~
## Setting Up the Airflow/HANA Express Environment

This repository includes a `docker-compose` file designed to quickly set up an environment with everything needed to run
the example DAG. The `docker-compose` file is based on the file provided in the Airflow quick-start guide but with several
key modifications.

* An additional service `hana-express` - This creates a SAP HANA Express instance, a tenant database named HXE,
and a schema named 'AIRFLOW'

* Additional environment variable `AIRFLOW_CONN_HANA_DEFAULT` which contains the URI needed to connect to the HANA express instance.
* A custom `Dockerfile` to extend Airflow to include airflow-provider-sap-hana and Faker, to generate the mock data for the DAG.

The Airflow quick-start guide and the original `docker-compose` file can be found here  [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
### Initializing Airflow
The following directories and environment files need to be created before you initialize Airflow.
~~~
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
~~~
To initialize Airflow, run.
~~~
docker compose up airflow-init
~~~
After initialization is complete, you should see a message like this:
~~~
airflow-init_1       | Upgrades done
airflow-init_1       | Admin user airflow created
airflow-init_1       | 2.10.4
start_airflow-init_1 exited with code 0
~~~

### Starting HANA Express
Before you start HANA Express, you need to create a directory to persist data outside the container and grant
it the proper permissions.

~~~
mkdir -p ./hana
sudo chown 12000:79 ./hana
~~~
To start hana-express, run.
~~~
docker compose up hana-express -d
~~~
If you want to have visibility to the start-up process, run.
~~~
docker compose up hana-express -d && docker logs -f hana-express
~~~
This should take several minutes.

### Start the Remaining Services

To start the remaining Airflow services, run.
~~~
docker compose up -d
~~~
