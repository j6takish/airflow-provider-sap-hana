FROM apache/airflow:2.10.5

RUN pip install airflow-provider-sap-hana Faker
