FROM apache/airflow:2.10.4

RUN pip install airflow-provider-sap-hana Faker
