FROM apache/airflow:3.0.3

RUN pip install airflow_provider_sap_hana Faker
