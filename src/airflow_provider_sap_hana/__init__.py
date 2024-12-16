__version__ = "1.0.0"


def get_provider_info():
    return {
        "package-name": "airflow-provider-sap-hana",
        "name": "SAP HANA Airflow Provider",
        "description": "An Airflow provider to connect to SAP HANA",
        "connection-types": [{
            "connection-type": "hana",
            "hook-class-name": "airflow_provider_sap_hana.hooks.hana.SapHanaHook",
        }],
        "versions": [__version__]
    }
