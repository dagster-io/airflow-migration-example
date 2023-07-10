from setuptools import find_packages, setup

setup(
    name="with_airflow",
    packages=find_packages(exclude=["with_airflow_tests"]),
    install_requires=[
        "dagster",
        "dagster_airflow",
        "dagster_aws",
        "apache-airflow==2.6.2",
        # for the kubernetes operator
        "apache-airflow-providers-cncf-kubernetes>=4.4.0",
        "apache-airflow-providers-docker>=3.1.0",
        # for the airflow dag code
        "pendulum",
        "pandas",
        "boto3",
        "requests",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
