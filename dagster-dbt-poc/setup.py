# Purpose: Makes project pip-installable in development mode. Key line: dagster-dbt in install_requires.
from setuptools import find_packages, setup

setup(
    name="dagster_dbt_poc",
    packages = find_packages(exclude=["dagster_dbt_poc_tests"]),
    install_requires=["dagster",
                      "dagit"
                      "dagster-dbt",
                      "dbt-core",
                      "dbt-duckdb",
                      "dbt-postgres",
                      "pandas"
                      ],
    extra_require = {"dev": ["dagit", "pytest"]},
)