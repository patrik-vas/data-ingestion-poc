

# Data ingestion Airflow POC

This project utilizes **Managed Workflows for Apache Airflow (MWAA)** and can be run locally using the [MWAA Local Runner](https://github.com/aws/aws-mwaa-local-runner) provided by AWS.

## Local Setup

To run MWAA locally, follow these steps:

1. Follow the instructions in the MWAA Local Runner repository to set up the environment.

2. Ensure the `scripta-core-data-management` project is copied to the `dags` folder to be accessible by Airflow:
    - Place the codebase in the `dags` directory.
    - Add the core data folder to `.airflowignore` to prevent unnecessary processing.

3. Start the MWAA local environment, and your DAGs will be available for testing.
