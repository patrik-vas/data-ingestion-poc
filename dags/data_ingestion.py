from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor

from datetime import datetime, timedelta

from operators.operators import (
    ClaimDeduplicatorDjangoOperator,
    ClaimReviewPromoterDjangoOperator,
    RunReportDjangoOperator,
)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def success_callable():
    return False


# Set schedule_interval to None to disable automatic scheduling
with DAG(
    "data_ingestion",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    params={"client": ""},
) as dag:

    # Define tasks using BashOperator
    t1 = BashOperator(
        task_id="starting_point",
        bash_command='echo "Starting data ingestion"',
    )

    t2 = ClaimDeduplicatorDjangoOperator(
        task_id="claims_deduplicator",
    )

    t3 = ClaimReviewPromoterDjangoOperator(
        task_id="promote_claims_to_staging",
    )

    t4 = RunReportDjangoOperator(
        task_id="date_of_service_report", report_name="dates_of_service.sql"
    )

    t5 = RunReportDjangoOperator(
        task_id="pbm_audit_report", report_name="pbm_audit_report.sql"
    )

    waiting_for_user_input = PythonSensor(
        task_id="waiting_for_user_input", python_callable=success_callable
    )

    t6 = BashOperator(
        task_id="end_point",
        bash_command='echo "Ending data ingestion"',
    )

    t1 >> t2 >> t3 >> [t4, t5] >> waiting_for_user_input >> t6
