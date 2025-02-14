import os
import sys
import logging
from airflow.models import BaseOperator
from airflow.exceptions import AirflowFailException


# Set up the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # You can adjust the logging level


def setup_django_for_airflow(client):
    try:
        # Add Django project root to path
        sys.path.append("/usr/local/airflow/dags/scripta-core-data-management/v3")
        os.environ.setdefault("AWS_SECRET_MANAGER", "int/scripta-core-data")
        os.environ.setdefault("DJANGO_UPLOAD_DIR", "upload/master_prod/")
        os.environ.setdefault("DJANGO_MASTER_SETTINGS", "master_integration_settings")
        os.environ.setdefault("DJANGO_LAMBDA_SETTINGS", "lambda_int_settings")
        os.environ.setdefault(
            "AWS_SSL_CERTIFICATE",
            "/usr/local/airflow/dags/scripta-core-data-management/v3/certificate/us-east-1-bundle.pem",
        )
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "lambda_int_settings")
        os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
        os.environ.setdefault("DJANGO_ALLOWED_HOSTS", "*")
        os.environ.setdefault("DATABASE_NAME", client)

        # Log Django setup process
        logger.info("Setting up Django...")

        import django
        from django.apps import apps
        from django.conf import settings

        settings.LOGGING_CONFIG = None
        if not apps.ready:
            django.setup()

        logger.info("Django setup completed.")

    except Exception as e:
        logger.error(f"Error setting up Django: {e}")
        raise


class DjangoOperator(BaseOperator):
    def pre_execute(self, context, *args, **kwargs):
        dag_run_conf = context["dag_run"].conf if context["dag_run"] else {}
        self.client = dag_run_conf.get("client")
        if self.client is None or self.client.strip() == "":
            raise AirflowFailException(
                "Mandatory parameters 'client' was not provided. Stopping DAG execution."
            )
        logger.info(f"Running pre_execute for {self.client} - {self.task_id}")
        setup_django_for_airflow(self.client)
