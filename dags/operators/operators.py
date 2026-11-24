from operators.common.django_operator import DjangoOperator
import logging
import json

# Set up the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # You can adjust the logging level


class MemberDeduplicatorDjangoOperator(DjangoOperator):
    def execute(self, context):
        logger.info(f"Starting Member deduplicator execution: {self.task_id}")
        try:
            from utils.antikruft import parse_matching_file  # noqa E402
            from data_transformation.deduplicator import EmployeeDeduplicator
            from constance import config  # noqa E402

            client_db = "default"
            comparison_fields = parse_matching_file(
                json.loads(config.EMPLOYEE_MAPPING_STRING).get("primary", {})
            ).keys()

            ed = EmployeeDeduplicator(client_db)
            ed.deduplicate(comparison_fields=comparison_fields)
            return "Deduplicated!"
        except Exception as e:
            logger.error(f"Error during task execution: {e}")
            raise


class MemberReviewPromoterDjangoOperator(DjangoOperator):
    def execute(self, context):
        logger.info(f"Starting  execution: {self.task_id}")
        try:
            from data_transformation.claims_linker import ClaimsLinker
            from data_transformation.review_promoter import ReviewMembersPromoter
            from constance import config  # noqa E402

            if not config.EMPLOYEE_MAPPING_STRING.strip():
                raise ValueError(
                    "Required constance config EMPLOYEE_MAPPING_STRING is not set"
                )

            client_db = self.client
            emp_map = json.loads(config.EMPLOYEE_MAPPING_STRING)
            if emp_map == {}:
                raise ValueError(
                    "Required constance config EMPLOYEE_MAPPING_STRING is empty"
                )

            rmp = ReviewMembersPromoter(emp_map, client_db)
            rmp.promote_to_staging()

            # run ClaimsLinker when only member file is loaded
            pbm_map = json.loads(config.PBM_MAPPING_STRING or {})

            cl = ClaimsLinker(
                client_db="default",
                pbm_map=pbm_map,
                employee_table="staging_employee",
                pbmhistory_table="staging_pbmhistory",
            )
            cl.link_claims_for_member_file()
        except Exception as e:
            logger.error(f"Error during task execution: {e}")
            raise


class ClaimDeduplicatorDjangoOperator(DjangoOperator):
    def execute(self, context):
        logger.info(f"Starting Claim deduplicator execution: {self.task_id}")
        try:
            from data_transformation.deduplicator import ClaimsDeduplicator
            from data_transformation.enums import Stage

            stage = Stage.REVIEW
            cd = ClaimsDeduplicator()
            cd.deduplicate(stage=stage)
            return "Deduplicated!"
        except Exception as e:
            logger.error(f"Error during task execution: {e}")
            raise


class ClaimReviewPromoterDjangoOperator(DjangoOperator):
    def execute(self, context):
        logger.info(f"Starting  execution: {self.task_id}")
        try:
            from data_transformation.claims_linker import ClaimsLinker
            from data_transformation.review_promoter import ReviewClaimsPromoter
            from constance import config  # noqa E402

            client_db = "default"
            pbm_map = json.loads(config.PBM_MAPPING_STRING or "{}")

            claims_linker = ClaimsLinker(client_db, pbm_map)
            rcp = ReviewClaimsPromoter(client_db, claims_linker)
            rcp.promote_claims()
        except Exception as e:
            logger.error(f"Error during task execution: {e}")
            raise


class RunReportDjangoOperator(DjangoOperator):

    def __init__(self, report_name: str, **kwargs):
        super().__init__(**kwargs)
        self.report_name = report_name

    def execute(self, context):
        logger.info(f"Starting  execution: {self.task_id}")
        try:
            from report.utils import run_standard_report

            report_name = self.report_name
            stage = "live_data"
            base_path = "/usr/local/airflow/dags/scripta-core-data-management/"
            starting_date = "2000-01-01"
            ending_date = "2100-01-01"
            header, results, count, sql = run_standard_report(
                starting_date, ending_date, report_name, base_path, stage
            )
            logger.info("Report:")
            logger.info(",".join([h[0] for h in header]))
            for result in results:
                logger.info(result)
        except Exception as e:
            logger.error(f"Error during task execution: {e}")
            raise
