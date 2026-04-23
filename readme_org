"""
BigQuery Table Management DAG module.

This module registers dynamic DAGs using DAGFactory and
BigQueryTableManagementDagBuilder.

Each DAG manages schema evolution for a single BigQuery table
using versioned scripts and strict DDL safety enforcement.
"""

import logging
from datetime import timedelta
from typing import Dict, Final, List

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import util.constants as consts
from dag_factory.abc import BaseDagBuilder
from dag_factory.environment_config import EnvironmentConfig
from dag_factory.terminus_dag_factory import DAGFactory

from bigquery_table_management.handlers.handler_factory import HandlerFactory

logger = logging.getLogger(__name__)

CONFIG_FILENAME: Final[str] = "bigquery_table_management_config.yaml"
DAG_TIMEOUT: Final[timedelta] = timedelta(minutes=30)


# ==========================================================
# DAG BUILDER
# ==========================================================


class BigQueryTableManagementDagBuilder(BaseDagBuilder):
    """
    DAG builder responsible for constructing the BigQuery Table Management DAG.

    This builder orchestrates schema changes for a single BigQuery table
    using versioned scripts. It handles environment, and dynamic task wiring.

    Attributes:
        project_id (str):
            Default GCP project ID derived from environment configuration.
        deploy_env (str):
            Deployment environment (e.g., dev, uat, prod).
    """

    def __init__(self, environment_config: EnvironmentConfig):
        """
        Initialize the DAG builder with environment configuration.

        Args:
            environment_config (EnvironmentConfig):
                Environment-specific configuration containing
                GCP project settings and deployment environment.

        Sets:
            self.project_id (str):
                Default landing zone project ID.
            self.deploy_env (str):
                Current deployment environment.
        """
        super().__init__(environment_config)
        self.project_id = self.environment_config.gcp_config.get(
            consts.LANDING_ZONE_PROJECT_ID
        )
        self.deploy_env = self.environment_config.deploy_env
        self.bq_location = self.environment_config.gcp_config.get(
            consts.BQ_QUERY_LOCATION, consts.BQ_DEFAULT_LOCATION
        )

    # ------------------------------------------------------
    # Helpers
    # ------------------------------------------------------

    def _extract_dataset_id(self, table_ref: str) -> str:
        """
        Extract dataset ID from a BigQuery table reference.

        Supports:
            - project.dataset.table
            - dataset.table

        Args:
            table_ref (str):
                Fully qualified or partially qualified table reference.

        Returns:
            str:
                Extracted dataset ID.

        Raises:
            AirflowException:
                If table_ref format is invalid.
        """
        parts = table_ref.split(".")

        if len(parts) == 3:
            return parts[1]  # project.dataset.table

        if len(parts) == 2:
            return parts[0]  # dataset.table

        raise AirflowException(
            f"Invalid table_ref format: {table_ref}. "
            f"Expected project.dataset.table or dataset.table"
        )

    def _extract_project_id(self, table_ref: str) -> str:
        """
        Extract project ID from a BigQuery table reference.

        Supports:
            - project.dataset.table
            - dataset.table (falls back to default project)

        Args:
            table_ref (str):
                BigQuery table reference.

        Returns:
            str:
                Extracted or default project ID.

        Raises:
            AirflowException:
                If table_ref format is invalid.

        """
        parts = table_ref.split(".")

        if len(parts) == 3:
            return parts[0]

        if len(parts) == 2:
            logger.info(
                f"No project in table_ref. Using default project: {self.project_id}"
            )
            return self.project_id

        raise AirflowException(f"Invalid table_ref format: {table_ref}")

    # ------------------------------------------------------
    # Execution Logic
    # ------------------------------------------------------

    def execute_table_schema_changes(
        self,
        table_ref: str,
        table_project_id: str,
        scripts: List[str],
        config: Dict,
        **context,
    ):
        """
        Execute versioned schema change scripts for BigQuery table.

        This method:
            - Iterates through ordered scripts
            - Resolves appropriate handler using HandlerFactory
            - Executes scripts with idempotency enforcement
            - Logs execution outcome

        Args:
            table_ref (str): Fully qualified table reference.
            table_project_id (str): GCP project ID of the target table.
            scripts (List[str]): Ordered list of schema script paths.
            config (Dict): DAG configuration dictionary.
            **context:
                Airflow task execution context.

        Raises:
            AirflowException:
                If scripts list is empty or execution fails.
        """

        logger.info(f"Starting schema execution for table: {table_ref}")

        if not scripts:
            raise AirflowException("No scripts provided in config.")

        dataset_id = self._extract_dataset_id(table_ref)

        # Extract timezone with defensive fallback
        timezone_str = getattr(
            self.environment_config.local_tz,
            "name",
            str(self.environment_config.local_tz),
        )

        # Validate timezone is non-empty
        if not timezone_str or not timezone_str.strip():
            raise AirflowException(
                "Timezone cannot be empty. Check environment_config.local_tz configuration."
            )

        for script_path in scripts:

            logger.info(f"Processing script: {script_path}")

            try:
                handler = HandlerFactory.get_handler(
                    script_path=script_path,
                    project_id=table_project_id,
                    dataset_id=dataset_id,
                    deploy_env=self.deploy_env,
                    timezone=timezone_str,
                    location=self.bq_location,
                )

                executed = handler.execute(
                    script_path=script_path,
                    table_ref=table_ref,
                )

                if executed:
                    logger.info(f"Executed: {script_path}")
                else:
                    logger.info(f"Skipped (already applied): {script_path}")
            except Exception as e:
                logger.error(
                    f"Failed to execute script {script_path} for table {table_ref}: {e}",
                    exc_info=True,
                )
                raise AirflowException(
                    f"Script execution failed: {script_path}. Error: {str(e)}"
                ) from e

        logger.info("Schema execution completed.")

    # ------------------------------------------------------
    # DAG Builder
    # ------------------------------------------------------

    def build(self, dag_id: str, config: Dict) -> DAG:
        """
        Build the BigQuery Table Management DAG.

        The constructed DAG:
            - Validates required configuration
            - Replaces environment placeholders
            - Creates execution task
            - Enforces DAG timeout and execution constraints

        Args:
            dag_id (str): DAG identifier.
            config (dict): DAG configuration including:
                    - table_ref (str)
                    - scripts (List[str])
                    - default_args (optional)
                    - tags (optional)

        Returns:
            DAG:
                Fully constructed DAG instance.

        Raises:
            AirflowException:
                If mandatory configuration values are missing.
        """

        default_args = self.prepare_default_args(config.get("default_args", {}))

        table_ref = config.get("table_ref")
        scripts = config.get("scripts", [])

        if not table_ref:
            raise AirflowException(f"Missing table_ref in config for DAG: {dag_id}")

        # Replace environment placeholder
        table_ref = table_ref.replace("{env}", self.deploy_env)

        table_project_id = self._extract_project_id(table_ref)

        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            schedule=None,
            start_date=pendulum.datetime(
                2026, 2, 1, tz=self.environment_config.local_tz
            ),
            catchup=False,
            max_active_runs=1,
            dagrun_timeout=DAG_TIMEOUT,
            render_template_as_native_obj=True,
            is_paused_upon_creation=True,
            tags=config.get("tags", []),
        )

        with dag:

            start = EmptyOperator(task_id="start")

            execute_task = PythonOperator(
                task_id="execute_table_schema_changes",
                python_callable=self.execute_table_schema_changes,
                op_kwargs={
                    "table_ref": table_ref,
                    "table_project_id": table_project_id,
                    "scripts": scripts,
                    "config": config,
                },
            )

            end = EmptyOperator(task_id="end")

            start >> execute_task >> end

        return dag


# ==========================================================
# DAG REGISTRATION
# ==========================================================

try:
    dag_factory = DAGFactory()
    dags = dag_factory.create_dynamic_dags(
        dag_class=BigQueryTableManagementDagBuilder,
        config_filename=CONFIG_FILENAME,
    )
    globals().update(dags)

except Exception as e:
    logger.error(
        f"Failed to create DAGs from config '{CONFIG_FILENAME}': {e}",
        exc_info=True,
    )
    raise
