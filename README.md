Spaces
PCB: Data Foundation

PCBDF-3006

PCBDF-2997


Design and Implement Migration Tool to Align Existing Tables with BigQuery Table Management Framework




Key details
Description

As a data engineer, I need a tool to migrate tables created in terraform to our BQ table management tool (in Airflow). More specifically, I need to:
a). remove tables from terraform states

b). capture current table schemas, partition, and clustering information and store them in our BQ table audit table.

since we don’t have DDL, the script field can use terraform, indicating the table was created by terraform

the table version should be set as '0', indicating this a schema created before our table management tool.

the schema creation time should be retrieved from creation_time of INFORMATION_SCHEMA.TABLES.

the schema field stores current table schema in JSON format

the meta-data field should stores partition and clustering information in JSON format

 

Acceptance criteria:
1. both a) and b) above are completed and being included in the same release. This is to avoid gaps in table change history.

Necessary validation is done before inserting schema 

if the version 0 schema already exists, the job should fail with record already exist error.

The tool should accept a list of tables and then backfill schemas of these tables one by one. One (one group) of Airflow task should be created for each table in the list.



///


"""
BigQuery Table Management DAG module for terraform table migration.
"""

import logging
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Optional

import pendulum
import util.constants as consts
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from dag_factory.terminus_dag_factory import add_tags
from google.cloud import bigquery
from util.miscutils import read_variable_or_file, read_yamlfile_env

logger = logging.getLogger(__name__)


class BigQueryTableConfig:
    """
    A class to handle BigQuery table configuration operations.
    """

    def __init__(self, project_id: str):
        """
        Initialize the BigQuery client.
        Args:
            project_id (str): Google Cloud project ID.
        """
        self.project_id = project_id

    @property
    def client(self):
        """
        This property ensures that the BigQuery client is created only once
        and reused across the class. The client is instantiated on first
        access and cached for subsequent use.

        Returns:
            bigquery.Client: An authenticated BigQuery client instance.
        """
        if not hasattr(self, "_client"):
            self._client = bigquery.Client(project=self.project_id)
        return self._client

    def get_table_config(self, table_fqn: str):
        """
        Fetch BigQuery table metadata and return in normalized format.

        Args:
            table_fqn (str): Fully qualified table name in the format project.dataset.table.

        Returns:
            tuple: Contains table schema, clustering & partitioning details, and last modified timestamp.
        """
        try:
            _, _, _ = table_fqn.split(".")
        except ValueError:
            raise ValueError("Expected format: project.dataset.table")

        # self.client = bigquery.Client(project=self.project_id)
        table = self.client.get_table(table_fqn)

        table_schema = {"columns": self._extract_fields(table.schema)}
        clustering_schema = {
            "clustering": table.clustering_fields or [],
            "partitioning": self._extract_partitioning(table),
        }
        last_modified = datetime.fromisoformat(table.modified.isoformat())

        return table_schema, clustering_schema, last_modified

    def _extract_fields(self, fields):
        """
        Extract schema fields from BigQuery table.

        Args:
            fields (list): BigQuery fields.

        Returns:
            list: Formatted list of column definitions.
        """
        result = []
        for field in fields:
            col = {"name": field.name, "type": field.field_type, "mode": field.mode}
            if field.default_value_expression:
                col["defaultValueExpression"] = field.default_value_expression
            if field.field_type == "RECORD" and field.fields:
                col["fields"] = self._extract_fields(field.fields)
            result.append(col)
        return result

    def _extract_partitioning(self, table):
        """
        Extract partitioning details from BigQuery table.

        Args:
            table (bigquery.Table): BigQuery table object.

        Returns:
            dict: Partitioning details.
        """
        if table.time_partitioning:
            return {
                "type": table.time_partitioning.type_,
                "field": table.time_partitioning.field or "_PARTITIONTIME",
            }
        return {}

    def build_table_config_rows(self, tables: list):
        """
        Build data rows from multiple BigQuery tables.

        Args:
            tables (list): List of table fully qualified names.

        Returns:
            dict: Rows containing table configurations.
        """
        rows = {}
        for table_id in tables:
            try:
                table_schema, clustering_details, last_modified = self.get_table_config(
                    table_id
                )
                rows[table_id] = {
                    "table_id": table_id,
                    "version": 0,
                    "applied_script": "terraform",
                    "insert_timestamp": last_modified,
                    "json_table_schema": table_schema,
                    "metadata": clustering_details,
                }
            except Exception as e:
                logger.error(f"Error processing {table_id}: {e}")
        logger.debug(f"rows value: {rows}")
        return rows

    @staticmethod
    def read_tables_from_file(file_path: str) -> list:
        """
        Read table list from file.

        Args:
            file_path (str): Path to the file containing table names.

        Returns:
            list: List of table names.
        """
        tables = []
        with open(file_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    tables.append(line)
        return tables

    def get_latest_version(self, target_table: str, table_ref: str) -> Optional[int]:
        """
        Get latest applied version for a table. Returns None if no history exists.

        Args:
            target_table (str): The target BigQuery table name.
            table_ref (str): The table_id reference to filter on.

        Returns:
            Optional[int]: Latest version; None if no history exists.
        """
        try:
            query = f"""
            SELECT version
            FROM `{target_table}`
            WHERE table_id = @table_id
            ORDER BY insert_timestamp DESC
            LIMIT 1
            """

            query_parameters = [
                bigquery.ScalarQueryParameter("table_id", "STRING", table_ref)
            ]
            query_job = self.client.query(
                query,
                job_config=bigquery.QueryJobConfig(query_parameters=query_parameters),
            )
            result = list(query_job.result())

            if result:
                return result[0].version
            return None
        except Exception as e:
            logger.error(f"[ERROR] Failed to get latest version for `{table_ref}`: {e}")
            return None

    def insert_row_bigquery(self, target_table: str, row: dict):
        """
        Insert a row into BigQuery using parameterized query.

        Args:
            target_table (str): Target BigQuery table for insert.
            row (dict): Row to insert.
        """
        query = f"""
        INSERT INTO `{target_table}` (
            table_id,
            version,
            applied_script,
            insert_timestamp,
            json_table_schema,
            metadata
        )
        VALUES (
            @table_id,
            @version,
            @applied_script,
            @insert_timestamp,
            @json_table_schema,
            @metadata
        )
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("table_id", "STRING", row["table_id"]),
                bigquery.ScalarQueryParameter("version", "INTEGER", row["version"]),
                bigquery.ScalarQueryParameter(
                    "applied_script", "STRING", row["applied_script"]
                ),
                bigquery.ScalarQueryParameter(
                    "insert_timestamp", "DATETIME", row["insert_timestamp"]
                ),
                bigquery.ScalarQueryParameter(
                    "json_table_schema", "JSON", row["json_table_schema"]
                ),
                bigquery.ScalarQueryParameter("metadata", "JSON", row["metadata"]),
            ]
        )
        job = self.client.query(query, job_config=job_config)
        job.result()
        logger.info(f"Insert successful for table {row['table_id']}")

    def update_table_schema(self, rows, table_id, target_table):
        """
        Update the schema in the BigQuery table.

        Args:
            rows (dict): Dictionary containing table configuration rows.
            table_id (str): Table ID to update.
            target_table (str): Target table in BigQuery.
        """
        # Fetch the specific row for the current table
        row = rows.get(table_id)
        if row is None:
            raise ValueError(f"Table ID '{table_id}' does not exist in the rows.")
        table_id_version = self.get_latest_version(target_table, row["table_id"])
        if table_id_version is None:
            self.insert_row_bigquery(target_table, row)
        elif table_id_version == 0:
            raise ValueError(
                f"Version is 0 for table ID '{row['table_id']}', unable to proceed."
            )
        else:
            logger.info(
                f"Row for table ID '{row['table_id']}' is not created via terraform. Skipping..."
            )


class BigQueryTableManager:
    """
    Manages BigQuery table configuration rows and operations.
    """

    def __init__(
        self, config_filename: str, config_dir: str = None, project_id: str = None
    ):
        """
        Initialize the manager with the required configs.

        Args:
            config_filename (str): Name of the configuration file.
            project_id (str): Google Cloud project ID.
        """
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        if config_dir is None:
            config_dir = f"{settings.DAGS_FOLDER}/config"

        self.config_dir = config_dir
        self.job_config = read_yamlfile_env(
            f"{config_dir}/{config_filename}", self.deploy_env
        )
        self.dbconfig = read_yamlfile_env(
            f"{self.config_dir}/db_config.yaml", self.deploy_env
        )
        self.local_tz = pendulum.timezone("America/Toronto")

        self.default_args = {
            "owner": "team-centaurs",
            "capability": "TBD",
            "severity": "P3",
            "sub_capability": "TBD",
            "business_impact": "TBD",
            "customer_impact": "TBD",
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 2,
            "retry_delay": timedelta(minutes=1),
            "retry_exponential_backoff": True,
        }

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        """
        Create a DAG based on the provided configuration.

        Args:
            dag_id (str): The ID for the DAG.
            config (dict): Configuration dictionary.

        Returns:
            DAG: DAG object with the defined tasks and configuration.
        """
        # Use config default_args if defined, otherwise use class default_args
        dag_default_args = deepcopy(self.default_args)
        if "default_args" in config:
            # Update only keys that exist in default_args
            dag_default_args.update(config["default_args"])

        # Create DAG object
        dag = DAG(
            dag_id=dag_id,
            default_args=dag_default_args,
            schedule=None,
            start_date=pendulum.today("America/Toronto").add(days=-1),
            dagrun_timeout=timedelta(hours=5),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True,
        )
        with dag:
            # Define start and end points for the DAG
            start_point = EmptyOperator(task_id="start")
            end_point = EmptyOperator(task_id="end")

            # Initialize BigQueryTableConfig
            table_manager = BigQueryTableConfig(config["project_id"])
            try:
                tables_config = [
                    table.replace("{env}", self.deploy_env)
                    for table in config["table_name"]
                ]
                rows = table_manager.build_table_config_rows(tables_config)
                print(f"tables_config is {tables_config}")
                print(f"tables_config is {tables_config}")
                # Iterate over the table configurations (assuming rows is a dict)
                for table_id in tables_config:
                    update_table_schema_task = PythonOperator(
                        task_id=f"task-{table_id}",
                        python_callable=table_manager.update_table_schema,
                        op_args=[rows, table_id, config["audit_table"]],
                    )
                    start_point >> update_table_schema_task >> end_point
            except TypeError:  # If no tables are present.
                start_point >> end_point
            return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if self.job_config:
            for job_id, config in self.job_config.items():
                dags[job_id] = self.create_dag(job_id, config)

        return dags

