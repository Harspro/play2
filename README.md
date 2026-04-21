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

