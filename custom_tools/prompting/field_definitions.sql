-- ===============================================================================
-- TOOL 2: FIELD DEFINITIONS
-- Provides comprehensive field definitions for query operator statistics
-- ===============================================================================

CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.QUERY_DEMO;

USE SCHEMA SNOWFLAKE_INTELLIGENCE.QUERY_DEMO;

CREATE OR REPLACE PROCEDURE FIELD_DEFINITIONS()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS $$
import json

def get_field_definitions() -> str:
    """Return comprehensive field definitions for Snowflake Intelligence."""
    return """
    QUERY OPERATOR STATISTICS FIELD DEFINITIONS:

    CORE FIELDS:
    - QUERY_ID: VARCHAR, The query ID, an internal, system-generated identifier for the SQL statement.
    - STEP_ID: NUMBER(38, 0), Identifier of the step in the query plan.
    - OPERATOR_ID: NUMBER(38, 0), The operator's identifier, unique within the query, starting at 0.
    - PARENT_OPERATORS: ARRAY of NUMBER(38, 0), Identifiers of the parent operators, or NULL for the final operator (usually Result). May be returned as a JSON string that needs parsing.
    - OPERATOR_TYPE: VARCHAR, The type of query operator including:
    * TableScan: Reads data from a table
    * Join/CartesianJoin: Combines data from multiple sources
    * Filter: Filters rows based on conditions
    * Sort/SortWithLimit: Orders data
    * Aggregate/GroupingSets: Performs aggregation operations
    * Result/ResultWorker: Final output operator
    * SecureView: Operations on secure views
    * CreateTableAsSelect: Creates table from query results
    * Delete/Insert/Update/Merge: DML operations
    * Flatten: Flattens semi-structured data
    * Generator: Generates rows
    * WindowFunction: Window function operations
    * And many others...

    OPERATOR_STATISTICS (JSON object with nested structures):

    I/O Statistics:
    - input_rows: INTEGER, Number of input rows processed by the operator.
    - output_rows: INTEGER, Number of output rows produced by the operator.
    - scan_progress: DOUBLE, Percentage of table scanned (0.0-1.0).
    - io.bytes_scanned: INTEGER, Number of bytes scanned (converted to MB/GB/TB in processed output).
    - io.percentage_scanned_from_cache: DOUBLE, Percentage of data scanned from cache (0-100).
    - io.bytes_written: INTEGER, Number of bytes written (converted to MB/GB/TB in processed output).
    - io.bytes_written_to_result: INTEGER, Number of bytes written to result (converted to MB/GB/TB).
    - io.bytes_read_from_result: INTEGER, Number of bytes read from result.
    - io.external_bytes_scanned: INTEGER, Bytes scanned from external sources.

    Pruning Statistics:
    - pruning.partitions_scanned: INTEGER, Number of partitions scanned (TableScan operations).
    - pruning.partitions_total: INTEGER, Total number of partitions available (TableScan operations).

    Network Statistics:
    - network_bytes: INTEGER, Number of bytes transferred over network.

    Spilling Statistics (appear under memory pressure):
    - spilling.bytes_spilled_remote_storage: INTEGER, Bytes spilled to remote storage.
    - spilling.bytes_spilled_local_storage: INTEGER, Bytes spilled to local storage.

    DML Statistics (for Insert/Update/Delete operations):
    - dml.number_of_rows_inserted: DOUBLE, Number of rows inserted.
    - dml.number_of_rows_updated: DOUBLE, Number of rows updated.
    - dml.number_of_rows_deleted: DOUBLE, Number of rows deleted.
    - dml.number_of_rows_unloaded: DOUBLE, Number of rows unloaded during data export.

    External Function Statistics (for UDFs and external services):
    - external_function.total_invocations: DOUBLE, Number of times the external function was called.
    - external_function.rows_sent: DOUBLE, Number of rows sent to external functions.
    - external_function.rows_received: DOUBLE, Number of rows received from external functions.
    - external_function.bytes_sent: INTEGER, Bytes sent to external functions (converted to MB/GB/TB).
    - external_function.bytes_received: INTEGER, Bytes received from external functions (converted to MB/GB/TB).
    - external_function.bytes_sent_x_region: INTEGER, Bytes sent across regions (converted to MB/GB/TB).
    - external_function.bytes_received_x_region: INTEGER, Bytes received across regions (converted to MB/GB/TB).
    - external_function.retries_due_to_transient_errors: DOUBLE, Number of retries due to transient errors.
    - external_function.average_latency_per_call: DOUBLE, Average latency per external function call (milliseconds).
    - external_function.average_latency: DOUBLE, Overall average latency for external function calls.
    - external_function.avg_throttle_latency_overhead: DOUBLE, Average latency overhead due to throttling.
    - external_function.batches_retried_due_to_throttling: DOUBLE, Number of batches retried due to throttling.
    - external_function.http_4xx_errors: DOUBLE, Number of HTTP 4xx errors from external functions.
    - external_function.http_5xx_errors: DOUBLE, Number of HTTP 5xx errors from external functions.
    - external_function.latency_per_successful_call_p50: DOUBLE, 50th percentile latency for successful calls.
    - external_function.latency_per_successful_call_p90: DOUBLE, 90th percentile latency for successful calls.
    - external_function.latency_per_successful_call_p95: DOUBLE, 95th percentile latency for successful calls.
    - external_function.latency_per_successful_call_p99: DOUBLE, 99th percentile latency for successful calls.

    EXECUTION_TIME_BREAKDOWN (JSON object):
    - overall_percentage: DOUBLE, Percentage of total query execution time consumed by this operator (0-100).
    - initialization: DOUBLE, Time spent setting up query processing for this operator.
    - processing: DOUBLE, Time spent processing the data by the CPU.
    - synchronization: DOUBLE, Time spent synchronizing activities between participating processes.
    - local_disk_io: DOUBLE, Time waiting for local disk access.
    - remote_disk_io: DOUBLE, Time waiting for remote disk access.
    - network_communication: DOUBLE, Time waiting for network data transfer.
    - other: DOUBLE, Time spent on other operations (common in DDL operations).

    OPERATOR_ATTRIBUTES (JSON object, varies by operator type):

    For Aggregate Operations:
    - functions: ARRAY of VARCHAR, List of aggregate functions computed (e.g., SUM, COUNT, AVG).
    - grouping_keys: ARRAY of VARCHAR, The GROUP BY expressions.

    For Join Operations:
    - join_type: VARCHAR, Type of join (INNER, LEFT OUTER, RIGHT OUTER, FULL OUTER).
    - equality_join_condition: VARCHAR, Equality-based join expressions.
    - additional_join_condition: VARCHAR, Non-equality join expressions (e.g., range conditions).

    For Filter Operations:
    - filter_condition: VARCHAR, The WHERE clause or filter expression used.

    For Sort Operations:
    - sort_keys: ARRAY of VARCHAR, Expressions defining the sorting order.

    For SortWithLimit Operations:
    - sort_keys: ARRAY of VARCHAR, Expressions defining the sorting order.
    - offset: NUMBER, Position in the ordered sequence from which produced tuples are emitted.
    - rows: NUMBER, Number of rows produced (LIMIT value).

    For TableScan Operations:
    - table_name: VARCHAR, Full name of the table being scanned.
    - table_alias: VARCHAR, Alias used in the query for this table.
    - columns: ARRAY of VARCHAR, List of columns being read from the table.
    - extracted_variant_paths: ARRAY of VARCHAR, List of paths extracted from VARIANT columns.

    For CreateTableAsSelect Operations:
    - table_name: VARCHAR, Target table name being created.
    - input_expressions: ARRAY of VARCHAR, SELECT clause expressions from your CREATE TABLE AS SELECT query. Each element represents one column being created. Very long expressions (>1000 chars) may be truncated with "..." by Snowflake to prevent response size issues.

    For Delete/Insert/Update/Merge Operations:
    - table_name: VARCHAR, Name of the table being modified.
    - input_expression: VARCHAR, Expressions being inserted (Insert operations).
    - table_names: ARRAY of VARCHAR, List of table names affected (Insert operations).

    For Flatten Operations:
    - input: VARCHAR, The input expression used to flatten semi-structured data.

    For Generator Operations:
    - row_count: NUMBER, Value of the input parameter ROWCOUNT.
    - time_limit: NUMBER, Value of the input parameter TIMELIMIT.

    For GroupingSets Operations:
    - functions: ARRAY of VARCHAR, List of aggregate functions computed.
    - key_sets: ARRAY of VARCHAR, List of grouping sets.

    For WindowFunction Operations:
    - functions: ARRAY of VARCHAR, List of window functions computed.

    For Pivot Operations:
    - grouping_keys: ARRAY of VARCHAR, Remaining columns on which results are aggregated.
    - pivot_column: ARRAY of VARCHAR, Resulting columns of pivot values.

    For Unpivot Operations:
    - expressions: ARRAY of VARCHAR, Output columns of the unpivot query.

    For Result Operations:
    - expressions: ARRAY of VARCHAR, List of expressions produced in the final result.

    For ValuesClause Operations:
    - value_count: NUMBER, Number of produced values.
    - values: VARCHAR, List of values.

    For WithClause Operations:
    - name: VARCHAR, Alias of the WITH clause.

    For ExternalScan Operations:
    - stage_name: VARCHAR, Name of the stage from which data is read.
    - stage_type: VARCHAR, Type of the stage.

    For InternalObject Operations:
    - object_name: VARCHAR, Name of the accessed internal object.

    For JoinFilter Operations:
    - join_id: NUMBER, Operator ID of the join used to identify tuples that can be filtered out.

    For Unload Operations:
    - location: VARCHAR, Stage where data is saved.

    IMPORTANT NOTES:
    - Many fields only appear under specific conditions (e.g., spilling only under memory pressure)
    - External function statistics only appear when UDFs or external services are used
    - DML statistics only appear for INSERT/UPDATE/DELETE operations
    - Complex operator attributes depend on the specific query and operations performed
    - Expression truncation: Snowflake may truncate very long SQL expressions with "..." to keep response sizes manageable
    - All byte values are converted to human-readable format (MB/GB/TB) in the processed output"""

def main(session) -> str:
    """
    Main handler function that returns field definitions.
    
    Args:
        session: Snowflake session object
        
    Returns:
        JSON string containing field definitions
    """
    return json.dumps({
        "status": "success",
        "field_definitions": get_field_definitions()
    }, indent=2)
$$;

-- Example usage:
-- CALL SNOWFLAKE_INTELLIGENCE.QUERY_DEMO.FIELD_DEFINITIONS();