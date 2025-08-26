-- ===============================================================================
-- TOOL 1: QUERY DATA FETCHER V3 - Operators as JSON String Array
-- Solves line count truncation by storing operators as JSON strings
-- ===============================================================================

CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.QUERY_DEMO;

USE SCHEMA SNOWFLAKE_INTELLIGENCE.QUERY_DEMO;

DROP PROCEDURE IF EXISTS
  SNOWFLAKE_INTELLIGENCE.QUERY_DEMO.QUERY_DATA_FETCHER(STRING);

CREATE OR REPLACE PROCEDURE QUERY_DATA_FETCHER(
    query_id STRING,
    output_format STRING DEFAULT 'pretty'  -- 'pretty' or 'minified'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS $$
import json
import uuid
from datetime import datetime
from typing import Dict, List, Any, Union, Optional

def format_bytes(bytes_val: float) -> str:
    """Convert bytes to human-readable format (MB, GB, TB) with consistent formatting."""
    if bytes_val is None or bytes_val == 0:
        return "0.00 MB"
    
    if bytes_val >= 1e12:
        return f"{bytes_val/1e12:.2f} TB"
    elif bytes_val >= 1e9:
        return f"{bytes_val/1e9:.2f} GB"
    else:
        return f"{bytes_val/1e6:.2f} MB"

def truncate_expression(expr: str, max_length: int = 200) -> str:
    """Truncate long expressions to prevent JSON bloat."""
    if not expr or len(expr) <= max_length:
        return expr
    
    # Keep first 80 and last 80 chars for 200 char limit
    keep_chars = (max_length - 20) // 2
    return f"{expr[:keep_chars]}...[truncated]...{expr[-keep_chars:]}"

def parse_parent_operators(parent_ops: Union[str, List, None]) -> Optional[List[int]]:
    """Parse PARENT_OPERATORS field which can be a JSON string, list, or null."""
    if parent_ops is None:
        return None
    
    if isinstance(parent_ops, str):
        try:
            parsed = json.loads(parent_ops)
            if isinstance(parsed, list):
                return parsed
        except (json.JSONDecodeError, ValueError):
            pass
    
    if isinstance(parent_ops, list):
        return parent_ops
    
    return None

def condense_operator_to_essentials(row, operator_id: int) -> Dict[str, Any]:
    """Extract only essential fields from an operator row."""
    condensed = {
        "operator_id": operator_id,
        "operator_type": row['OPERATOR_TYPE'],
        "parent_operators": parse_parent_operators(row['PARENT_OPERATORS'])
    }
    
    # Add execution time if significant
    if row['EXECUTION_TIME_BREAKDOWN']:
        try:
            exec_breakdown = json.loads(row['EXECUTION_TIME_BREAKDOWN'])
            overall_pct = exec_breakdown.get('overall_percentage', 0)
            if overall_pct > 0:
                condensed['overall_percentage'] = overall_pct
                
                # Find primary time category if execution is significant
                if overall_pct > 5:
                    categories = {
                        'processing': exec_breakdown.get('processing', 0),
                        'sync': exec_breakdown.get('synchronization', 0),
                        'local_io': exec_breakdown.get('local_disk_io', 0),
                        'remote_io': exec_breakdown.get('remote_disk_io', 0),
                        'network': exec_breakdown.get('network_communication', 0),
                        'other': exec_breakdown.get('other', 0)
                    }
                    # Get highest category
                    max_cat = max(categories.items(), key=lambda x: x[1])
                    if max_cat[1] > 0:
                        condensed['primary_time'] = f"{max_cat[0]}:{max_cat[1]}%"
        except json.JSONDecodeError:
            pass
    
    # Add operator statistics
    if row['OPERATOR_STATISTICS']:
        try:
            stats = json.loads(row['OPERATOR_STATISTICS'])
            
            # Row counts
            if 'input_rows' in stats and stats['input_rows']:
                condensed['input_rows'] = stats['input_rows']
            if 'output_rows' in stats and stats['output_rows']:
                condensed['output_rows'] = stats['output_rows']
            
            # I/O metrics
            io = stats.get('io', {})
            if io:
                if io.get('bytes_scanned'):
                    condensed['bytes_scanned'] = format_bytes(io['bytes_scanned'])
                if io.get('percentage_scanned_from_cache'):
                    condensed['cache_hit_rate'] = io['percentage_scanned_from_cache']
                if io.get('bytes_written'):
                    condensed['bytes_written'] = format_bytes(io['bytes_written'])
            
            # Pruning for TableScans
            pruning = stats.get('pruning', {})
            if pruning and pruning.get('partitions_total'):
                scanned = pruning.get('partitions_scanned', 0)
                total = pruning['partitions_total']
                if total > 0:
                    condensed['pruning_efficiency'] = round((1 - scanned/total) * 100, 1)
            
            # Spilling if present
            spilling = stats.get('spilling', {})
            remote = spilling.get('bytes_spilled_remote_storage', 0)
            local = spilling.get('bytes_spilled_local_storage', 0)
            if remote > 0 or local > 0:
                condensed['spilling'] = format_bytes(remote + local)
            
            # DML stats if present
            dml = stats.get('dml', {})
            dml_total = sum([
                dml.get('number_of_rows_inserted', 0),
                dml.get('number_of_rows_updated', 0),
                dml.get('number_of_rows_deleted', 0)
            ])
            if dml_total > 0:
                condensed['dml_rows_affected'] = dml_total
                
        except json.JSONDecodeError:
            pass
    
    # Add key attributes based on operator type
    if row['OPERATOR_ATTRIBUTES']:
        try:
            attrs = json.loads(row['OPERATOR_ATTRIBUTES'])
            
            # TableScan
            if row['OPERATOR_TYPE'] == 'TableScan':
                if 'table_name' in attrs:
                    condensed['table_name'] = attrs['table_name']
                if 'columns' in attrs:
                    condensed['column_count'] = len(attrs['columns'])
                    
            # Joins
            elif row['OPERATOR_TYPE'] in ['InnerJoin', 'LeftOuterJoin', 'RightOuterJoin', 'CartesianJoin']:
                if 'equality_join_condition' in attrs:
                    condensed['join_condition'] = truncate_expression(attrs['equality_join_condition'], 100)
                    
            # Filter
            elif row['OPERATOR_TYPE'] == 'Filter':
                if 'filter_condition' in attrs:
                    condensed['filter_condition'] = truncate_expression(attrs['filter_condition'], 150)
                    
            # Aggregate
            elif row['OPERATOR_TYPE'] in ['Aggregate', 'GroupingSets']:
                functions = attrs.get('functions', [])
                if functions:
                    condensed['aggregate_functions'] = ','.join(functions[:5])
                grouping_keys = attrs.get('grouping_keys', [])
                if grouping_keys:
                    condensed['group_by'] = ','.join(grouping_keys[:3])
                    
            # Sort
            elif row['OPERATOR_TYPE'] in ['Sort', 'SortWithLimit']:
                sort_keys = attrs.get('sort_keys', [])
                if sort_keys:
                    condensed['sort_keys'] = ','.join(sort_keys[:3])
                if row['OPERATOR_TYPE'] == 'SortWithLimit' and 'rows' in attrs:
                    condensed['limit'] = attrs['rows']
                    
            # CreateTableAsSelect
            elif row['OPERATOR_TYPE'] == 'CreateTableAsSelect':
                if 'table_name' in attrs:
                    condensed['target_table'] = attrs['table_name']
                if 'input_expressions' in attrs:
                    expressions = attrs['input_expressions']
                    condensed['expression_count'] = len(expressions)
                    if expressions:
                        # Show first expression sample
                        condensed['sample_expression'] = truncate_expression(expressions[0], 150)
                        
            # DML Operations
            elif row['OPERATOR_TYPE'] in ['Insert', 'Update', 'Delete', 'Merge']:
                if 'table_name' in attrs:
                    condensed['target_table'] = attrs['table_name']
                    
            # Result
            elif row['OPERATOR_TYPE'] == 'Result':
                if 'expressions' in attrs:
                    condensed['output_columns'] = len(attrs['expressions'])
                    
        except json.JSONDecodeError:
            pass
    
    return condensed

def calculate_summary_metrics(operator_stats_list: List[Dict[str, Any]], df) -> Dict[str, Any]:
    """Calculate summary metrics from the raw dataframe."""
    total_bytes_scanned = 0
    total_bytes_written = 0
    total_bytes_spilled = 0
    final_output_rows = 0
    total_dml_rows = 0
    cache_hits = []
    pruning_efficiencies = []
    high_execution_operators = []
    operators_with_spilling = []
    exploding_joins = []
    
    for _, row in df.iterrows():
        operator_id = row['OPERATOR_ID']
        operator_type = row['OPERATOR_TYPE']
        
        # Parse statistics
        if row['OPERATOR_STATISTICS']:
            try:
                stats = json.loads(row['OPERATOR_STATISTICS'])
                
                # Track final output rows from Result operator
                # Result operator shows final rows as input_rows, not output_rows
                if operator_type == 'Result':
                    input_rows = stats.get('input_rows', 0)
                    if input_rows:
                        final_output_rows = input_rows
                
                # Sum I/O bytes
                io = stats.get('io', {})
                total_bytes_scanned += io.get('bytes_scanned', 0)
                total_bytes_scanned += io.get('external_bytes_scanned', 0)
                total_bytes_written += io.get('bytes_written', 0)
                total_bytes_written += io.get('bytes_written_to_result', 0)
                
                # Cache hits
                cache_hit = io.get('percentage_scanned_from_cache')
                if cache_hit is not None:
                    cache_hits.append(cache_hit)
                
                # Pruning
                pruning = stats.get('pruning', {})
                if pruning and pruning.get('partitions_total'):
                    scanned = pruning.get('partitions_scanned', 0)
                    total = pruning['partitions_total']
                    if total > 0:
                        efficiency = (1 - scanned/total) * 100
                        pruning_efficiencies.append(efficiency)
                
                # Spilling
                spilling = stats.get('spilling', {})
                remote = spilling.get('bytes_spilled_remote_storage', 0)
                local = spilling.get('bytes_spilled_local_storage', 0)
                if remote > 0 or local > 0:
                    total_bytes_spilled += remote + local
                    operators_with_spilling.append({
                        "operator_id": operator_id,
                        "operator_type": operator_type,
                        "bytes_spilled_remote": format_bytes(remote) if remote > 0 else "0 MB",
                        "bytes_spilled_local": format_bytes(local) if local > 0 else "0 MB",
                        "total_bytes_spilled": format_bytes(remote + local)
                    })
                
                # DML rows
                dml = stats.get('dml', {})
                total_dml_rows += dml.get('number_of_rows_inserted', 0)
                total_dml_rows += dml.get('number_of_rows_updated', 0)
                total_dml_rows += dml.get('number_of_rows_deleted', 0)
                
                # Check for exploding joins
                if operator_type in ['Join', 'InnerJoin', 'LeftOuterJoin', 'RightOuterJoin', 'OuterJoin', 'CartesianJoin']:
                    input_rows = stats.get('input_rows', 0)
                    output_rows = stats.get('output_rows', 0)
                    if input_rows > 0 and output_rows > input_rows * 10:
                        exploding_joins.append({
                            "operator_id": operator_id,
                            "operator_type": operator_type,
                            "input_rows": input_rows,
                            "output_rows": output_rows,
                            "multiplication_factor": round(output_rows / input_rows, 2)
                        })
                
            except json.JSONDecodeError:
                pass
        
        # Parse execution time
        if row['EXECUTION_TIME_BREAKDOWN']:
            try:
                exec_breakdown = json.loads(row['EXECUTION_TIME_BREAKDOWN'])
                overall_pct = exec_breakdown.get('overall_percentage', 0)
                if overall_pct > 15:
                    high_execution_operators.append({
                        "id": operator_id,
                        "type": operator_type,
                        "pct": overall_pct
                    })
            except json.JSONDecodeError:
                pass
    
    # Determine query type
    query_type = "SELECT"
    for _, row in df.iterrows():
        if row['OPERATOR_TYPE'] == 'CreateTableAsSelect':
            query_type = "CREATE TABLE AS SELECT"
            break
        elif row['OPERATOR_TYPE'] == 'Insert':
            query_type = "INSERT"
            break
        elif row['OPERATOR_TYPE'] in ['Update', 'Delete', 'Merge']:
            query_type = row['OPERATOR_TYPE'].upper()
            break
    
    # Build summary
    summary = {
        "query_type": query_type,
        "operator_count": len(df),
        "total_bytes_scanned": format_bytes(total_bytes_scanned),
        "total_bytes_written": format_bytes(total_bytes_written),
        "final_output_rows": final_output_rows
    }
    
    # Add DML metrics if relevant
    if total_dml_rows > 0:
        summary["dml_rows_affected"] = {
            "total": total_dml_rows
        }
    else:
        summary["dml_rows_affected"] = {
            "inserted": 0,
            "updated": 0,
            "deleted": 0
        }
    
    # Add spilling information
    if total_bytes_spilled > 0:
        summary["total_bytes_spilled"] = format_bytes(total_bytes_spilled)
        summary["spilling_operator_count"] = len(operators_with_spilling)
    
    # Always include cache and pruning statistics
    summary["average_cache_hit_rate"] = round(sum(cache_hits) / len(cache_hits), 1) if cache_hits else 0.0
    summary["average_pruning_efficiency"] = round(sum(pruning_efficiencies) / len(pruning_efficiencies), 1) if pruning_efficiencies else 0.0
    
    # Convert performance issues to JSON strings like operators
    high_execution_json = [json.dumps(op, separators=(',', ':')) for op in high_execution_operators]
    exploding_joins_json = [json.dumps(op, separators=(',', ':')) for op in exploding_joins]
    spilling_operators_json = [json.dumps(op, separators=(',', ':')) for op in operators_with_spilling]
    
    # Add detailed performance issues section with JSON strings
    performance_issues = {
        "high_execution_time_operators_count": len(high_execution_operators),
        "high_execution_time_operators_json": high_execution_json,
        "exploding_joins_count": len(exploding_joins),
        "exploding_joins_json": exploding_joins_json,
        "operators_with_spilling_count": len(operators_with_spilling),
        "operators_with_spilling_json": spilling_operators_json,
        "external_function_operators_count": 0,
        "external_function_operators_json": [],  # TODO: Add external function tracking
        "total_operators": len(df)
    }
    summary["performance_issues"] = performance_issues
    
    # Add external function summary (placeholder for now)
    summary["external_function_summary"] = {
        "total_calls": 0,
        "total_errors": 0,
        "success_rate_percentage": None,
        "operators_using_external_functions": 0
    }
    
    return summary

def main(session, query_id: str, output_format: str = 'pretty') -> str:
    """
    Main handler function for fetching query data.
    
    Args:
        session: Snowflake session object
        query_id: The query ID to analyze (UUID format)
        output_format: 'pretty' for readable or 'minified' for single line
        
    Returns:
        JSON string with operators as JSON string array to avoid line truncation
    """
    try:
        # Validate query ID format
        try:
            uuid.UUID(query_id)
        except ValueError:
            return json.dumps({
                "status": "error",
                "error": "Invalid Query ID format. Please provide a valid UUID.",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        
        # Get operator statistics
        query = f"SELECT * FROM TABLE(GET_QUERY_OPERATOR_STATS('{query_id}'))"
        df = session.sql(query).to_pandas()
        
        if df.empty:
            return json.dumps({
                "status": "error",
                "error": "No operator statistics found for the provided Query ID.",
                "query_id": query_id,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        
        # Calculate summary metrics from raw data
        # Note: We pass None for operator_stats_list since we're using the raw df
        summary_metrics = calculate_summary_metrics(None, df)
        
        # Process each operator into a condensed JSON string
        operator_json_strings = []
        for _, row in df.iterrows():
            operator_id = row['OPERATOR_ID']
            condensed_op = condense_operator_to_essentials(row, operator_id)
            
            # Convert to JSON string (minified)
            op_json_str = json.dumps(condensed_op, separators=(',', ':'))
            operator_json_strings.append(op_json_str)
        
        # Build final response with operators as JSON strings
        response = {
            "status": "success",
            "query_id": query_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "summary_metrics": summary_metrics,
            "operators_json_lines": operator_json_strings,
            "parse_instructions": "Each element in operators_json_lines is a JSON string. Parse with json.loads() to get operator dict."
        }
        
        # Return based on format preference
        if output_format == 'minified':
            return json.dumps(response, separators=(',', ':'))
        else:
            return json.dumps(response, indent=2)
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        
        return json.dumps({
            "status": "error",
            "error": str(e),
            "details": error_details.replace('\n', ' | '),
            "query_id": query_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
$$;

-- Example usage:
-- Pretty format (readable, ~70 lines):
CALL SNOWFLAKE_INTELLIGENCE.QUERY_DEMO.QUERY_DATA_FETCHER('01be7f13-0206-4552-000d-f46b00202422', 'pretty');

-- Minified format (single line):
-- CALL SNOWFLAKE_INTELLIGENCE.QUERY_DEMO.QUERY_DATA_FETCHER_V3('01be7631-0206-41f2-000d-f46b001e25e6', 'minified');