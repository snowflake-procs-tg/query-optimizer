/*
===============================================================================
DDL PROCEDURES - PYTHON VERSION FOR SNOWFLAKE INTELLIGENCE
===============================================================================

PURPOSE:
Python-based stored procedures for Data Definition Language (DDL) operations.
These procedures manage database structure including creating, altering, and
dropping database objects like databases, schemas, tables, and views.

KEY FEATURES:
- Validates DDL statements before execution to prevent errors
- Formats SHOW/DESCRIBE results as readable tables with proper column alignment
- Handles all common DDL operations (CREATE, ALTER, DROP, TRUNCATE, GRANT, REVOKE)
- Returns clear success/error messages for troubleshooting
- Limits display width to 40 characters per column for readability

USAGE:
- EXECUTE_DDL: Execute any DDL statement with optional result formatting
  - ddl_statement: The DDL command to execute
  - show_results: Display formatted results for SHOW/DESCRIBE commands (default: TRUE)

INTEGRATION WITH QUERY PERFORMANCE ANALYZER:
Works seamlessly with the Query Performance Analyzer to help optimize
database structures based on performance metrics. For example:
- Create indexes based on pruning efficiency recommendations
- Alter table clustering keys when scan patterns are inefficient
- Create materialized views for frequently accessed data

===============================================================================
*/

/*
===============================================================================
EXECUTE DDL - PYTHON VERSION
===============================================================================
Executes any DDL statement and returns formatted results.

Supported Operations:
- CREATE: Tables, views, schemas, databases, warehouses, stages
- ALTER: Modify existing objects, add/drop columns, change data types
- DROP: Remove objects with cascade support
- TRUNCATE: Clear table data while preserving structure
- GRANT/REVOKE: Manage permissions and access control
- SHOW: List objects with formatted tabular output
- DESCRIBE/DESC: Show object structure and metadata

Returns:
- Success message for DDL operations with the executed statement
- Formatted table for SHOW/DESCRIBE commands (max 40 chars per column)
- Detailed error messages if operation fails, including the problematic statement

Performance Note:
DDL operations may show high "other" category in execution time breakdown
when analyzed with the Query Performance Analyzer, which is normal for
structure-modifying operations.
*/

USE SCHEMA SNOWFLAKE_INTELLIGENCE.TOOLS;
CREATE OR REPLACE PROCEDURE EXECUTE_DDL(
    ddl_statement STRING,
    output_format STRING DEFAULT 'pretty'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import json
from datetime import datetime
from typing import Dict, List, Any, Optional

def format_table_output(df, columns: List[str], max_rows: int = 100) -> str:
    """Format dataframe as ASCII table with proper column alignment."""
    if len(df) == 0:
        return "No objects found."
    
    # Calculate column widths
    col_widths = []
    for i, col in enumerate(columns):
        max_width = len(col)
        for row in df[:min(max_rows, len(df))]:
            val_len = len(str(row[i]) if row[i] is not None else 'NULL')
            if val_len > max_width:
                max_width = val_len
        col_widths.append(min(max_width, 40))
    
    # Build table
    output = ""
    
    # Header
    header = '| '
    separator = '|-'
    for i, col in enumerate(columns):
        header += col[:col_widths[i]].ljust(col_widths[i]) + ' | '
        separator += '-' * col_widths[i] + '-|-'
    
    output += header + '\n'
    output += separator + '\n'
    
    # Data rows
    for idx, row in enumerate(df):
        if idx >= max_rows:
            output += f"\n... {len(df) - max_rows} more rows ..."
            break
        row_str = '| '
        for i, val in enumerate(row):
            val_str = str(val) if val is not None else 'NULL'
            val_str = val_str[:col_widths[i]]
            row_str += val_str.ljust(col_widths[i]) + ' | '
        output += row_str + '\n'
    
    return output

def process_show_results(df, columns: List[str]) -> List[Dict[str, Any]]:
    """Convert SHOW/DESCRIBE results to JSON-friendly format."""
    results = []
    for row in df:
        row_dict = {}
        for i, col in enumerate(columns):
            val = row[i]
            # Convert to JSON-serializable types
            if val is None:
                row_dict[col] = None
            elif isinstance(val, (int, float, bool)):
                row_dict[col] = val
            else:
                row_dict[col] = str(val)
        results.append(row_dict)
    return results

def main(session, ddl_statement: str, output_format: str = 'pretty') -> str:
    """
    Main handler function for executing DDL statements.
    
    Args:
        session: Snowflake session object
        ddl_statement: The DDL statement to execute
        output_format: 'pretty' for readable JSON, 'minified' for compact JSON, 'table' for ASCII table
        
    Returns:
        JSON string with execution results or formatted table for 'table' format
    """
    try:
        # Validate input
        if not ddl_statement or not ddl_statement.strip():
            return json.dumps({
                "status": "error",
                "error": "DDL statement cannot be empty",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }, indent=2 if output_format == 'pretty' else None)
        
        # Get statement type
        statement_parts = ddl_statement.strip().split()
        if not statement_parts:
            return json.dumps({
                "status": "error",
                "error": "Invalid DDL statement",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }, indent=2 if output_format == 'pretty' else None)
        
        statement_type = statement_parts[0].upper()
        
        # Common DDL keywords
        ddl_keywords = ['CREATE', 'ALTER', 'DROP', 'TRUNCATE', 'GRANT', 'REVOKE', 
                       'SHOW', 'DESCRIBE', 'DESC', 'USE', 'COMMENT']
        
        if statement_type not in ddl_keywords:
            return json.dumps({
                "status": "error",
                "error": f"Expected DDL statement, got: {statement_type}",
                "statement": ddl_statement,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }, indent=2 if output_format == 'pretty' else None)
        
        # Execute the DDL
        result = session.sql(ddl_statement)
        
        # Handle SHOW and DESCRIBE commands differently
        if statement_type in ['SHOW', 'DESCRIBE', 'DESC']:
            df = result.collect()
            columns = result.schema.names
            
            # For table format, return ASCII table directly
            if output_format == 'table':
                output = f"Statement: {ddl_statement}\n"
                output += f"Results: {len(df)} rows\n\n"
                output += format_table_output(df, columns)
                return output
            
            # For JSON formats, build structured response
            response = {
                "status": "success",
                "statement_type": statement_type,
                "statement": ddl_statement,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "result": {
                    "row_count": len(df),
                    "columns": columns,
                    "data": process_show_results(df, columns)
                }
            }
            
            # Return based on format preference
            if output_format == 'minified':
                return json.dumps(response, separators=(',', ':'))
            else:
                return json.dumps(response, indent=2)
            
        else:
            # For other DDL, execute and return success
            result.collect()
            
            response = {
                "status": "success",
                "statement_type": statement_type,
                "statement": ddl_statement,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "message": f"DDL executed successfully"
            }
            
            # Add additional info for specific operations
            if statement_type == 'CREATE':
                if 'TABLE' in ddl_statement.upper():
                    response["object_type"] = "TABLE"
                elif 'VIEW' in ddl_statement.upper():
                    response["object_type"] = "VIEW"
                elif 'SCHEMA' in ddl_statement.upper():
                    response["object_type"] = "SCHEMA"
                elif 'DATABASE' in ddl_statement.upper():
                    response["object_type"] = "DATABASE"
            elif statement_type == 'DROP':
                response["operation"] = "DROP"
            elif statement_type == 'ALTER':
                response["operation"] = "ALTER"
            
            # Return based on format preference
            if output_format == 'minified':
                return json.dumps(response, separators=(',', ':'))
            else:
                return json.dumps(response, indent=2)
                
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        
        response = {
            "status": "error",
            "error": str(e),
            "details": error_details.replace('\n', ' | '),
            "statement": ddl_statement,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Return based on format preference
        if output_format == 'minified':
            return json.dumps(response, separators=(',', ':'))
        else:
            return json.dumps(response, indent=2)
$$;

/*
===============================================================================
EXAMPLE USAGE
===============================================================================
*/

-- Example 1: Show all databases
CALL EXECUTE_DDL('SHOW DATABASES');

-- Example 2: Create a simple table
CALL EXECUTE_DDL('CREATE TABLE test_table (id INT, name VARCHAR(100))');

-- Example 3: Describe table structure
CALL EXECUTE_DDL('DESCRIBE TABLE test_table');
/*
===============================================================================
BENEFITS OF PYTHON DDL PROCEDURES
===============================================================================

1. FORMATTED OUTPUT: Returns readable, formatted results for SHOW commands
   - Automatic column width calculation (max 40 chars)
   - Proper alignment and separators
   - NULL value handling

2. VALIDATION: Better input validation and error messages
   - Checks for empty statements
   - Validates DDL keywords before execution
   - Clear error reporting with statement context

3. FLEXIBLE PARAMETERS: Support for optional parameters and defaults
   - show_results flag for controlling output format
   - Handles both simple and complex DDL operations

4. INTEGRATION WITH PERFORMANCE ANALYSIS:
   - DDL changes can be tracked and correlated with query performance
   - Helps validate that structural changes improve query efficiency
   - Supports performance-driven schema optimization

These procedures provide a robust interface for DDL operations that integrates
with the Query Performance Analyzer for comprehensive database optimization.
===============================================================================
*/