/*
===============================================================================
DML PROCEDURES - PYTHON VERSION FOR SNOWFLAKE INTELLIGENCE
===============================================================================

PURPOSE:
Python-based stored procedures that can handle query results and return
formatted data for Snowflake Intelligence tools. These procedures can
process and return actual query results as formatted strings.

KEY FEATURES:
- Executes SELECT, INSERT, UPDATE, DELETE, and MERGE statements
- Multiple output formats: table, JSON, or summary
- Automatic column width calculation (max 50 chars per column)
- Row limiting (displays first 100 rows for large result sets)
- Clear validation and error messages

USAGE:
- EXECUTE_DML: Execute any DML statement with flexible output formatting
  - dml_statement: The DML command to execute
  - output_format: 'table' (default), 'json', or 'summary'

INTEGRATION WITH QUERY PERFORMANCE ANALYZER:
After executing DML statements, you can analyze their performance using
the Query Performance Analyzer to understand:
- Data scanning efficiency (pruning, cache usage)
- Join performance and row multiplication
- Memory usage and spilling
- Execution time distribution across operators

This helps optimize queries based on actual performance metrics rather
than guesswork.

===============================================================================
*/


/*
===============================================================================
EXECUTE DML - PYTHON VERSION
===============================================================================
Executes any DML statement and returns appropriate results.

Supported Operations:
- SELECT: Returns formatted query results
  - Table format: Displays data in aligned columns
  - JSON format: Returns structured JSON with metadata
  - Summary format: Returns row count only
- INSERT: Executes insertion and confirms success
- UPDATE: Modifies data and confirms success
- DELETE: Removes data and confirms success
- MERGE: Performs upsert operations and confirms success

Output Formats:
- 'table': Formatted table with columns and separators (default)
  - Auto-calculates column widths (max 50 chars)
  - Limits display to 100 rows for readability
  - Shows count of additional rows if truncated
- 'json': Structured JSON with statement, type, row count, and data
- 'summary': Simple success message with row count

Performance Considerations:
- For large result sets, consider using LIMIT in your SELECT
- Use the Query Performance Analyzer after execution to identify bottlenecks
- Check final_output_rows in summary_metrics for actual rows returned
*/
USE SCHEMA SNOWFLAKE_INTELLIGENCE.TOOLS;
CREATE OR REPLACE PROCEDURE EXECUTE_DML(
    dml_statement STRING,
    output_format STRING DEFAULT 'table'  -- 'table', 'json', 'summary'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'execute_dml'
AS
$$
import json

def execute_dml(session, dml_statement, output_format):
    try:
        # Validate input
        if not dml_statement or not dml_statement.strip():
            return "Error: DML statement cannot be empty"
        
        # Get statement type
        statement_type = dml_statement.strip().split()[0].upper()
        
        # Validate it's a DML statement
        if statement_type not in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE']:
            return f"Error: Only DML statements are allowed. Got: {statement_type}"
        
        # Execute the statement
        result = session.sql(dml_statement)
        
        if statement_type == 'SELECT':
            # For SELECT, collect and format results
            df = result.collect()
            columns = result.schema.names
            
            if output_format == 'json':
                rows = []
                for row in df:
                    row_dict = {columns[i]: str(row[i]) for i in range(len(columns))}
                    rows.append(row_dict)
                return json.dumps({
                    "statement": dml_statement,
                    "type": "SELECT",
                    "row_count": len(df),
                    "data": rows
                }, indent=2)
            
            elif output_format == 'summary':
                return f"SELECT executed successfully. Retrieved {len(df)} rows."
            
            else:  # table format
                output = f"Statement: {dml_statement}\n"
                output += f"Rows returned: {len(df)}\n\n"
                
                if len(df) == 0:
                    return output + "No data found."
                
                # Format as table (similar to QUERY_DATA_PY)
                col_widths = []
                for i, col in enumerate(columns):
                    max_width = len(col)
                    for row in df[:50]:
                        val_len = len(str(row[i]))
                        if val_len > max_width:
                            max_width = val_len
                    col_widths.append(min(max_width, 50))
                
                header = '| '
                separator = '|-'
                for i, col in enumerate(columns):
                    header += col.ljust(col_widths[i]) + ' | '
                    separator += '-' * col_widths[i] + '-|-'
                
                output += header + '\n'
                output += separator + '\n'
                
                for row in df[:100]:  # Limit display to 100 rows
                    row_str = '| '
                    for i, val in enumerate(row):
                        val_str = str(val)[:col_widths[i]]
                        row_str += val_str.ljust(col_widths[i]) + ' | '
                    output += row_str + '\n'
                
                if len(df) > 100:
                    output += f"\n... ({len(df) - 100} more rows)"
                
                return output
        else:
            # For other DML operations, get affected rows
            result.collect()  # Execute the statement
            return f"{statement_type} executed successfully. Use QUERY_HISTORY to check affected rows."
            
    except Exception as e:
        return f"Error executing DML: {str(e)}"
$$;

/*
===============================================================================
EXAMPLE USAGE
===============================================================================
*/

-- Example 1: Simple SELECT query
CALL EXECUTE_DML('SELECT * FROM my_table LIMIT 10');

-- Example 2: Insert data
CALL EXECUTE_DML('INSERT INTO my_table (id, name) VALUES (1, ''John''), (2, ''Jane'')');

-- Example 3: Get row count as summary
CALL EXECUTE_DML('SELECT COUNT(*) FROM my_table', 'summary');

/*
===============================================================================
BENEFITS OF PYTHON VERSION
===============================================================================

1. ACTUAL RESULTS: Returns formatted query results directly
   - No need for RESULT_SCAN
   - Immediate access to data
   - Formatted for readability

2. FLEXIBLE OUTPUT: Support for multiple formats
   - Table: Human-readable with proper alignment
   - JSON: Machine-parseable with metadata
   - Summary: Quick row count confirmation

3. ERROR HANDLING: Better exception handling with detailed messages
   - Validates statement type before execution
   - Clear error messages with context
   - Safe handling of NULL values

4. DATA PROCESSING: Can manipulate and format data before returning
   - Automatic column width calculation
   - Row limiting for large datasets
   - String truncation for wide columns

5. PERFORMANCE INTEGRATION:
   - Results can be correlated with Query Performance Analyzer
   - Helps identify why queries return unexpected row counts
   - Supports data-driven query optimization

These procedures are ideal for Snowflake Intelligence tools where you need
programmatic access to query results with formatted output, combined with
performance analysis capabilities.
===============================================================================
*/