/*
===============================================================================
QUERY HISTORY TOOL - SNOWFLAKE INTELLIGENCE (PYTHON VERSION)
===============================================================================

PURPOSE:
Python-based stored procedure for retrieving query execution details from 
Snowflake's query history. This tool enables agents and analysts to access 
specific query information for performance analysis and optimization.

KEY FEATURES:
- Retrieves query details by query_id from ACCOUNT_USAGE
- Returns essential execution metadata in JSON format
- Handles errors gracefully with clear messages
- Integrates with Query Performance Analyzer for comprehensive diagnostics
- Efficient field selection to minimize ACCOUNT_USAGE impact

USAGE:
- QUERY_TEXT: Retrieve query details for a specific query_id
  - query_id: The unique identifier of the query to retrieve
  - Returns: JSON object with query text, user, role, warehouse size, and load percentage

INTEGRATION WITH QUERY PERFORMANCE ANALYZER:
Works with the Query Performance Analyzer to provide complete query context:
- Retrieve original query text for operator statistics analysis
- Identify user and role for access pattern optimization
- Analyze warehouse sizing relative to query complexity
- Track query load for concurrency analysis

NOTE:
Query history data in ACCOUNT_USAGE may have up to 45 minutes latency.
Queries older than 365 days may not be available.

===============================================================================
*/

/*
===============================================================================
QUERY_TEXT STORED PROCEDURE - PYTHON VERSION
===============================================================================
Retrieves execution details for a specific query from Snowflake's query history.

Purpose:
- Enable agents to retrieve query details for analysis
- Provide context for performance optimization recommendations
- Support query pattern analysis and workload characterization

Parameters:
- query_id: VARCHAR - The unique identifier of the query to retrieve

Returns:
JSON string containing:
- status: Success or error status
- query_id: The unique query identifier
- query_text: The actual SQL text of the query
- query_text_formatted: Cleaned single-line version of the query
- user_name: The user who executed the query
- role_name: The role used to execute the query  
- warehouse_size: The size of the warehouse used
- query_load_percent: The percentage of warehouse load
- message: Error message if query not found (only on error)

COLUMN DEFINITIONS:
-------------------
query_id (VARCHAR):
  Internal/system-generated identifier for the SQL statement. This is a unique 
  UUID that can be used to retrieve query details and operator statistics.
  Format: "01be5eaa-0206-3b99-000d-f46b001ba73a"

query_text (VARCHAR):
  The actual SQL statement text as executed. Limited to 100K characters.
  Longer SQL statements are truncated with "..." at the end. May contain
  parameter placeholders like :param_name for parameterized queries.

query_text_formatted (VARCHAR):
  A cleaned version of query_text with excessive whitespace removed and
  formatted as a single line for better readability. Comments are preserved
  but newlines and extra spaces are condensed.

user_name (VARCHAR):
  The Snowflake user account that issued the query. This is the login name
  of the user who executed the statement.

role_name (VARCHAR):
  The Snowflake role that was active in the session when the query was executed.
  Determines the permissions and access rights used for the query.

warehouse_size (VARCHAR):
  The size of the virtual warehouse when this statement executed.
  Values: X-Small, Small, Medium, Large, X-Large, 2X-Large, 3X-Large, 4X-Large
  Indicates the compute resources allocated for query execution.

query_load_percent (NUMBER):
  The approximate percentage of active compute resources in the warehouse 
  used by this query execution. Range: 0.0 to 100.0
  Helps identify queries that consumed significant warehouse resources.
  High values (>80%) indicate the query used most of the warehouse capacity.

Performance Considerations:
- Queries ACCOUNT_USAGE which may have higher latency
- Limited to single query retrieval to minimize impact
- Returns only essential fields for efficiency

Security Notes:
- Function inherits caller's privileges
- Requires SELECT privilege on SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
- Query text may contain sensitive information - handle appropriately
*/

USE SCHEMA SNOWFLAKE_INTELLIGENCE.TOOLS;

-- Drop the existing function if it exists (to replace with procedure)
DROP FUNCTION IF EXISTS QUERY_TEXT(VARCHAR);

CREATE OR REPLACE PROCEDURE QUERY_TEXT(query_id VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_query_text'
COMMENT = 'Retrieves query execution details from Snowflake query history for a specific query_id'
AS
$$
import json

def get_query_text(session, query_id):
    """
    Retrieves query execution details for a specific query_id.
    
    Args:
        session: Snowflake session object
        query_id: The unique identifier of the query to retrieve (UUID format)
        
    Returns:
        JSON string with query details containing:
        
        - query_id: Internal/system-generated identifier for the SQL statement. 
                   Unique UUID format like "01be5eaa-0206-3b99-000d-f46b001ba73a"
        
        - query_text: The actual SQL statement text as executed. Limited to 100K chars.
                     May contain parameter placeholders like :param_name for 
                     parameterized queries. Longer statements truncated with "..."
        
        - query_text_formatted: Cleaned single-line version with excessive whitespace 
                               removed for better readability
        
        - user_name: The Snowflake user account that issued the query
        
        - role_name: The Snowflake role active when the query was executed.
                    Determines permissions and access rights used
        
        - warehouse_size: Size of the virtual warehouse (X-Small, Small, Medium, 
                         Large, X-Large, 2X-Large, 3X-Large, 4X-Large)
        
        - query_load_percent: Percentage of active compute resources used by this 
                             query (0.0-100.0). High values (>80%) indicate heavy 
                             warehouse utilization
        
        - status: "success" or "error"
        - message: Error details if query not found
    """
    try:
        # Validate input
        if not query_id or not query_id.strip():
            return json.dumps({
                "status": "error",
                "message": "Query ID cannot be empty"
            })
        
        # Clean the query_id (remove any extra quotes or spaces)
        query_id = query_id.strip().strip("'\"")
        
        # Query the ACCOUNT_USAGE.QUERY_HISTORY view - only get the 6 requested fields
        query = f"""
        SELECT 
            QUERY_ID,
            QUERY_TEXT,
            USER_NAME,
            ROLE_NAME,
            WAREHOUSE_SIZE,
            QUERY_LOAD_PERCENT
        FROM SNOWFLAKE_INTELLIGENCE.PUBLIC.QUERY_HISTORY
        WHERE QUERY_ID = '{query_id}'
          AND QUERY_ID IS NOT NULL
        ORDER BY START_TIME DESC
        LIMIT 1
        """
        
        # Execute the query
        result = session.sql(query).collect()
        
        # Check if we found the query
        if not result:
            return json.dumps({
                "status": "error",
                "message": f"Query ID '{query_id}' not found in query history. Note: ACCOUNT_USAGE may have up to 45 minutes latency."
            })
        
        # Extract the row
        row = result[0]
        
        # Get the query text and format it for readability
        query_text = str(row['QUERY_TEXT']) if row['QUERY_TEXT'] else None
        
        # Create a more readable version by cleaning up formatting
        if query_text:
            import re
            # Remove excessive whitespace while preserving structure
            formatted_text = re.sub(r'\n\s+', ' ', query_text)  # Replace newlines + spaces with single space
            formatted_text = re.sub(r'\s+', ' ', formatted_text)  # Replace multiple spaces with single space
            formatted_text = formatted_text.strip()
            
            # Make SQL keywords more visible
            sql_keywords = ['SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 
                          'DELETE', 'CREATE', 'DROP', 'ALTER', 'WITH', 'AS', 'JOIN', 'LEFT', 'RIGHT', 
                          'INNER', 'OUTER', 'ON', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN',
                          'LIKE', 'ORDER BY', 'GROUP BY', 'HAVING', 'UNION', 'CURRENT_TIMESTAMP']
            
            # Add the formatted version
            query_text_formatted = formatted_text
        else:
            query_text_formatted = None
        
        # Build the response with ONLY the 6 requested fields plus formatted version
        response = {
            "status": "success",
            "query_id": str(row['QUERY_ID']),
            "query_text_formatted": query_text_formatted,
            "user_name": str(row['USER_NAME']) if row['USER_NAME'] else None,
            "role_name": str(row['ROLE_NAME']) if row['ROLE_NAME'] else None,
            "warehouse_size": str(row['WAREHOUSE_SIZE']) if row['WAREHOUSE_SIZE'] else None,
            "query_load_percent": float(row['QUERY_LOAD_PERCENT']) if row['QUERY_LOAD_PERCENT'] is not None else None
        }
        
        return json.dumps(response, indent=2)
        
    except Exception as e:
        return json.dumps({
            "status": "error",
            "message": f"Error retrieving query: {str(e)}"
        })
$$;

/*
===============================================================================
EXAMPLE USAGE AND TESTING
===============================================================================
*/

-- Test 1: Retrieve details for a specific query
-- Replace with an actual query_id from your environment
CALL QUERY_TEXT('01be64cd-0206-3d45-000d-f46b001c04ae');

/*
===============================================================================
INTEGRATION EXAMPLE WITH QUERY PERFORMANCE ANALYZER
===============================================================================
*/

-- Example: Analyze a query's performance after retrieving its details
/*
-- Step 1: Get query details
CALL QUERY_TEXT('your-query-id-here');

-- Step 2: Parse the JSON result to get the query text
-- (The result will be a JSON string with all query details)

-- Step 3: Analyze its performance
CALL SNOWFLAKE_INTELLIGENCE.QUERY_DEMO.QUERY_PERFORMANCE('your-query-id-here');
*/

/*
===============================================================================
SAMPLE OUTPUT
===============================================================================

Successful query retrieval:
{
  "status": "success",
  "query_id": "01be5eaa-0206-3b99-000d-f46b001ba73a",
  "query_text": "INSERT INTO column_match_analysis_v2 VALUES (:col_a_table, :col_a_name, :col_a_type, ...)",
  "user_name": "TGORDONJR",
  "role_name": "SYSADMIN",
  "warehouse_size": "Medium",
  "query_load_percent": 25.0
}

Query not found:
{
  "status": "error",
  "message": "Query ID 'invalid-id' not found in query history. Note: ACCOUNT_USAGE may have up to 45 minutes latency."
}

===============================================================================
*/

/*
===============================================================================
NOTES AND LIMITATIONS
===============================================================================

1. LATENCY: ACCOUNT_USAGE views have up to 45 minutes latency
   - For real-time data, consider using INFORMATION_SCHEMA views
   - Note that INFORMATION_SCHEMA has retention limits (typically 7 days)

2. RETENTION: Query history is retained for 365 days
   - Older queries will not be available
   - Consider archiving important query metadata if needed

3. PERMISSIONS: Requires appropriate privileges
   - SELECT on SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
   - May need ACCOUNTADMIN or SECURITY_ADMIN role for initial setup

4. PERFORMANCE: Be mindful of ACCOUNT_USAGE impact
   - These views can be resource-intensive
   - Use specific filters (query_id) to limit scope
   - Consider caching results for frequently accessed queries

5. TRUNCATION: Large query texts may be truncated
   - QUERY_TEXT field has a maximum length
   - Very long queries may show "..." at the end

6. PRIVACY: Query text may contain sensitive information
   - Implement appropriate access controls
   - Consider masking or filtering sensitive data
   - Audit usage of these functions regularly

7. JSON OUTPUT: The procedure returns a JSON string
   - Parse the JSON in your application to access individual fields
   - All fields are included even if NULL for consistency

===============================================================================
*/