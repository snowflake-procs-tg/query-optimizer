-- ===============================================================================
-- TOOL 3: ANALYSIS GUIDANCE
-- Provides analysis guidance and performance thresholds for query optimization
-- ===============================================================================

CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.QUERY_DEMO;

USE SCHEMA SNOWFLAKE_INTELLIGENCE.QUERY_DEMO;

CREATE OR REPLACE PROCEDURE ANALYSIS_GUIDANCE()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS $$
import json

def get_analysis_guidance() -> str:
    """Return guidance for Snowflake Intelligence on how to analyze the data."""
    return """
    WORKFLOW DECISION TREE:

    IF operator_type = "CreateTableAsSelect" AND input_expressions EXISTS:
      → TASK 1: Analyze input_expressions for understanding query
      → TASK 2: Analyze scattered expressions across operators
      → TASK 3: Compare both expressions to the submitted query
      → TASK 4: Look for redundant calculations, inefficient JSON parsing, repeated patterns etc
      → TASK 5: Provide CTAS restructuring, expression consolidation, semi-structured optimization etc

    IF operator_type = "Insert" AND input_expressions EXISTS:
      → TASK 1: Analyze input_expressions for data transformation efficiency
      → TASK 2: Analyze scattered expressions across operators
      → TASK 3: Compare both expressions to the submitted query
      → TASK 4: Look for complex calculations, data type conversions, business logic patterns etc
      → TASK 5: Provide staging table recommendations, batch optimization etc

    IF operator_type = "Result" "SELECT" AND expressions EXISTS:
      → TASK 1: Analyze scattered expressions across operators
      → TASK 2: Compare expressions to the submitted query
      → TASK 3: Look for join efficiency, aggregation patterns, sort optimization etc
      → TASK 4: Provide query structure recommendations

    For all other use cases, provide a general analysis if applicable data is available.

    ═══════════════════════════════════════════════════════════════════════════

    PERFORMANCE THRESHOLDS:

    EXECUTION TIME:
    • GREAT: Operators consuming <5% of total execution time
    • GOOD: Operators consuming 5-15% of total execution time
    • POOR: Operators consuming 15-30% of total execution time
    • CRITICAL: Operators consuming >30% of total execution time

    TABLE SCAN EFFICIENCY:
    • GREAT: Pruning efficiency >80% AND cache hit rate >90%
    • GOOD:  Pruning efficiency 60-80% OR cache hit rate 70-90%
    • POOR:  Pruning efficiency 30-60% OR cache hit rate 40-70%
    • CRITICAL: Pruning efficiency <30% OR cache hit rate <40%

    JOIN PERFORMANCE:
    • GREAT: Row multiplication factor <1.2 (minimal row expansion)
    • GOOD: Row multiplication factor 1.2-1.5
    • POOR: Row multiplication factor 1.5-2.0
    • CRITICAL: Row multiplication factor >2.0 (join explosion)

    I/O OPERATIONS:
    • GREAT: <100 MB scanned with >90% cache hit
    • GOOD: 100 MB - 1 GB scanned OR 70-90% cache hit
    • POOR: 1-10 GB scanned OR 40-70% cache hit
    • CRITICAL: >10 GB scanned OR <40% cache hit

    SPILLING:
    • GREAT: No spilling detected
    • GOOD: Local spilling only (<100 MB)
    • POOR: Local spilling (100 MB - 1 GB)
    • CRITICAL: Remote spilling OR >1 GB spilled

    EXTERNAL FUNCTIONS:
    • GREAT: <10ms average latency, 100% success rate
    • GOOD: 10-50ms average latency, >95% success rate
    • POOR: 50-200ms average latency, 90-95% success rate
    • CRITICAL: >200ms average latency OR <90% success rate

    ═══════════════════════════════════════════════════════════════════════════

    OPERATOR-SPECIFIC FOCUS AREAS:

    TableScan Operators:
    - Evaluate pruning efficiency and cache utilization
    - Check columns scanned vs total columns available
    - Recommend clustering keys if pruning is poor

    Join Operators:
    - Assess join type appropriateness (INNER vs OUTER)
    - Check for missing or inefficient join conditions
    - Identify exploding joins that multiply rows unnecessarily

    Filter Operators:
    - Calculate selectivity (output_rows/input_rows)
    - Suggest filter pushdown opportunities

    Sort/SortWithLimit Operators:
    - Check if sorting is necessary for the query
    - Evaluate LIMIT usage efficiency

    Aggregate/GroupingSets Operators:
    - Review grouping key cardinality
    - Check for complex aggregate functions

    CreateTableAsSelect Operators:
    - Focus on input_expressions complexity
    - Look for expression consolidation opportunities
    - Check for redundant calculations

    Insert/Update/Delete Operators:
    - Monitor rows affected vs rows scanned
    - Check for batch size optimization opportunities

    SecureView Operators:
    - Note high processing overhead is common
    - Check local_disk_io and remote_disk_io components

    ═══════════════════════════════════════════════════════════════════════════

    REPORTING GUIDELINES:

    1. Lead with Performance Classification:
       - Start with overall query performance (GREAT/GOOD/POOR/CRITICAL)
       - Highlight the most critical bottlenecks first

    2. Business Impact Translation:
       - "Poor pruning" → "Scanning unnecessary data partitions"
       - "Low cache hit" → "Re-reading data from storage"
       - "Join explosion" → "Creating excessive intermediate results"
       - "Spilling" → "Running out of memory, using disk storage"

    3. Actionable Recommendations Priority:
       IMMEDIATE ACTION (for CRITICAL issues):
       - Add clustering keys for tables with <30% pruning
       - Rewrite queries with exploding joins
       - Increase warehouse size for memory pressure

       OPTIMIZE SOON (for POOR performance):
       - Review and optimize join conditions
       - Consider materialized views for large scans
       - Optimize filter placement

       MONITOR (for GOOD performance):
       - Track performance trends
       - Consider result caching strategies
       - Review query patterns for optimization

    4. Quick Wins:
       - Identify simple changes with high impact
       - Suggest query rewrites that reduce data movement
       - Recommend appropriate warehouse sizing

    ═══════════════════════════════════════════════════════════════════════════

    SUMMARY METRICS INTERPRETATION:

    Use the provided summary_metrics to give a high-level overview:
    - Total data scanned and written
    - Overall cache effectiveness
    - Average pruning efficiency across table scans
    - Identified performance bottlenecks
    - Query type and DML impact

    Always provide specific, measurable improvements when making recommendations.

    If a query is submitted with request, ask the user if you can query the source tables to understand structure.
    If they say yes, you will use the appropriate tools to do the following:
      1. Query the source table(s) with a limit of 3
      2. Analyze data structure and think about ways to optimize trouble query
      3. Produce 2 examples of how to optimize query but be concise with the code. 
         It should be a formal/non-technical explanation of what to do.
      4. Only produce the full query if the user asks for it.
    """

def main(session) -> str:
    """
    Main handler function that returns analysis guidance.
    
    Args:
        session: Snowflake session object
        
    Returns:
        JSON string containing analysis guidance
    """
    return json.dumps({
        "status": "success",
        "analysis_guidance": get_analysis_guidance()
    }, indent=2)
$$;

-- Example usage:
CALL SNOWFLAKE_INTELLIGENCE.QUERY_DEMO.ANALYSIS_GUIDANCE();