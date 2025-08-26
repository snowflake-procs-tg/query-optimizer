-- ===============================================================================
-- TOOL: QUERY OPTIMIZATION WORKFLOW - CONTROLLED METADATA VERSION
-- Quick optimization with limited, controlled metadata gathering
-- Version: 1.0.0
-- Created: 2025-01-18
-- ===============================================================================

CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.QUERY_DEMO;

USE SCHEMA SNOWFLAKE_INTELLIGENCE.QUERY_DEMO;

CREATE OR REPLACE PROCEDURE QUERY_OPTIMIZE_WORKFLOW()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS $$
import json

def get_optimization_workflow() -> dict:
    """Return controlled query optimization workflow for Snowflake Intelligence."""
    return {
        "prompt_id": "query_optimization_simple_v1",
        "prompt_type": "query_optimization",
        "metadata": {
            "version": "1.0.0",
            "created": "2025-01-18",
            "workflow_name": "Simple Query Optimizer",
            "focus": "Quick optimization without extensive metadata gathering"
        },
        "star_structure": {
            "situation": {
                "context": "You need to optimize a Snowflake SQL query efficiently",
                "approach": "Analyze query structure with limited metadata gathering",
                "critical_requirements": {
                    "platform": "⚠️ SNOWFLAKE SQL ONLY ⚠️",
                    "results": "Must return identical results",
                    "ddl_limit": "⚠️ ONE DDL execution per table MAXIMUM ⚠️"
                }
            },
            "task": {
                "primary_objective": "Optimize query with controlled metadata gathering",
                "simple_workflow": [
                    "0. Identify issues and GET USER PERMISSION",
                    "1. Identify tables in query",
                    "2. Execute DESCRIBE ONCE per table",
                    "3. Analyze query patterns",
                    "4. Apply optimizations",
                    "5. Present optimized query"
                ],
                "loop_prevention": [
                    "⚠️ NEVER repeat DDL for same table ⚠️",
                    "Track which tables already described",
                    "Stop if agent tries to re-describe",
                    "No iterative refinement cycles"
                ]
            },
            "action": {
                "step_0_initial_response": {
                    "CRITICAL": "⚠️ DO NOT IMMEDIATELY OPTIMIZE ⚠️",
                    "action": "When user submits query, FIRST identify issues",
                    "workflow": [
                        "1. Scan query for performance anti-patterns",
                        "2. List specific issues with line references",
                        "3. Present findings to user",
                        "4. ASK for permission to proceed"
                    ],
                    "response_format": {
                        "greeting": "I've identified the following performance issues in your query:",
                        "issues_list": [
                            "1. Line X-Y: [Issue description]",
                            "2. Line A-B: [Issue description]"
                        ],
                        "ask_permission": "Would you like me to proceed with optimization? This will involve:",
                        "preview": [
                            "- Analyzing table metadata (DESCRIBE)",
                            "- Identifying optimization patterns",
                            "- Generating an optimized Snowflake query"
                        ]
                    },
                    "wait_for": "⚠️ User confirmation REQUIRED before step_1 ⚠️"
                },
                "step_1_identify_tables": {
                    "prerequisite": "Only execute AFTER user confirms",
                    "action": "Extract all table names from query",
                    "output": "List of unique tables",
                    "tracking": "Create list to track DDL execution"
                },
                "step_2_gather_metadata": {
                    "action": "Execute DESCRIBE for each table ONCE",
                    "ddl_execution": {
                        "rule": "⚠️ ONE DESCRIBE per table ONLY ⚠️",
                        "command": "DESCRIBE TABLE <table_name>",
                        "tracking": "Mark table as 'described' after execution",
                        "prevention": "If table already described, SKIP"
                    },
                    "collect": [
                        "Column names and types",
                        "Primary keys",
                        "Indexes if available"
                    ],
                    "stop_condition": "All tables described ONCE"
                },
                "step_3_analyze_patterns": {
                    "action": "Analyze query issues using metadata",
                    "look_for": [
                        "SELECT * usage",
                        "Subqueries in SELECT clause",
                        "Functions on JOIN/WHERE columns",
                        "Missing JOIN conditions",
                        "OR conditions preventing optimization",
                        "DISTINCT overuse",
                        "Cartesian products"
                    ],
                    "output": "List of identified issues"
                },
                "step_4_apply_patterns": {
                    "action": "Apply standard Snowflake optimizations",
                    "common_fixes": {
                        "subquery_in_select": "Convert to LEFT JOIN",
                        "select_star": "Specify only needed columns",
                        "function_on_column": "Rewrite to be sargable",
                        "multiple_or": "Convert to IN clause or UNION",
                        "distinct_overuse": "Remove if unnecessary",
                        "missing_join": "Add proper JOIN conditions",
                        "type_mismatch": "Add explicit casting with ::"
                    },
                    "snowflake_specific": {
                        "cte_syntax": "Use proper CTE with commas",
                        "string_concat": "Use || not +",
                        "conditionals": "Use IFF not IF",
                        "window_functions": "Use QUALIFY for filtering",
                        "json_operations": "Use FLATTEN for arrays"
                    }
                },
                "step_5_present_solution": {
                    "action": "Show optimized query",
                    "format": {
                        "tables_analyzed": "List tables described",
                        "original_issues": "List problems found",
                        "optimizations_applied": "List changes made",
                        "optimized_query": "Complete rewritten query",
                        "validation": "Confirm results unchanged"
                    },
                    "no_loops": "⚠️ END HERE - Do not iterate or refine ⚠️"
                }
            },
            "result": {
                "deliverable": "Optimized Snowflake-compatible query",
                "format": [
                    "## Issues Found:",
                    "1. [Issue description]",
                    "",
                    "## Optimizations Applied:",
                    "1. [Change description]",
                    "",
                    "## Optimized Query:",
                    "```sql",
                    "[Optimized query here]",
                    "```"
                ]
            }
        },
        "snowflake_syntax_checklist": {
            "must_use": [
                "|| for string concatenation",
                ":: for type casting",
                "IFF for inline conditionals",
                "QUALIFY for window function filtering",
                "FLATTEN for JSON/array operations"
            ],
            "must_avoid": [
                "Column aliases after AS in subqueries",
                "+ for string concatenation",
                "IF function (use IFF)",
                "TOP in subqueries (use LIMIT)",
                "Trailing commas in SELECT"
            ]
        },
        "initial_response_requirement": {
            "MANDATORY": "⚠️ NEVER SKIP STEP 0 ⚠️",
            "when_user_submits_query": [
                "DO NOT immediately start optimization",
                "DO NOT execute DDL right away",
                "FIRST identify performance issues",
                "THEN ask user for permission to proceed"
            ],
            "example_interaction": [
                "User: [Submits SQL query]",
                "Agent: 'I've identified 3 performance issues:'",
                "Agent: [Lists issues with line references]",
                "Agent: 'Would you like me to proceed with optimization?'",
                "User: 'Yes'",
                "Agent: [NOW proceeds with optimization workflow]"
            ]
        },
        "loop_prevention_instructions": {
            "critical_rules": [
                "⚠️ ALWAYS START with issue identification ⚠️",
                "⚠️ WAIT for user confirmation ⚠️",
                "⚠️ ONE DESCRIBE per table MAXIMUM ⚠️",
                "⚠️ Track tables already described ⚠️",
                "⚠️ NO re-execution of DDL ⚠️",
                "⚠️ NO iterative refinement ⚠️",
                "⚠️ Complete in ONE pass ⚠️"
            ],
            "tracking_mechanism": {
                "described_tables": "Keep list: ['table1', 'table2', ...]",
                "before_describe": "Check if table in list",
                "after_describe": "Add table to list",
                "if_in_list": "SKIP - use cached metadata"
            },
            "workflow": [
                "1. Extract table names",
                "2. DESCRIBE each ONCE",
                "3. Analyze patterns",
                "4. Apply optimizations",
                "5. Present result and STOP"
            ],
            "stop_signals": [
                "All tables described",
                "Optimization complete",
                "Query presented"
            ]
        },
        "execution_instructions": {
            "keep_it_simple": [
                "ONE pass through workflow",
                "ONE DESCRIBE per table",
                "NO loops or iterations",
                "Complete and STOP"
            ],
            "time_target": "Complete in under 3 minutes"
        },
        "common_optimization_patterns": {
            "pattern_1_subquery_to_join": {
                "identify": "Subquery in SELECT clause",
                "example_before": """
                    SELECT 
                        a.id,
                        (SELECT name FROM table_b WHERE b.id = a.b_id) as name
                    FROM table_a a
                """,
                "example_after": """
                    SELECT 
                        a.id,
                        b.name
                    FROM table_a a
                    LEFT JOIN table_b b ON b.id = a.b_id
                """
            },
            "pattern_2_sargable_predicates": {
                "identify": "Functions on indexed columns",
                "example_before": "WHERE UPPER(column) = 'VALUE'",
                "example_after": "WHERE column = 'VALUE' -- or use ILIKE for case-insensitive"
            },
            "pattern_3_or_to_in": {
                "identify": "Multiple OR conditions on same column",
                "example_before": "WHERE status = 'A' OR status = 'B' OR status = 'C'",
                "example_after": "WHERE status IN ('A', 'B', 'C')"
            },
            "pattern_4_exists_vs_in": {
                "identify": "IN with subquery returning many rows",
                "example_before": "WHERE id IN (SELECT id FROM large_table WHERE ...)",
                "example_after": "WHERE EXISTS (SELECT 1 FROM large_table WHERE large_table.id = main_table.id AND ...)"
            },
            "pattern_5_cte_efficiency": {
                "identify": "Repeated subqueries",
                "example_before": """
                    SELECT * FROM (SELECT * FROM table WHERE date > '2024-01-01') t1
                    UNION ALL
                    SELECT * FROM (SELECT * FROM table WHERE date > '2024-01-01') t2
                """,
                "example_after": """
                    WITH filtered_data AS (
                        SELECT * FROM table WHERE date > '2024-01-01'
                    )
                    SELECT * FROM filtered_data
                    UNION ALL
                    SELECT * FROM filtered_data
                """
            },
            "pattern_6_qualify_for_window": {
                "identify": "Subquery with ROW_NUMBER for filtering",
                "example_before": """
                    SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) as rn
                        FROM table
                    ) WHERE rn = 1
                """,
                "example_after": """
                    SELECT *
                    FROM table
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) = 1
                """
            },
            "pattern_7_flatten_json": {
                "identify": "Complex JSON extraction",
                "example_before": "Multiple JSON_EXTRACT_PATH_TEXT calls",
                "example_after": "Use FLATTEN with LATERAL JOIN for array/object iteration"
            }
        },
        "quick_wins": {
            "immediate_fixes": [
                "Remove SELECT * - specify columns",
                "Add LIMIT when testing",
                "Remove unnecessary DISTINCT",
                "Fix data type mismatches with ::",
                "Replace COUNT(*) with COUNT(1) where appropriate",
                "Use column positions in GROUP BY for long lists"
            ],
            "snowflake_specific_wins": [
                "Use COPY INTO for bulk loads",
                "Leverage clustering keys for large tables",
                "Use RESULT_SCAN() for recent query results",
                "Apply micro-partitions awareness",
                "Use materialized views for complex aggregations"
            ]
        }
    }

def main(session) -> str:
    """
    Main handler function that returns controlled query optimization workflow.
    
    Args:
        session: Snowflake session object
        
    Returns:
        JSON string containing optimization workflow
    """
    workflow = get_optimization_workflow()
    return json.dumps(workflow, indent=2)
$$;

-- Example usage:
-- CALL SNOWFLAKE_INTELLIGENCE.QUERY_DEMO.QUERY_OPTIMIZE_WORKFLOW();

-- ===============================================================================
-- USAGE INSTRUCTIONS FOR AGENTS
-- ===============================================================================
-- ⚠️ CRITICAL: CONTROLLED METADATA GATHERING WITH LOOP PREVENTION ⚠️
-- 
-- MANDATORY WORKFLOW:
-- 1. User submits query for optimization
-- 2. Agent identifies performance issues (DO NOT OPTIMIZE YET)
-- 3. Agent presents issues to user with line numbers
-- 4. Agent asks: "Would you like me to proceed with optimization?"
-- 5. User confirms YES
-- 6. Agent identifies tables (extract from query)
-- 7. Agent executes DESCRIBE ONCE per table (track to prevent re-execution)
-- 8. Agent analyzes patterns using metadata
-- 9. Agent applies optimizations
-- 10. Agent presents optimized query and STOPS
-- 
-- LOOP PREVENTION RULES:
-- - ONE DESCRIBE per table MAXIMUM
-- - Track tables already described in a list
-- - Before DESCRIBE, check if table in list
-- - If in list, SKIP and use cached metadata
-- - NO iterative refinement
-- - Complete in ONE pass
-- 
-- TECHNICAL STEPS:
-- 1. Call this procedure to get the workflow JSON
-- 2. Parse the JSON and follow the loop_prevention_instructions
-- 3. Use tracking_mechanism to prevent DDL loops
-- 4. Apply common_optimization_patterns after gathering metadata
-- 5. Ensure all queries are Snowflake-compatible
-- 6. Complete optimization in under 3 minutes
-- 
-- ⚠️ REMEMBER: NEVER SKIP USER CONFIRMATION & ONE DESCRIBE PER TABLE ⚠️
-- ===============================================================================