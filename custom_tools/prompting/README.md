# Query Optimization Prompting Tools

SQL procedures and workflows for analyzing and optimizing Snowflake query performance.

## Setup

Run these files:
1. `field_definitions.sql` - Defines performance metrics
2. `analysis_guidance.sql` - Analysis decision trees
3. `query_optimize_workflow.sql` - Optimization workflow prompting

## Usage

### Get Field Definitions
```sql
-- View all available performance metrics
CALL FIELD_DEFINITIONS();
```

### Get Analysis Guidance
```sql
-- Get detailed guidance for query analysis
CALL ANALYSIS_GUIDANCE();
```

### Optimize a Query
```sql
-- Analyze and optimize a problematic query
CALL QUERY_OPTIMIZE_WORKFLOW();
```

## Tool Context

These procedures return structured information designed for AI agents and manual analysis:

**FIELD_DEFINITIONS** - Returns JSON with all performance metric definitions, thresholds, and descriptions. Use this to help the agent understand what metrics are available in query analysis results.

**ANALYSIS_GUIDANCE** - Provides decision trees and troubleshooting workflows as structured text. Helps agents systematically diagnose performance issues based on metrics.

**QUERY_OPTIMIZE_WORKFLOW** - Returns a comprehensive JSON workflow with optimization patterns, Snowflake-specific syntax rules, and step-by-step instructions for query improvement.

These tools provide the knowledge base that enables intelligent query optimization decisions.