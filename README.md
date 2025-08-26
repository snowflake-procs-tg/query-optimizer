# Query Optimizer Toolkit

A comprehensive Snowflake query performance optimization toolkit that provides AI-powered analysis tools and automated optimization workflows.

## What This Toolkit Provides

- **Performance Analysis**: Deep operator-level statistics and bottleneck identification
- **Query Optimization**: Automated pattern detection and Snowflake-specific optimizations
- **GitHub Integration**: Access complete SQL queries stored in repositories
- **Database Management**: DDL/DML procedures for structure and data operations
- **AI-Ready Prompts**: Structured guidance for intelligent query optimization

## Setup Workflow

### Step 1: Foundation Setup
**Required for first-time installation**

```sql
-- Run as ACCOUNTADMIN
snowflake_intelligence/setup.sql
```

This creates:
- SNOWFLAKE_INTELLIGENCE database
- TOOLS, AGENTS, INTEGRATIONS schemas
- Dedicated warehouse and admin role

### Step 2: Deploy Custom Tools
**Run these in order as SYSADMIN**

1. **Query Analysis Tools**
   ```sql
   custom_tools/query/query_data_fetcher.sql
   custom_tools/query/query_history_fetcher.sql
   ```

2. **Prompting Tools**
   ```sql
   custom_tools/prompting/field_definitions.sql
   custom_tools/prompting/analysis_guidance.sql
   custom_tools/prompting/query_optimize_workflow.sql
   ```

3. **Object Management Tools**
   ```sql
   custom_tools/object_management/ddl_manager.sql
   custom_tools/object_management/dml_manager.sql
   ```

### Step 3: Optional - GitHub Integration
**For accessing queries stored in GitHub**

1. Get GitHub Personal Access Token
2. Edit `custom_tools/github/setup_github_integration.sql`
3. Replace `YOUR_GITHUB_PAT_HERE` with your token
4. Run as ACCOUNTADMIN
5. Deploy procedures:
   ```sql
   custom_tools/github/extract_file.sql
   custom_tools/github/list_directory.sql
   ```

## Quick Start Example

```sql
-- Analyze query performance
CALL QUERY_DATA_FETCHER('your-query-id');

-- Get query text
CALL QUERY_TEXT('your-query-id');

-- Get optimization guidance
CALL QUERY_OPTIMIZE_WORKFLOW();
```

## Directory Structure

```
query-optimizer/
├── snowflake_intelligence/     # Foundation setup
├── custom_tools/
│   ├── query/                  # Performance data extraction
│   ├── prompting/              # AI optimization guidance
│   ├── object_management/      # DDL/DML operations
│   └── github/                 # GitHub integration
├── agent_instructions.md       # Performance analysis guidelines
└── agent_orchestration.md      # Workflow and tool usage patterns
```

## Key Features

- **Operator Statistics**: Detailed execution time breakdowns and memory usage
- **Bottleneck Detection**: Identifies high execution time operators and spilling
- **Pattern Recognition**: Detects common anti-patterns and suggests fixes
- **Snowflake Optimizations**: Platform-specific improvements (QUALIFY, FLATTEN, etc.)
- **Performance Tracking**: Historical analysis and optimization validation

## Requirements

- Snowflake account with ACCOUNTADMIN access (for setup)
- SYSADMIN role for deploying procedures
- Access to ACCOUNT_USAGE views for query analysis

## Support

For issues or questions, refer to the individual README files in each directory for detailed documentation.