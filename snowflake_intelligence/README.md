# Snowflake Intelligence Setup

Foundation infrastructure for the Query Optimizer toolkit.

## What Gets Created

Running `setup.sql` creates:
- **Role**: SNOWFLAKE_INTELLIGENCE_ADMIN_RL
- **Warehouse**: SNOWFLAKE_INTELLIGENCE_WH (X-Small, auto-suspend)
- **Database**: SNOWFLAKE_INTELLIGENCE
- **Schemas**: AGENTS, INTEGRATIONS, TOOLS

## Prerequisites

- ACCOUNTADMIN role access
- Ability to create roles, warehouses, and databases

## Setup

1. Open `setup.sql` in Snowflake worksheet
2. Run entire script as ACCOUNTADMIN
3. Script automatically grants new role to current user

## Post-Setup

After running setup, you'll have:
- Admin role granted to your user
- Dedicated warehouse for query analysis
- Organized schema structure for tools

## Schema Purpose

**AGENTS** - AI agent configurations and prompts  
**INTEGRATIONS** - External service connections (GitHub, APIs)  
**TOOLS** - Stored procedures and functions for analysis

## Next Steps

1. Run GitHub integration setup if using GitHub tools
2. Deploy query analysis procedures to TOOLS schema
3. Configure any additional integrations as needed

## Files

- `setup.sql` - Complete infrastructure setup script