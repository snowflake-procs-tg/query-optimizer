# Query Analysis Tools

SQL procedures for fetching and analyzing Snowflake query performance data.

## Setup

Run these files as SYSADMIN:
1. `query_data_fetcher.sql` - Fetches detailed query operator statistics
2. `query_history_fetcher.sql` - Retrieves query text and execution context

## Usage

### Fetch Query Performance Data
```sql
-- Get detailed operator statistics for a query
CALL QUERY_DATA_FETCHER('01bdee2b-0306-13e1-000d-f46b0008f056');
```

### Get Query Text and Context
```sql
-- Retrieve the actual SQL text and execution details
CALL QUERY_TEXT('01bdee2b-0306-13e1-000d-f46b0008f056');
```

## Tool Context

These procedures extract critical data that agents analyze for optimization:

**QUERY_DATA_FETCHER** - Returns comprehensive JSON with operator-level statistics including execution time breakdowns, memory usage, spilling details, and pruning efficiency. This is the primary data source for identifying query bottlenecks and performance issues.

**QUERY_TEXT** - Retrieves the actual SQL statement text along with execution context (user, role, warehouse size, load percentage). Essential for understanding what query was run and under what conditions. Note: Has up to 45-minute latency due to ACCOUNT_USAGE.

These tools form the data collection layer that feeds into the analysis and optimization workflow.

## Files

- `query_data_fetcher.sql` - Detailed operator statistics extraction
- `query_history_fetcher.sql` - Query text and execution context retrieval