# Object Management Tools

SQL procedures for managing database objects and data in Snowflake.

## Setup

Run these files as SYSADMIN:
- `ddl_manager.sql` - Structure operations (CREATE, ALTER, DROP)
- `dml_manager.sql` - Data operations (INSERT, UPDATE, DELETE, SELECT)

## Usage

### DDL Operations (Structure)

```sql
-- Show all tables
CALL EXECUTE_DDL('SHOW TABLES');

-- Create a table
CALL EXECUTE_DDL('CREATE TABLE my_table (id INT, name VARCHAR)');

-- Add a column
CALL EXECUTE_DDL('ALTER TABLE my_table ADD COLUMN email VARCHAR');

-- Drop a table
CALL EXECUTE_DDL('DROP TABLE my_table');
```

### DML Operations (Data)

```sql
-- Insert data
CALL EXECUTE_DML('INSERT INTO my_table VALUES (1, ''John'')');

-- Update data
CALL EXECUTE_DML('UPDATE my_table SET name = ''Jane'' WHERE id = 1');

-- Query data
CALL EXECUTE_DML('SELECT * FROM my_table');

-- Delete data
CALL EXECUTE_DML('DELETE FROM my_table WHERE id = 1');
```

## Output Formats

### DDL Output
- `pretty` - Formatted JSON (default)
- `minified` - Compact JSON
- `table` - ASCII table for SHOW/DESCRIBE

### DML Output  
- `table` - ASCII table (default)
- `json` - Structured JSON
- `summary` - Row count only

## Examples

```sql
-- Show users as ASCII table
CALL EXECUTE_DDL('SHOW USERS', 'table');

-- Query with JSON output
CALL EXECUTE_DML('SELECT * FROM my_table LIMIT 10', 'json');
```

## Files

- `ddl_manager.sql` - Data Definition Language operations
- `dml_manager.sql` - Data Manipulation Language operations