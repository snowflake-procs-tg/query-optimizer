# GitHub Integration Tools

Quick setup for accessing GitHub repositories from Snowflake.

## Setup

### 1. Get a GitHub Token
- Go to GitHub → Settings → Developer settings → Personal access tokens
- Generate new token (classic)
- Select `public_repo` scope (or `repo` for private repos)
- Copy the token

### 2. Run Setup
Edit `setup_github_integration.sql`:
- Replace `YOUR_GITHUB_PAT_HERE` with your token
- Run the entire file as ACCOUNTADMIN

### 3. Create Procedures
Run these files as SYSADMIN:
- `extract_file.sql` - Fetch file contents
- `list_directory.sql` - Browse repository structure

## Usage

### List Repository Contents
```sql
CALL GITHUB_DIRECTORY_LISTER(
    'microsoft',     -- owner
    'vscode',        -- repo
    'src',           -- path (empty for root)
    'main'           -- branch
);
```

### Get File Contents
```sql
CALL GITHUB_FILE_EXTRACTOR(
    'pandas-dev',              -- owner
    'pandas',                  -- repo  
    'pandas/core/frame.py',    -- file path
    'main'                     -- branch
);
```

## Troubleshooting

**Authentication Error**: Check your GitHub token is valid  
**Network Error**: Verify setup script ran successfully  
**Rate Limit**: Wait an hour (5000 requests/hour limit)

## Files

- `setup_github_integration.sql` - One-time setup (ACCOUNTADMIN required)
- `extract_file.sql` - Fetch file contents procedure
- `list_directory.sql` - List directory contents procedure