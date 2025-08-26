/*
===============================================================================
GITHUB INTEGRATION SETUP FOR SNOWFLAKE
===============================================================================

PURPOSE:
Sets up the external access integration required for GitHub API procedures.
This includes network rules, secrets storage, and integration configuration
to allow Snowflake stored procedures to communicate with the GitHub API.

PREREQUISITES:
- ACCOUNTADMIN role access for initial setup
- Valid GitHub Personal Access Token (PAT)

KEY COMPONENTS:
- Network rule for GitHub API endpoint
- Secret storage for GitHub token
- External access integration
- Role grants for accessing the integration

TABLE OF CONTENTS (Search for these markers):
- [SECTION_1_NETWORK]      : Network rule configuration
- [SECTION_2_SECRET]       : GitHub token secret setup
- [SECTION_3_INTEGRATION]  : External access integration
- [SECTION_4_GRANTS]       : Permission grants
- [SECTION_5_VALIDATION]   : Setup validation steps

SECURITY NOTE:
The GitHub token in this file should be replaced with your own token.
Never commit actual tokens to version control. Consider using environment
variables or secure secret management systems in production.

===============================================================================
*/

-- [SECTION_1_NETWORK] Network Rule Configuration
-- ===============================================
-- This rule allows outbound HTTPS connections to GitHub's API

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE NETWORK RULE SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_API_NETWORK_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('api.github.com:443')
    COMMENT = 'Allows HTTPS connections to GitHub API for repository access';

-- [SECTION_2_SECRET] GitHub Token Secret Setup
-- ============================================
-- Store your GitHub Personal Access Token securely
-- ⚠️ IMPORTANT: Replace YOUR_GITHUB_PAT_HERE with your actual GitHub token

CREATE OR REPLACE SECRET SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_TOKEN
    TYPE = GENERIC_STRING
    SECRET_STRING = 'YOUR_GITHUB_PAT_HERE'  -- ⚠️ REPLACE THIS WITH YOUR ACTUAL GITHUB TOKEN
    COMMENT = 'GitHub Personal Access Token for API authentication';

/*
To create a GitHub Personal Access Token:
1. Go to GitHub Settings > Developer settings > Personal access tokens
2. Click "Generate new token" (classic)
3. Select scopes: 
   - repo (for private repos)
   - public_repo (for public repos only)
4. Generate and copy the token
5. Replace the SECRET_STRING above with your token
*/

-- [SECTION_3_INTEGRATION] External Access Integration
-- ===================================================
-- Creates the integration that allows procedures to use external network access

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GITHUB_API_INTEGRATION
    ALLOWED_NETWORK_RULES = (SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_API_NETWORK_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_TOKEN)
    ENABLED = TRUE
    COMMENT = 'External access integration for GitHub API procedures';

-- [SECTION_4_GRANTS] Permission Grants
-- ====================================
-- Grant necessary permissions to roles that will use GitHub procedures

-- Grant usage on the integration to SYSADMIN
GRANT USAGE ON INTEGRATION GITHUB_API_INTEGRATION TO ROLE SYSADMIN;

-- Optional: Grant to additional roles as needed
-- GRANT USAGE ON INTEGRATION GITHUB_API_INTEGRATION TO ROLE DATA_ENGINEER;
-- GRANT USAGE ON INTEGRATION GITHUB_API_INTEGRATION TO ROLE DEVELOPER;

-- Grant usage on the secret to roles that need it
GRANT USAGE ON SECRET SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_TOKEN TO ROLE SYSADMIN;

-- [SECTION_5_VALIDATION] Setup Validation Steps
-- =============================================
-- Verify the setup is complete and working

-- Check that the network rule exists
SHOW NETWORK RULES LIKE 'GITHUB_API_NETWORK_RULE' IN SCHEMA SNOWFLAKE_INTELLIGENCE.TOOLS;

-- Check that the secret exists (won't show the actual token value)
SHOW SECRETS LIKE 'GITHUB_TOKEN' IN SCHEMA SNOWFLAKE_INTELLIGENCE.TOOLS;

-- Check that the integration exists and is enabled
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'GITHUB_API_INTEGRATION';

-- Verify integration details
DESCRIBE EXTERNAL ACCESS INTEGRATION GITHUB_API_INTEGRATION;

/*
===============================================================================
POST-SETUP NOTES
===============================================================================

After running this setup script:

1. The GitHub procedures (GITHUB_FILE_EXTRACTOR and GITHUB_DIRECTORY_LISTER)
   will be able to access the GitHub API

2. If you encounter authentication errors:
   - Verify your GitHub token is valid
   - Check token permissions (needs repo access)
   - Ensure the token hasn't expired

3. If you encounter network errors:
   - Verify the network rule is properly configured
   - Check that the integration is ENABLED
   - Ensure your Snowflake account allows external network access

4. Rate Limiting:
   - GitHub API has rate limits (5000 requests/hour for authenticated requests)
   - The procedures will return appropriate error messages if limits are exceeded

5. Security Best Practices:
   - Rotate your GitHub token periodically
   - Use tokens with minimal required permissions
   - Monitor usage through GitHub's API dashboard
   - Never commit tokens to version control

===============================================================================
*/