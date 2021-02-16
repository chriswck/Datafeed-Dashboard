# Datafeed-Dashboard

The following 2 json files are required at top-level
1. credentials.json - the [GSuite API](https://developers.google.com/sheets/api/quickstart/python) credentials
2. envvars.json - containing parameters for connection to databases and aws s3:
    1. AWS_KEY_ACCESS
    2. AWS_KEY_SECRET
    3. AWS_BUCKET_PRIVE_ETL
    4. MYSQL_USERNAME
    5. MYSQL_PASSWORD
    6. MYSQL_HOST
    7. MYSQL_DB_NAME
