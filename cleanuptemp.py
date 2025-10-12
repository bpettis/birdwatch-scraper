import psycopg2
import re, os
from dotenv import load_dotenv, find_dotenv

# set up some global variables:
load_dotenv(find_dotenv()) # load environment variables

# Get DB info from the environment
db_host = os.environ.get("DB_HOST")
db_user = os.environ.get("DB_USER")
db_name = os.environ.get("DB_NAME")
db_password = os.environ.get("DB_PASS")

# Database connection parameters
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=5432
)



cursor = conn.cursor()

# Step 1: Get all table names matching the pattern
cursor.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_name LIKE 'note_requests_%'
    AND table_type = 'BASE TABLE';
""")

tables = cursor.fetchall()

# Step 2: Iterate through tables and perform SQL operations
for (schema, table_name) in tables:
    print(f"Processing table: {table_name}")
    
    # Example SQL operation: count rows
    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
    count = cursor.fetchone()[0]
    print(f"Table {table_name} has {count} rows.")
    
    sql = 'INSERT INTO note_requests ("requestId", "userId", "tweetId", "createdAtMillis", "sourceLink") SELECT "requestId", "userId", "tweetId", "createdAtMillis", "sourceLink" FROM {0} ON CONFLICT DO NOTHING;'.format(table_name)
    cursor.execute(sql)
    cursor.execute("""DROP TABLE IF EXISTS """ + table_name + """ CASCADE;""")

# Cleanup
cursor.close()
conn.close()