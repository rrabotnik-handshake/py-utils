from google.cloud import bigquery

client = bigquery.Client(project="handshake-production")

# Perform a query.
QUERY = (
    'SELECT * FROM `coresignal.linkedin_member_us_snapshot_intermediate`'
    'LIMIT 1')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row)
