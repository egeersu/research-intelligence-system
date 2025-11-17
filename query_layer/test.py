    
import duckdb
from datetime import datetime, timedelta

DB_PATH = "data/hummingbird.db"

# Connect to database
conn = duckdb.connect(DB_PATH, read_only=True)

query = f"""
SELECT topic_name, count(*)
FROM paper_topics
group by 1
order by 2 desc
LIMIT 1000
"""


results = conn.execute(query).fetchdf().to_dict('records')
conn.close()

for r in results:
    print(r)

