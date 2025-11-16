    
import duckdb
from datetime import datetime, timedelta
import pandas as pd

DB_PATH = "data/hummingbird.db"
CSV_PATH = "static/qs_rankings_bio.csv"

# Read the CSV
rankings_df = pd.read_csv(CSV_PATH)

# Connect to database
conn = duckdb.connect(DB_PATH, read_only=True)

# Loop through each university and find matches
for index, row in rankings_df.iterrows():
    ranking = row['ranking']
    university = row['university']
    
    query = f"""
    SELECT institution_name, count(*) as paper_count
    FROM paper_authors
    WHERE regexp_replace(lower(institution_name), '[^a-z0-9 ]', '', 'g') 
        LIKE '%' || regexp_replace(lower('{university}'), '[^a-z0-9 ]', '', 'g') || '%'
    GROUP BY institution_name
    ORDER BY paper_count DESC
    """
        
    results = conn.execute(query).fetchdf().to_dict('records')
    
    if results:
        print(f"\n#{ranking} - {university}")
        for r in results:
            print(f"  {r['institution_name']}: {r['paper_count']} papers")
    else:
        print(f"\n#{ranking} - {university}: No matches found")


query = f"""
SELECT institution_name, count(*)
FROM paper_authors
where lower(institution_name) like '%harvard%' 
group by 1
order by 2 desc
LIMIT 1000
"""

query = f"""
SELECT server, count(*)
FROM raw_papers
group by 1
order by 2 desc
LIMIT 1000
"""

results = conn.execute(query).fetchdf().to_dict('records')
conn.close()


for r in results:
    print(r)

