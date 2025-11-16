    
import duckdb
from datetime import datetime, timedelta
import pandas as pd

DB_PATH = "data/hummingbird.db"

conn.execute("""
    COPY raw_papers 
    TO 'raw_papers.parquet' 
    (FORMAT parquet, COMPRESSION zstd)
""")

