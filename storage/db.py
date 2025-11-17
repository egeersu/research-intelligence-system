"""Database connection and initialization"""
import duckdb
from pathlib import Path

# Default database path
DEFAULT_DB_PATH = "data/hummingbird.db"


def get_connection(db_path=None):
    """
    Get a DuckDB connection.
    
    Args:
        db_path: Path to database file. If None, uses DEFAULT_DB_PATH
        
    Returns:
        duckdb.DuckDBPyConnection
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    
    # Ensure data directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    return duckdb.connect(db_path)

def load_rankings_data(con):
    """Load QS rankings from CSV if table is empty"""
    csv_path = Path(__file__).parent.parent / "static" / "qs_rankings.csv"
    
    # Check if already loaded
    count = con.execute("SELECT COUNT(*) FROM institution_rankings").fetchone()[0]
    if count > 0:
        return
    
    # Load CSV
    con.execute(f"""
        COPY institution_rankings 
        FROM '{csv_path}' 
        (HEADER TRUE, DELIMITER ',')
    """)
    print("✅ Loaded QS rankings data")


def init_schema(con=None):
    """
    Initialize database schema from schema.sql
    
    Args:
        con: Existing connection, or None to create new one
    """
    close_after = False
    if con is None:
        con = get_connection()
        close_after = True
    
    # Read schema file
    schema_path = Path(__file__).parent / "schema.sql"
    with open(schema_path, 'r') as f:
        schema_sql = f.read()
    
    # Execute schema
    con.execute(schema_sql)
    
    # Load rankings data
    load_rankings_data(con)
    
    if close_after:
        con.close()
    
    print("✅ Database schema initialized")

def reset_db(db_path=None):
    """
    Drop all tables and reinitialize schema.
    WARNING: This deletes all data!
    
    Args:
        db_path: Path to database file
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    
    con = get_connection(db_path)
    
    # Drop all tables
    tables = [
        'paper_citation_snapshots',
        'paper_authors',
        'paper_topics',
        'enrichment_status',
        'raw_papers',
        'institution_rankings'
    ]
    
    for table in tables:
        con.execute(f"DROP TABLE IF EXISTS {table}")
    
    print("🗑️  Dropped all tables")
    
    # Reinitialize
    init_schema(con)
    con.close()


def get_stats(con=None):
    """
    Get database statistics
    
    Returns:
        dict with counts of papers, enriched papers, etc.
    """
    close_after = False
    if con is None:
        con = get_connection()
        close_after = True
    
    stats = {}
    
    # Total papers
    stats['total_papers'] = con.execute(
        "SELECT COUNT(*) FROM raw_papers"
    ).fetchone()[0]
    
    # Enriched papers
    stats['enriched_papers'] = con.execute(
        "SELECT COUNT(*) FROM enrichment_status WHERE openalex_enriched = TRUE"
    ).fetchone()[0]
    
    # Papers with citation tracking
    stats['papers_with_citations'] = con.execute(
        "SELECT COUNT(DISTINCT doi) FROM paper_citation_snapshots"
    ).fetchone()[0]
    
    # Average citations (from latest snapshot)
    result = con.execute("""
        SELECT AVG(cited_by_count)
        FROM (
            SELECT doi, cited_by_count
            FROM paper_citation_snapshots
            WHERE (doi, snapshot_date) IN (
                SELECT doi, MAX(snapshot_date)
                FROM paper_citation_snapshots
                GROUP BY doi
            )
        )
    """).fetchone()
    stats['avg_citations'] = result[0] if result[0] else 0
    
    if close_after:
        con.close()
    
    return stats