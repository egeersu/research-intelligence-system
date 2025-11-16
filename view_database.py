"""
Inspect database tables, schemas, and sample data
"""
from storage.db import get_connection


def inspect_database():
    """Print all tables, their schemas, and sample rows"""
    con = get_connection()
    
    # Get all tables
    tables = con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).fetchall()
    
    if not tables:
        print("❌ No tables found in database")
        print("   Run: python -m storage.db --init")
        con.close()
        return
    
    print("\n" + "=" * 100)
    print("📊 DATABASE INSPECTION")
    print("=" * 100)
    
    for (table_name,) in tables:
        print(f"\n{'=' * 100}")
        print(f"TABLE: {table_name}")
        print("=" * 100)
        
        # Get schema
        schema = con.execute(f"PRAGMA table_info({table_name})").fetchall()
        
        print("\n📋 SCHEMA:")
        print("-" * 100)
        for col in schema:
            col_id, name, col_type, not_null, default, pk = col
            pk_marker = " [PK]" if pk else ""
            not_null_marker = " NOT NULL" if not_null else ""
            default_marker = f" DEFAULT {default}" if default else ""
            print(f"  {name:30} {col_type:15}{pk_marker}{not_null_marker}{default_marker}")
        
        # Get row count
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"\n📊 TOTAL ROWS: {count:,}")
        
        if count > 0:
            # Get sample rows
            print("\n📄 SAMPLE ROWS (up to 3):")
            print("-" * 100)
            
            rows = con.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
            columns = [desc[0] for desc in con.description]
            
            # Print column headers
            header = " | ".join(f"{col[:20]:20}" for col in columns)
            print(header)
            print("-" * len(header))
            
            # Print rows
            for row in rows:
                # Format each value (truncate strings, handle None)
                formatted_values = []
                for val in row:
                    if val is None:
                        formatted_values.append("NULL".ljust(20))
                    elif isinstance(val, str):
                        # Truncate long strings
                        truncated = val[:20] if len(val) > 20 else val
                        formatted_values.append(truncated.ljust(20))
                    else:
                        formatted_values.append(str(val)[:20].ljust(20))
                
                print(" | ".join(formatted_values))
        else:
            print("\n  (No rows yet)")
        
        print()
    
    print("=" * 100)
    print("✅ Inspection complete")
    print("=" * 100 + "\n")
    
    con.close()


def custom_query(query, description="Custom Query"):
    """Execute a custom SQL query and print results"""
    con = get_connection()
    
    print("\n" + "=" * 80)
    print(f"🔍 {description}")
    print("=" * 80 + "\n")
    
    try:
        rows = con.execute(query).fetchall()
        columns = [desc[0] for desc in con.description]
        
        # Print results
        for row in rows:
            for col, val in zip(columns, row):
                print(f"{col}: {val}")
            print("-" * 80)
        
        print(f"\nTotal: {len(rows)} rows\n")
        
    except Exception as e:
        print(f"❌ Error: {e}\n")
    
    con.close()


if __name__ == "__main__":
    inspect_database()
    
    # Example: Check for duplicate DOIs
    custom_query(
        query="""
            SELECT doi, author_id, author_name, orcid, institution_name
            from paper_authors
            LIMIT 20
        """,
        description=""
    )