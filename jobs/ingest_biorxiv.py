"""
Job 1: Ingest papers from bioRxiv/medRxiv API
"""
import requests
from datetime import datetime, timedelta
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from storage.db import get_connection, init_schema


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.exceptions.RequestException, requests.exceptions.HTTPError))
)
def fetch_papers_from_api(server, start_date, end_date, cursor=0):
    """Fetch one page from bioRxiv API with retry logic"""
    url = f"https://api.biorxiv.org/details/{server}/{start_date}/{end_date}/{cursor}/json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def ingest_papers(server, days_back=7, con=None):
    """
    Ingest papers from bioRxiv/medRxiv.
    
    Args:
        server: 'biorxiv' or 'medrxiv'
        days_back: How many days to look back
        con: Existing connection or None
    """
    close_after = False
    if con is None:
        con = get_connection()
        close_after = True
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    
    print(f"\n📥 Ingesting {server} papers from {start_date} to {end_date}")
    print("=" * 70)
    
    cursor = 0
    total_inserted = 0
    total_updated = 0
    total_skipped = 0
    
    while True:
        print(f"Fetching cursor {cursor}...", end=" ")
        
        try:
            data = fetch_papers_from_api(server, start_date, end_date, cursor)
        except Exception as e:
            print(f"❌ Failed after retries: {e}")
            break
        
        papers = data.get('collection', [])
        if not papers:
            print("✅ No more papers")
            break
        
        print(f"got {len(papers)} papers")
        
        for paper in papers:
            doi = paper['doi']
            new_version = int(paper['version'])
            
            existing = con.execute(
                "SELECT version FROM raw_papers WHERE doi = ?",
                (doi,)
            ).fetchone()
            
            if existing is None:
                # New paper - INSERT
                try:
                    con.execute("""
                        INSERT INTO raw_papers (
                            doi, version, title, abstract, authors,
                            author_corresponding, author_corresponding_institution,
                            published_date, category, server
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        doi,
                        new_version,
                        paper['title'],
                        paper.get('abstract'),
                        paper.get('authors'),
                        paper.get('author_corresponding'),
                        paper.get('author_corresponding_institution'),
                        paper['date'],
                        paper.get('category'),
                        server
                    ))
                    
                    con.execute("""
                        INSERT INTO enrichment_status (doi, openalex_enriched)
                        VALUES (?, FALSE)
                    """, (doi,))
                    
                    total_inserted += 1
                    
                except Exception as e:
                    print(f"⚠️ Error inserting {doi}: {e}")
                    
            elif new_version > existing[0]:
                # Newer version of the same DOI - UPDATE existing record
                try:
                    con.execute("""
                        UPDATE raw_papers
                        SET version = ?,
                            title = ?,
                            abstract = ?,
                            authors = ?,
                            author_corresponding = ?,
                            author_corresponding_institution = ?,
                            published_date = ?,
                            category = ?
                        WHERE doi = ?
                    """, (
                        new_version,
                        paper['title'],
                        paper.get('abstract'),
                        paper.get('authors'),
                        paper.get('author_corresponding'),
                        paper.get('author_corresponding_institution'),
                        paper['date'],
                        paper.get('category'),
                        doi
                    ))
                    
                    total_updated += 1
                    
                except Exception as e:
                    print(f"⚠️ Error updating {doi}: {e}")
            else:
                total_skipped += 1
        
        messages = data.get('messages', [{}])[0]
        total = int(messages.get('total', 0))
        
        cursor += 100
        
        if cursor < total:
            time.sleep(0.5)
        else:
            break
    
    print(f"✅ Ingestion complete!")
    print(f"New papers: {total_inserted}")
    print(f"Updated papers: {total_updated}")
    print(f"Skipped (old versions): {total_skipped}")
    
    if close_after:
        con.close()