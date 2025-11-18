"""
Job 3: Track citation growth over time
"""
from datetime import datetime
from openalex.client import OpenAlexClient
from storage.db import get_connection
from storage.queries import (
    get_papers_due_for_citation_check,
    get_last_citation_count,
    update_citation_check_schedule,
    calculate_next_citation_check
)


def track_citations(email, batch_size=500, con=None):
    """Update citation counts for papers due for checking."""
    close_after = False
    if con is None:
        con = get_connection()
        close_after = True
    
    papers = get_papers_due_for_citation_check(limit=batch_size, con=con)
    
    if not papers:
        print("✅ No papers due for citation check!")
        if close_after:
            con.close()
        return
    
    dois = [p['doi'] for p in papers]
    
    print(f"\n📊 Tracking citations for {len(dois)} papers")
    
    client = OpenAlexClient(email=email)
    raw_papers = client.get_papers_by_dois(dois, verbose=True)
    papers_by_doi = {p['doi']: p for p in raw_papers}
    
    updated_count = 0
    skipped_count = 0
    high_growth_count = 0
    today = datetime.now().date()
    
    for paper_info in papers:
        doi = paper_info['doi']
        published_date = paper_info['published_date']
        
        try:
            if doi in papers_by_doi:
                openalex_paper = papers_by_doi[doi]
                new_citation_count = openalex_paper['cited_by_count']
                
                previous_count = get_last_citation_count(doi, con=con)
                growth = new_citation_count - previous_count
                
                con.execute("""
                    INSERT OR REPLACE INTO paper_citation_snapshots
                    (doi, snapshot_date, cited_by_count, growth_since_last)
                    VALUES (?, ?, ?, ?)
                """, (doi, today, new_citation_count, growth))
                
                paper_age_days = (today - published_date).days
                next_check = calculate_next_citation_check(
                    paper_age_days, growth, new_citation_count
                )
                
                update_citation_check_schedule(doi, next_check, con=con)
                
                updated_count += 1
                
                if growth > 4:
                    high_growth_count += 1
                    print(f"🔥 High growth: {doi} (+{growth} citations)")
                
            else:
                skipped_count += 1
                
        except Exception as e:
            print(f"⚠️  Error tracking {doi}: {e}")
            skipped_count += 1
    
    print(f"✅ Citation tracking complete!")
    print(f"Updated: {updated_count}")
    print(f"High growth papers: {high_growth_count}")
    print(f"Skipped/errors: {skipped_count}")

    if close_after:
        con.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Track citation growth over time')
    parser.add_argument('--email', required=True, help='Email for OpenAlex polite pool')
    parser.add_argument('--batch-size', type=int, default=500, help='Max papers to check')
    
    args = parser.parse_args()
    
    track_citations(email=args.email, batch_size=args.batch_size)