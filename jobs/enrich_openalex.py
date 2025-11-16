"""
Job 2: Enrich papers with OpenAlex data
"""
from datetime import datetime
from openalex.client import OpenAlexClient
from storage.db import get_connection
from storage.queries import (
    get_papers_needing_enrichment,
    mark_enrichment_complete,
    mark_enrichment_attempt,
    calculate_next_citation_check
)


def enrich_papers(email, batch_size=500, con=None):
    """Enrich one batch of papers with OpenAlex data."""
    close_after = False
    if con is None:
        con = get_connection()
        close_after = True
    
    client = OpenAlexClient(email=email)
    
    print(f"\n🔬 OpenAlex Enrichment")
    print("=" * 70)
    
    papers = get_papers_needing_enrichment(limit=batch_size, con=con)
    
    if not papers:
        print(f"✅ No papers need enrichment!")
        if close_after:
            con.close()
        return
    
    dois = [p['doi'] for p in papers]
    print(f"📦 Processing {len(dois)} papers")
    print("-" * 70)
    
    try:
        enriched_papers = client.get_papers_by_dois(dois, verbose=True)
    except Exception as e:
        print(f"❌ OpenAlex API error: {e}")
        if close_after:
            con.close()
        return
    
    enriched_by_doi = {p['doi']: p for p in enriched_papers}
    
    # Debug info
    print(f"\n📊 Fetched {len(enriched_papers)}/{len(dois)} papers from OpenAlex")
    if len(enriched_papers) < len(dois):
        missing_count = len(dois) - len(enriched_papers)
        print(f"   Missing: {missing_count} papers")
    print()
    
    success_count = 0
    not_found_count = 0
    error_count = 0
    today = datetime.now().date()
    
    for paper_info in papers:
        doi = paper_info['doi']
        
        try:
            if doi not in enriched_by_doi:
                mark_enrichment_attempt(doi, con=con)
                not_found_count += 1
                print(f"  ❌ Not found: {doi}")
                continue
            
            paper = enriched_by_doi[doi]
            
            # Insert topics
            for topic in paper['topics']:
                con.execute("""
                    INSERT OR REPLACE INTO paper_topics
                    (doi, topic_name, field)
                    VALUES (?, ?, ?)
                """, (doi, topic['name'], topic['field']))
            
            # Insert authors
            for author in paper['authors']:
                con.execute("""
                    INSERT OR REPLACE INTO paper_authors
                    (doi, author_id, author_name, orcid, institution_name)
                    VALUES (?, ?, ?, ?, ?)
                """, (doi, author['id'], author['name'], 
                      author.get('orcid'), author.get('institution_name')))
            
            # Create first citation snapshot
            con.execute("""
                INSERT OR REPLACE INTO paper_citation_snapshots
                (doi, snapshot_date, cited_by_count, growth_since_last)
                VALUES (?, ?, ?, 0)
            """, (doi, today, paper['cited_by_count']))
            
            # Calculate initial citation check schedule
            published_date = con.execute(
                "SELECT published_date FROM raw_papers WHERE doi = ?",
                (doi,)
            ).fetchone()[0]
            
            paper_age_days = (today - datetime.fromisoformat(str(published_date)).date()).days
            next_check = calculate_next_citation_check(
                paper_age_days, 0, paper['cited_by_count']
            )
            
            # Mark as enriched
            mark_enrichment_complete(doi, con=con)
            con.execute("""
                UPDATE enrichment_status
                SET next_citation_check = ?
                WHERE doi = ?
            """, (next_check, doi))
            
            success_count += 1
            
        except Exception as e:
            mark_enrichment_attempt(doi, con=con)
            print(f"  ⚠️  Error: {doi}: {e}")
            error_count += 1
    
    print("\n" + "=" * 70)
    print(f"📊 Batch Complete:")
    print(f"   ✅ Enriched: {success_count}")
    print(f"   ❌ Not in OpenAlex: {not_found_count}")
    print(f"   ⚠️  Errors: {error_count}")
    
    remaining = get_papers_needing_enrichment(limit=1, con=con)
    if remaining:
        print(f"\n💡 More papers need enrichment. Run again to continue.")
    else:
        print(f"\n✅ All papers enriched!")
    
    print("=" * 70)
    
    if close_after:
        con.close()