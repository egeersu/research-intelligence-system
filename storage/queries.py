"""Helper functions for common database queries"""
from datetime import datetime, timedelta
from storage.db import get_connection


def calculate_next_citation_check(paper_age_days, growth, current_citations):
    """
    Heuristic for citation check scheduling.
    
    Rules:
    1. Recent (< 90 days): weekly
    2. Growth (>4 citations): weekly
    3. Young + traction (< 1 year, >5 citations): bi-weekly
    4. Established (>50 citations): weekly
    5. Default: 16 weeks
    """
    today = datetime.now().date()
    
    if paper_age_days < 90:
        return today + timedelta(days=7)
    
    if growth > 4:
        return today + timedelta(days=7)
    
    if paper_age_days < 365 and current_citations > 5:
        return today + timedelta(days=14)
    
    if current_citations > 50:
        return today + timedelta(days=7)
    
    return today + timedelta(weeks=16)


def get_papers_needing_enrichment(limit=500, retry_days=30, con=None):
    """Get papers that need OpenAlex enrichment."""
    close_after = False
    if con is None:
        con = get_connection()
        close_after = True
    
    cutoff = datetime.now() - timedelta(days=retry_days)
    
    results = con.execute("""
        SELECT rp.doi, rp.version
        FROM raw_papers rp
        JOIN enrichment_status es ON rp.doi = es.doi
        WHERE es.openalex_enriched = FALSE
          AND (es.last_enrichment_timestamp IS NULL 
               OR es.last_enrichment_timestamp < ?)
        LIMIT ?
    """, (cutoff, limit)).fetchall()
    
    if close_after:
        con.close()
    
    return [{'doi': r[0], 'version': r[1]} for r in results]


def get_papers_due_for_citation_check(limit=500, con=None):
    """Get papers that are due for citation tracking."""
    close_after = False
    if con is None:
        con = get_connection()
        close_after = True
    
    today = datetime.now().date()
    
    results = con.execute("""
        SELECT 
            rp.doi,
            rp.published_date,
            (SELECT cited_by_count 
             FROM paper_citation_snapshots 
             WHERE doi = rp.doi 
             ORDER BY snapshot_date DESC 
             LIMIT 1) as current_citations
        FROM raw_papers rp
        JOIN enrichment_status es ON rp.doi = es.doi
        WHERE es.next_citation_check <= ?
          AND es.openalex_enriched = TRUE
        ORDER BY es.next_citation_check ASC
        LIMIT ?
    """, (today, limit)).fetchall()
    
    if close_after:
        con.close()
    
    return [{
        'doi': r[0],
        'published_date': r[1],
        'current_citations': r[2] or 0
    } for r in results]


def get_last_citation_count(doi, con=None):
    """Get the most recent citation count for a paper."""
    close_after = False
    if con is None:
        con = get_connection()
        close_after = True
    
    result = con.execute("""
        SELECT cited_by_count
        FROM paper_citation_snapshots
        WHERE doi = ?
        ORDER BY snapshot_date DESC
        LIMIT 1
    """, (doi,)).fetchone()
    
    if close_after:
        con.close()
    
    return result[0] if result else 0


def mark_enrichment_complete(doi, con=None):
    """Mark a paper as successfully enriched."""
    close_after = False
    if con is None:
        con = get_connection()
        close_after = True
    
    con.execute("""
        UPDATE enrichment_status
        SET openalex_enriched = TRUE,
            last_enrichment_timestamp = CURRENT_TIMESTAMP
        WHERE doi = ?
    """, (doi,))
    
    if close_after:
        con.close()


def mark_enrichment_attempt(doi, con=None):
    """Record an enrichment attempt (for retry later)."""
    close_after = False
    if con is None:
        con = get_connection()
        close_after = True
    
    con.execute("""
        UPDATE enrichment_status
        SET last_enrichment_timestamp = CURRENT_TIMESTAMP
        WHERE doi = ?
    """, (doi,))
    
    if close_after:
        con.close()


def update_citation_check_schedule(doi, next_check_date, con=None):
    """Update when a paper should be checked next for citations."""
    close_after = False
    if con is None:
        con = get_connection()
        close_after = True
    
    con.execute("""
        UPDATE enrichment_status
        SET next_citation_check = ?
        WHERE doi = ?
    """, (next_check_date, doi))
    
    if close_after:
        con.close()

def count_papers_needing_enrichment(retry_days=30, con=None):
    """Count how many papers need enrichment."""
    close_after = False
    if con is None:
        con = get_connection()
        close_after = True
    
    cutoff = datetime.now() - timedelta(days=retry_days)
    
    result = con.execute("""
        SELECT COUNT(*)
        FROM raw_papers rp
        JOIN enrichment_status es ON rp.doi = es.doi
        WHERE es.openalex_enriched = FALSE
          AND (es.last_enrichment_timestamp IS NULL 
               OR es.last_enrichment_timestamp < ?)
    """, (cutoff,)).fetchone()
    
    if close_after:
        con.close()
    
    return result[0]