import duckdb
from datetime import datetime, timedelta

DB_PATH = "data/hummingbird.db"

def search_papers_by_topic(topics, limit=20):
    """Find papers matching topic keywords."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    topic_filters = " OR ".join([f"LOWER(pt.topic_name) LIKE '%{t.lower()}%'" for t in topics])
    
    query = f"""
    SELECT 
        rp.doi,
        rp.title,
        rp.published_date,
        rp.category,
        STRING_AGG(DISTINCT pt.topic_name, '; ') as topics,
        COALESCE(pcs.cited_by_count, 0) as citations
    FROM raw_papers rp
    JOIN paper_topics pt ON rp.doi = pt.doi
    LEFT JOIN paper_citation_snapshots pcs ON rp.doi = pcs.doi
    WHERE {topic_filters}
    GROUP BY rp.doi, rp.title, rp.published_date, rp.category, pcs.cited_by_count
    ORDER BY pcs.cited_by_count DESC
    LIMIT {limit}
    """
    
    results = conn.execute(query).fetchdf().to_dict('records')
    conn.close()
    return results


def get_trending_topics(weeks=8, top_n=20):
    """Get topics with most citation growth."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    cutoff_date = (datetime.now() - timedelta(weeks=weeks)).strftime('%Y-%m-%d')
    
    query = f"""
    SELECT 
        pt.topic_name,
        COUNT(DISTINCT pt.doi) as paper_count,
        SUM(pcs.growth_since_last) as citation_growth
    FROM paper_topics pt
    JOIN paper_citation_snapshots pcs ON pt.doi = pcs.doi
    WHERE pcs.snapshot_date >= '{cutoff_date}'
    GROUP BY pt.topic_name
    HAVING citation_growth > 0
    ORDER BY citation_growth DESC
    LIMIT {top_n}
    """
    
    results = conn.execute(query).fetchdf().to_dict('records')
    conn.close()
    return results


def find_topic_experts(topic, top_n=10):
    """Find authors who publish most on a topic."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    query = f"""
    SELECT 
        pa.author_name,
        pa.institution_name,
        COUNT(DISTINCT pa.doi) as paper_count,
        SUM(COALESCE(pcs.cited_by_count, 0)) as total_citations
    FROM paper_authors pa
    JOIN paper_topics pt ON pa.doi = pt.doi
    LEFT JOIN paper_citation_snapshots pcs ON pa.doi = pcs.doi
    WHERE LOWER(pt.topic_name) LIKE '%{topic.lower()}%'
    GROUP BY pa.author_name, pa.institution_name
    ORDER BY total_citations DESC, paper_count DESC
    LIMIT {top_n}
    """
    
    results = conn.execute(query).fetchdf().to_dict('records')
    conn.close()
    return results
