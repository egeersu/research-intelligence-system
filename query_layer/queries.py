"""Optimized topic-based queries using vector embeddings"""
import duckdb
from sentence_transformers import SentenceTransformer

DB_PATH = "data/hummingbird.db"

model = SentenceTransformer('all-MiniLM-L6-v2')

def search_topics(keyword, limit=10):
    """Find the best matching topics for a keyword using vector similarity."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    # Use DuckDB to do the similarity calculation
    query_embedding = model.encode(keyword).tolist()
    
    results = conn.execute("""
        SELECT 
            topic_name,
            array_cosine_similarity(embedding, ?::FLOAT[384]) as similarity
        FROM topic_embeddings
        ORDER BY similarity DESC
        LIMIT ?
    """, [query_embedding, limit]).fetchall()
    
    conn.close()
    
    return [(r[0], r[1]) for r in results]

def search_papers_by_topic(topic, limit=20, top_institution_only=False, similarity_threshold=0.6):
    """Find papers matching topic using vector similarity."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    # Step 1: Find similar topics (fast - only ~3k comparisons in DuckDB)
    query_embedding = model.encode(topic).tolist()
    
    similar_topics = conn.execute("""
        SELECT topic_name
        FROM topic_embeddings
        WHERE array_cosine_similarity(embedding, ?::FLOAT[384]) > ?
        ORDER BY array_cosine_similarity(embedding, ?::FLOAT[384]) DESC
        LIMIT 30
    """, [query_embedding, similarity_threshold, query_embedding]).fetchall()
    
    topic_list = [t[0] for t in similar_topics]
    
    if not topic_list:
        conn.close()
        return []
    
    # Step 2: Get papers with those topics (simple SQL)
    institution_filter = ""
    institution_join = ""
    if top_institution_only:
        institution_join = """
            JOIN paper_authors pa ON rp.doi = pa.doi
            JOIN institution_rankings ir ON pa.institution_name = ir.institution_name
        """
    
    papers_query = f"""
        WITH matching_papers AS (
            SELECT DISTINCT doi
            FROM paper_topics
            WHERE topic_name IN ({','.join(['?'] * len(topic_list))})
        ),
        latest_citations AS (
            SELECT doi, cited_by_count
            FROM paper_citation_snapshots
            WHERE (doi, snapshot_date) IN (
                SELECT doi, MAX(snapshot_date)
                FROM paper_citation_snapshots
                GROUP BY doi
            )
        ),
        paper_topics_agg AS (
            SELECT 
                doi,
                STRING_AGG(topic_name, '; ') as topics
            FROM paper_topics
            WHERE doi IN (SELECT doi FROM matching_papers)
            GROUP BY doi
        )
        SELECT 
            rp.doi,
            rp.title,
            rp.published_date,
            rp.category,
            pta.topics,
            COALESCE(lc.cited_by_count, 0) as citations
        FROM raw_papers rp
        JOIN matching_papers mp ON rp.doi = mp.doi
        LEFT JOIN paper_topics_agg pta ON rp.doi = pta.doi
        LEFT JOIN latest_citations lc ON rp.doi = lc.doi
        {institution_join}
        ORDER BY citations DESC
        LIMIT ?
    """
    
    results = conn.execute(papers_query, topic_list + [limit]).fetchall()
    
    papers = [
        {
            'doi': r[0],
            'title': r[1],
            'published_date': r[2],
            'category': r[3],
            'topics': r[4],
            'citations': r[5]
        }
        for r in results
    ]
    
    conn.close()
    return papers


def find_topic_experts(topic, top_n=10, similarity_threshold=0.6, top_institution_only=False):
    """Find authors who publish most on a topic using vector similarity."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    # Step 1: Find similar topics
    query_embedding = model.encode(topic).tolist()
    
    similar_topics = conn.execute("""
        SELECT topic_name
        FROM topic_embeddings
        WHERE array_cosine_similarity(embedding, ?::FLOAT[384]) > ?
        ORDER BY array_cosine_similarity(embedding, ?::FLOAT[384]) DESC
        LIMIT 30
    """, [query_embedding, similarity_threshold, query_embedding]).fetchall()
    
    topic_list = [t[0] for t in similar_topics]
    
    if not topic_list:
        conn.close()
        return []
    
    # Step 2: Find authors with papers on those topics
    institution_filter = ""
    if top_institution_only:
        institution_filter = """
            AND pa.institution_name IN (SELECT institution_name FROM institution_rankings)
        """
    
    experts_query = f"""
        WITH matching_papers AS (
            SELECT DISTINCT doi
            FROM paper_topics
            WHERE topic_name IN ({','.join(['?'] * len(topic_list))})
        ),
        latest_citations AS (
            SELECT doi, cited_by_count
            FROM paper_citation_snapshots
            WHERE (doi, snapshot_date) IN (
                SELECT doi, MAX(snapshot_date)
                FROM paper_citation_snapshots
                GROUP BY doi
            )
        ),
        author_stats AS (
            SELECT 
                pa.author_name,
                pa.institution_name,
                COUNT(DISTINCT pa.doi) as paper_count,
                SUM(COALESCE(lc.cited_by_count, 0)) as total_citations,
                STRING_AGG(DISTINCT pt.topic_name, '; ') as matched_topics
            FROM paper_authors pa
            JOIN matching_papers mp ON pa.doi = mp.doi
            JOIN paper_topics pt ON pa.doi = pt.doi
            LEFT JOIN latest_citations lc ON pa.doi = lc.doi
            WHERE pt.topic_name IN ({','.join(['?'] * len(topic_list))})
                {institution_filter}
            GROUP BY pa.author_name, pa.institution_name
        )
        SELECT 
            author_name,
            institution_name,
            paper_count,
            total_citations,
            matched_topics
        FROM author_stats
        ORDER BY total_citations DESC, paper_count DESC
        LIMIT ?
    """
    
    results = conn.execute(experts_query, topic_list + topic_list + [top_n]).fetchall()
    
    experts = [
        {
            'author_name': r[0],
            'institution_name': r[1],
            'paper_count': r[2],
            'total_citations': r[3],
            'matched_topics': r[4]
        }
        for r in results
    ]
    
    conn.close()
    return experts

def find_experts_with_scores(topic, top_n=10, similarity_threshold=0.6):
    """Find topic experts with comprehensive scoring metrics."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    query_embedding = model.encode(topic).tolist()
    
    similar_topics = conn.execute("""
        SELECT topic_name
        FROM topic_embeddings
        WHERE array_cosine_similarity(embedding, ?::FLOAT[384]) > ?
        ORDER BY array_cosine_similarity(embedding, ?::FLOAT[384]) DESC
        LIMIT 30
    """, [query_embedding, similarity_threshold, query_embedding]).fetchall()
    
    topic_list = [t[0] for t in similar_topics]
    
    if not topic_list:
        conn.close()
        return []
    
    query = f"""
        WITH matching_papers AS (
            SELECT DISTINCT doi
            FROM paper_topics
            WHERE topic_name IN ({','.join(['?'] * len(topic_list))})
        ),
        latest_citations AS (
            SELECT doi, cited_by_count
            FROM paper_citation_snapshots
            WHERE (doi, snapshot_date) IN (
                SELECT doi, MAX(snapshot_date)
                FROM paper_citation_snapshots
                GROUP BY doi
            )
        ),
        author_metrics AS (
            SELECT 
                pa.author_name,
                pa.institution_name,
                COUNT(DISTINCT pa.doi) as paper_count,
                SUM(COALESCE(lc.cited_by_count, 0)) as total_citations,
                MAX(rp.published_date) as most_recent_paper,
                MIN(rp.published_date) as first_paper_date,
                MAX(rp.published_date) as last_paper_date,
                MAX(CASE WHEN ir.institution_name IS NOT NULL THEN 1 ELSE 0 END) as top_institution
            FROM paper_authors pa
            JOIN matching_papers mp ON pa.doi = mp.doi
            JOIN raw_papers rp ON pa.doi = rp.doi
            LEFT JOIN latest_citations lc ON pa.doi = lc.doi
            LEFT JOIN institution_rankings ir ON pa.institution_name = ir.institution_name
            GROUP BY pa.author_name, pa.institution_name
        )
        SELECT 
            author_name,
            institution_name,
            paper_count,
            total_citations,
            most_recent_paper,
            (last_paper_date - first_paper_date) as consistency_days,
            top_institution
        FROM author_metrics
        WHERE paper_count > 0
    """
    
    results = conn.execute(query, topic_list).fetchall()
    conn.close()
    
    if not results:
        return []
    
    # Normalize metrics to 0-1 scale
    from datetime import date
    from math import log
    
    paper_counts = [r[2] for r in results]
    citations = [r[3] for r in results]
    consistencies = [r[5] for r in results]
    
    max_papers = max(paper_counts)
    max_citations = max(citations) if max(citations) > 0 else 1
    max_consistency = max(consistencies) if max(consistencies) > 0 else 1
    
    experts = []
    for r in results:
        months_since = (date.today() - r[4]).days / 30.44
        if months_since <= 6:
            recency_norm = 1.0
        else:
            recency_norm = 6.0 / months_since
        
        paper_norm = r[2] / max_papers
        citation_norm = log(1 + r[3]) / log(1 + max_citations)
        consistency_norm = r[5] / max_consistency if r[5] > 0 else 0
        top_institution = r[6]
        
        # Weighted final score
        final_score = (0.25 * paper_norm + 
                      0.35 * citation_norm + 
                      0.15 * recency_norm + 
                      0.15 * consistency_norm + 
                      0.10 * top_institution)
        
        experts.append({
            'author_name': r[0],
            'institution_name': r[1],
            'paper_count': r[2],
            'paper_norm': round(paper_norm, 3),
            'total_citations': r[3],
            'citation_norm': round(citation_norm, 3),
            'most_recent_paper': r[4],
            'months_since_last_paper': round(months_since, 1),
            'recency_norm': round(recency_norm, 3),
            'consistency_days': r[5],
            'consistency_norm': round(consistency_norm, 3),
            'top_institution': bool(top_institution),
            'final_score': round(final_score, 3)
        })
    
    experts.sort(key=lambda x: x['final_score'], reverse=True)
    
    return experts[:top_n]