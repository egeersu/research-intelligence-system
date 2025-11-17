CREATE TABLE IF NOT EXISTS raw_papers (
    doi TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    title TEXT NOT NULL,
    abstract TEXT,
    authors TEXT,
    author_corresponding TEXT,
    author_corresponding_institution TEXT,
    published_date DATE NOT NULL,
    category TEXT,
    server TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS enrichment_status (
    doi TEXT PRIMARY KEY,
    openalex_enriched BOOLEAN DEFAULT FALSE,
    last_enrichment_timestamp TIMESTAMP,
    next_citation_check DATE
);

CREATE TABLE IF NOT EXISTS paper_topics (
    doi TEXT NOT NULL,
    topic_name TEXT NOT NULL,
    field TEXT,
    PRIMARY KEY (doi, topic_name)
);

CREATE TABLE IF NOT EXISTS paper_authors (
    doi TEXT NOT NULL,
    author_id TEXT NOT NULL,
    author_name TEXT NOT NULL,
    orcid TEXT,
    institution_name TEXT,
    PRIMARY KEY (doi, author_id)
);

CREATE TABLE IF NOT EXISTS paper_citation_snapshots (
    doi TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    cited_by_count INTEGER NOT NULL,
    growth_since_last INTEGER,
    PRIMARY KEY (doi, snapshot_date)
);

CREATE TABLE IF NOT EXISTS institution_rankings (
    institution_name TEXT PRIMARY KEY,
    ranking INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rankings ON institution_rankings(ranking);

CREATE INDEX IF NOT EXISTS idx_papers_published_date ON raw_papers(published_date);
CREATE INDEX IF NOT EXISTS idx_papers_category ON raw_papers(category);
CREATE INDEX IF NOT EXISTS idx_papers_server ON raw_papers(server);

CREATE INDEX IF NOT EXISTS idx_enrichment_status ON enrichment_status(openalex_enriched, last_enrichment_timestamp);
CREATE INDEX IF NOT EXISTS idx_citation_check_due ON enrichment_status(next_citation_check);

CREATE INDEX IF NOT EXISTS idx_topics_name ON paper_topics(topic_name);
CREATE INDEX IF NOT EXISTS idx_topics_field ON paper_topics(field);

CREATE INDEX IF NOT EXISTS idx_authors_name ON paper_authors(author_name);
CREATE INDEX IF NOT EXISTS idx_authors_institution ON paper_authors(institution_name);

CREATE INDEX IF NOT EXISTS idx_snapshots_latest ON paper_citation_snapshots(doi, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_rankings ON institution_rankings(ranking);
