## Environment Setup

Create a virtual environment:
```bash
python3 -m venv venv
```

Activate the virtual environment:
```bash
source venv/bin/activate
```

Install dependencies:
```bash
pip3 install -r requirements.txt
```

## Database Setup
If you'd like to quickly download a complete database (goes back 1000 days) [Recommended]
```bash
python3 download_data.py
```

If you want to start from scratch, the pipeline will create the database for you:
```bash
python3 -m jobs.run_pipeline 
```

If you want to delete the database:
```bash
python3 reset.py 
```

## Query Layer

Run analysis notebook:
```bash
jupyter lab main.ipynb
```

## DuckDB

We're using DuckDB because it's basically "SQLite for analytics", an easily sharable single-file database that's fast at the kinds of queries we need (aggregations, joins, filtering). 

No server to run, just a `.db` file. Bonus: it natively supports arrays, so we can store embeddings directly without tricks.

The database holds papers, topics, authors, citation snapshots over time, and institution rankings.

You can check out the tables:
```bash
python3 view_database.py
```

## OpenAlex

The raw bioRxiv/medRxiv data has messy author info - no institutions per author, missing first names, inconsistent formatting. 

OpenAlex gives us clean, structured data:
- Authors with standardized institutions and ORCIDs
- Curated research topics
- Citation counts

It's a free, open catalog with an API that's actually pleasant to use.

**Coverage**
- 99.5% of papers successfully enriched with OpenAlex data
- 97.8% have research topics
- 99.5% have author information
- 72% have at least one author with institutional affiliation

## Jobs

The pipeline runs in 4 stages, coordinated through an `enrichment_status` table that tracks what each paper needs:

1. **Ingest** - Fetch papers from bioRxiv/medRxiv APIs
   - Handles new papers and version updates (newer versions update existing records)
   - Creates an `enrichment_status` entry for each new paper

2. **Enrich** - Add topics, authors, and initial citation counts from OpenAlex
   - Queries `enrichment_status` for papers where `openalex_enriched = FALSE`
   - Batches DOIs and fetches from OpenAlex
   - Marks papers as enriched and calculates `next_citation_check` date

3. **Track Citations** - Update citation counts over time
   - Queries `enrichment_status` for papers where `next_citation_check <= today`
   - Smart scheduling: weekly checks for papers <90 days old, showing growth (>4 new citations), or popular (>50 citations), otherwise every 16 weeks.
   - Stores snapshots in `paper_citation_snapshots` to track growth over time

4. **Vectorize** - Generate embeddings for topic similarity search
   - Finds topics without embeddings and generates them in batch
   - Uses sentence-transformers (all-MiniLM-L6-v2)

**Key orchestration logic:**
- Each stage only processes what needs processing (incremental)
- Failed enrichments get retry cooldowns (30 days)
- Citation checks are scheduled based on paper age, growth rate, and current citations
- It is safe to rerun. You can set python3 -m jobs.run_pipeline as a cron job if needed.

Try it out:
```bash
python3 -m jobs.run_pipeline 
```

## Query Layer
The query layer uses vector embeddings for semantic search - you can search by concept, not just exact keyword matches.

**Core queries:**
- `search_topics(keyword)` - Find similar research topics using cosine similarity
- `search_papers_by_topic(topic)` - Find papers on similar topics, ranked by citations
- `find_experts_with_scores(topic)` - Rank authors by expertise using a weighted score:
  - 35% citation impact (log-scaled)
  - 25% paper count
  - 15% recency (published in last 6 months?)
  - 15% consistency (active over time?)
  - 10% top institution (QS top-30)

**LinkedIn matching**:
- Uses Swarm API to find LinkedIn profiles given author name + institution

**Citation tracking** (demonstrated with simulated data):
- Velocity (citations/month), smoothed velocity (3-month avg), and growth rate

See `main.ipynb` for examples!

## QS Rankings Data

To identify top institutions, used QS World University Rankings for biological sciences (top 30):

1. Extracted potential OpenAlex institution matches via fuzzy matching against QS names
2. Used Perplexity to verify matches and remove false positives (e.g., "Harvard Medical School" ✓, "Harvard Street Clinic" ✗)
3. Final list saved to `static/qs_rankings.csv`

This powers the `top_institution` flag in expert scoring and institution filtering in queries.