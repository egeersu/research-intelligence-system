"""
Pipeline orchestrator - runs all jobs in sequence
"""
import argparse
from datetime import datetime
from storage.db import get_connection, init_schema, get_stats
from storage.queries import count_papers_needing_enrichment
from jobs.ingest_biorxiv import ingest_papers
from jobs.enrich_openalex import enrich_papers
from jobs.track_citations import track_citations


EMAIL = 'egeersu@gmail.com'


def print_header(message):
    """Print a nice header"""
    print("\n" + "=" * 80)
    print(f"  {message}")
    print("=" * 80)


def run_pipeline(server='biorxiv', days_back=7, stages='all', enrich_batch_size=500):
    """
    Run the complete data pipeline.
    
    Args:
        server: 'biorxiv' or 'medrxiv'
        days_back: How many days to ingest
        stages: 'all', 'ingest', 'enrich', 'citations', or comma-separated
        enrich_batch_size: Papers per enrichment batch
    """
    start_time = datetime.now()
    
    print_header(f"🚀 Starting Hummingbird Pipeline - {start_time.strftime('%Y-%m-%d %H:%M')}")
    
    if stages == 'all':
        run_stages = ['ingest', 'enrich', 'citations']
    else:
        run_stages = [s.strip() for s in stages.split(',')]
    
    print(f"\n📋 Running stages: {', '.join(run_stages)}")
    
    con = get_connection()
    init_schema(con)
    
    if 'ingest' in run_stages:
        print_header("📥 Stage 1: Ingest Papers")
        ingest_papers(server=server, days_back=days_back, con=con)
    
    if 'enrich' in run_stages:
        print_header("🔬 Stage 2: Enrich with OpenAlex")
        
        total_needing = count_papers_needing_enrichment(con=con)
        
        if total_needing == 0:
            print("✅ No papers need enrichment!")
        else:
            print(f"\n📊 {total_needing} papers need enrichment")
            print(f"📦 Processing in batches of {enrich_batch_size}")
            
            run_num = 0
            while True:
                remaining = count_papers_needing_enrichment(con=con)
                if remaining == 0:
                    print(f"\n✅ All papers enriched after {run_num} runs!")
                    break
                
                run_num += 1
                print(f"\n{'─' * 80}")
                print(f"  Enrichment Run {run_num} ({remaining} papers remaining)")
                print(f"{'─' * 80}")
                
                enrich_papers(email=EMAIL, batch_size=enrich_batch_size, con=con)
    
    if 'citations' in run_stages:
        print_header("📊 Stage 3: Track Citation Growth")
        track_citations(email=EMAIL, batch_size=500, con=con)
    
    print_header("📈 Pipeline Summary")
    
    stats = get_stats(con)
    
    print(f"""
  Total papers in database:     {stats['total_papers']:>6,}
  Papers enriched with topics:  {stats['enriched_papers']:>6,} ({stats['enriched_papers']/max(stats['total_papers'], 1)*100:.1f}%)
  Papers with citation history: {stats['papers_with_citations']:>6,}
  Average citations per paper:  {stats['avg_citations']:>6.1f}
    """)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"\n⏱️  Pipeline completed in {duration:.1f} seconds")
    print("=" * 80 + "\n")
    
    con.close()


def main():
    parser = argparse.ArgumentParser(
        description='Run the Hummingbird data pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m jobs.run_pipeline
  python -m jobs.run_pipeline --stages ingest
  python -m jobs.run_pipeline --server medrxiv
  python -m jobs.run_pipeline --days-back 30
  python -m jobs.run_pipeline --enrich-batch-size 1000
        """
    )
    
    parser.add_argument('--server', default='biorxiv', choices=['biorxiv', 'medrxiv'])
    parser.add_argument('--days-back', type=int, default=7)
    parser.add_argument('--stages', default='all')
    parser.add_argument('--enrich-batch-size', type=int, default=500)
    
    args = parser.parse_args()
    
    run_pipeline(
        server=args.server,
        days_back=args.days_back,
        stages=args.stages,
        enrich_batch_size=args.enrich_batch_size
    )


if __name__ == "__main__":
    main()