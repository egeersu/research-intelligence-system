from queries import search_papers_by_topic, get_trending_topics, find_topic_experts
import json

print("\n=== TEST 1: Search Papers ===")
results = search_papers_by_topic(["CRISPR"], limit=5)
print(f"Found {len(results)} papers")
for r in results[:3]:
    print(f"- {r['title'][:60]}... ({r['citations']} citations)")

print("\n=== TEST 2: Trending Topics ===")
results = get_trending_topics(weeks=8, top_n=10)
print(f"Found {len(results)} trending topics")
for r in results[:5]:
    print(f"- {r['topic_name']}: {r['citation_growth']} citations growth ({r['paper_count']} papers)")

print("\n=== TEST 3: Find Experts ===")
results = find_topic_experts("machine learning", top_n=10)
print(f"Found {len(results)} experts")
for r in results[:5]:
    print(f"- {r['author_name']}: {r['paper_count']} papers, {r['total_citations']} citations")

