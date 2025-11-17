"""
Hummingbird Research Intelligence System - Demo

This script demonstrates the query capabilities of the system.
Modify the variables below to test different queries.
"""

from queries import (
    search_papers_by_topic, 
    get_trending_topics, 
    find_topic_experts, 
    search_topics
)
import pandas as pd

# ============================================================================
# CONFIGURATION - Modify these to test different queries
# ============================================================================

# Topics to search
SEARCH_TOPICS = ["obesity treatment"]
EXPERT_TOPIC = "immune cell function"
TRENDING_WEEKS = 8

# Limits
PAPER_LIMIT = 10
EXPERT_LIMIT = 15
TRENDING_LIMIT = 20

# Filters
USE_TOP_INSTITUTIONS = True

# ============================================================================
# DEMO QUERIES
# ============================================================================

print("=" * 80)
print("HUMMINGBIRD RESEARCH INTELLIGENCE SYSTEM - DEMO")
print("=" * 80)

# 1. Search Papers by Topic
print(f"\n{'='*80}")
print(f"1. PAPERS ON TOPICS: {SEARCH_TOPICS}")
print(f"{'='*80}\n")

print("--- All Institutions ---")
results = search_papers_by_topic(SEARCH_TOPICS, limit=PAPER_LIMIT)
df = pd.DataFrame(results)
if not df.empty:
    print(df[['title', 'topics', 'citations', 'published_date']].to_string())
    print(f"\nTotal: {len(df)} papers")
else:
    print("No papers found")

if USE_TOP_INSTITUTIONS:
    print("\n--- Top 30 Institutions Only ---")
    results = search_papers_by_topic(SEARCH_TOPICS, limit=PAPER_LIMIT, top_institution_only=True)
    df = pd.DataFrame(results)
    if not df.empty:
        print(df[['title', 'topics', 'citations', 'published_date']].to_string())
        print(f"\nTotal: {len(df)} papers")
    else:
        print("No papers found from top institutions")

# 2. Trending Topics
print(f"\n{'='*80}")
print(f"2. TRENDING TOPICS (Last {TRENDING_WEEKS} weeks)")
print(f"{'='*80}\n")

results = get_trending_topics(weeks=TRENDING_WEEKS, top_n=TRENDING_LIMIT)
df = pd.DataFrame(results)
if not df.empty:
    print(df.to_string())
else:
    print("No trending topics found")

# 3. Find Topic Experts
print(f"\n{'='*80}")
print(f"3. EXPERTS IN: '{EXPERT_TOPIC}'")
print(f"{'='*80}\n")

# First, find the matching topic
matched_topic = search_topics(EXPERT_TOPIC)
print(f"Matched Topic: {matched_topic}\n")

print("--- All Institutions ---")
results = find_topic_experts(EXPERT_TOPIC, top_n=EXPERT_LIMIT)
df = pd.DataFrame(results)
if not df.empty:
    print(df[['author_name', 'institution_name', 'paper_count', 'total_citations']].to_string())
    print(f"\nTotal: {len(df)} experts")
else:
    print("No experts found")

if USE_TOP_INSTITUTIONS:
    print("\n--- Top 30 Institutions Only ---")
    results = find_topic_experts(EXPERT_TOPIC, top_n=EXPERT_LIMIT, top_institution_only=True)
    df = pd.DataFrame(results)
    if not df.empty:
        print(df[['author_name', 'institution_name', 'paper_count', 'total_citations']].to_string())
        print(f"\nTotal: {len(df)} experts")
    else:
        print("No experts found from top institutions")

print(f"\n{'='*80}")
print("Demo Complete!")
print(f"{'='*80}\n")