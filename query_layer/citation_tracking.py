import duckdb
import pandas as pd
from datetime import datetime, timedelta
import random

DB_PATH = "data/hummingbird.db"

def generate_fake_weekly_snapshots():
    """Generate fake weekly citation data for neural papers"""
    
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    query = """
    SELECT 
        pcs.doi,
        rp.published_date,
        pcs.cited_by_count as final_citations,
        DATEDIFF('week', rp.published_date, CURRENT_DATE) as weeks_old
    FROM paper_citation_snapshots pcs
    JOIN raw_papers rp ON pcs.doi = rp.doi
    JOIN paper_topics pt ON rp.doi = pt.doi
    WHERE pt.topic_name = 'Neural dynamics and brain function'
        AND pcs.cited_by_count >= 5
    """
    
    papers = conn.execute(query).fetchdf()
    conn.close()
    
    snapshots = []
    
    for _, paper in papers.iterrows():
        doi = paper['doi']
        pub_date = paper['published_date']
        final_cites = int(paper['final_citations'])
        weeks_old = int(paper['weeks_old'])
        
        if weeks_old == 0:
            weeks_old = 1
        
        pattern = random.choices(
            ['ignored', 'steady', 'viral', 'plateau'],
            weights=[0.20, 0.45, 0.10, 0.25]
        )[0]
        
        weekly_new_citations = []
        
        for week in range(weeks_old + 1):
            progress = week / weeks_old
            
            if pattern == 'steady':
                weight = 1.0
            elif pattern == 'viral':
                if progress < 0.8:
                    weight = 0.1 * (progress ** 2)
                else:
                    weight = 10.0 * ((progress - 0.8) / 0.2) ** 2
            elif pattern == 'plateau':
                if progress < 0.3:
                    weight = 5.0 * (1.0 - (progress / 0.3))
                else:
                    weight = 0.2
            else:
                weight = 0.5 + 0.5 * random.random()
            
            weekly_new_citations.append(max(weight, 0))
        
        total_weight = sum(weekly_new_citations)
        if total_weight > 0:
            weekly_new_citations = [int(w / total_weight * final_cites) for w in weekly_new_citations]
        else:
            weekly_new_citations = [0] * (weeks_old + 1)
            weekly_new_citations[-1] = final_cites
        
        diff = final_cites - sum(weekly_new_citations)
        if diff != 0:
            if pattern == 'viral':
                weekly_new_citations[-1] += diff
            elif pattern == 'plateau':
                weekly_new_citations[min(5, len(weekly_new_citations)-1)] += diff
            else:
                weekly_new_citations[-1] += diff
        
        cumulative = 0
        for week in range(weeks_old + 1):
            snapshot_date = pub_date + timedelta(weeks=week)
            cumulative += weekly_new_citations[week]
            
            snapshots.append({
                'doi': doi,
                'snapshot_date': snapshot_date,
                'cited_by_count': cumulative
            })
    
    df = pd.DataFrame(snapshots)
    return df


def calculate_traction_metrics(df):
    """
    Calculate simple traction metrics:
    1. Velocity: citations/week in last 4 weeks
    2. Smoothed velocity: citations/week in last 8 weeks (moving average)
    3. Growth rate: smoothed velocity / historical average velocity
    """
    
    # Sort by doi and date
    df_sorted = df.sort_values(['doi', 'snapshot_date']).copy()
    
    # Calculate weekly growth
    df_sorted['weekly_growth'] = df_sorted.groupby('doi')['cited_by_count'].diff().fillna(0)
    
    results = []
    
    for doi in df_sorted['doi'].unique():
        paper_df = df_sorted[df_sorted['doi'] == doi].copy()
        
        weeks_old = len(paper_df) - 1
        if weeks_old < 4:  # Need at least 4 weeks
            continue
        
        total_citations = paper_df['cited_by_count'].iloc[-1]
        if total_citations == 0:
            continue
        
        # 1. Velocity (last 4 weeks)
        recent_growth = paper_df.tail(4)['weekly_growth'].sum()
        velocity = recent_growth / 4
        
        # 2. Smoothed velocity (last 8 weeks if available, else last 4)
        if weeks_old >= 8:
            smoothed_growth = paper_df.tail(8)['weekly_growth'].sum()
            smoothed_velocity = smoothed_growth / 8
            confidence = 'high'
        else:
            smoothed_velocity = velocity
            confidence = 'low'
        
        # 3. Historical velocity
        historical_velocity = total_citations / weeks_old
        
        # 4. Growth rate (normalized)
        growth_rate = smoothed_velocity / max(historical_velocity, 0.1)
        
        results.append({
            'doi': doi,
            'total_citations': total_citations,
            'weeks_old': weeks_old,
            'velocity': velocity,
            'smoothed_velocity': smoothed_velocity,
            'historical_velocity': historical_velocity,
            'growth_rate': growth_rate,
            'confidence': confidence
        })
    
    return pd.DataFrame(results)


if __name__ == "__main__":
    print("🔄 Generating fake data...")
    fake_data = generate_fake_weekly_snapshots()
    print(f"✅ Generated {len(fake_data)} snapshots for {fake_data['doi'].nunique()} papers\n")
    
    print("📊 Calculating traction metrics...")
    metrics_df = calculate_traction_metrics(fake_data)
    
    print("\n" + "="*100)
    print("🚀 TOP PAPERS BY GROWTH RATE (gaining traction)")
    print("="*100)
    top_papers = metrics_df.sort_values('growth_rate', ascending=False).head(20)
    print(top_papers.to_string(index=False))
    
    print("\n" + "="*100)
    print("⚡ TOP PAPERS BY VELOCITY (most active right now)")
    print("="*100)
    top_velocity = metrics_df.sort_values('velocity', ascending=False).head(20)
    print(top_velocity.to_string(index=False))