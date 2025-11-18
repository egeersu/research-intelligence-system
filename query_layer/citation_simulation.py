import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime


def generate_fake_data():
    """Generate fake citation data for 3 papers over 12 months."""
    dates = [datetime(2025, i, 1) for i in range(1, 13)]
    
    papers = {
        'Paper A (Steady Growth)': [5, 8, 12, 17, 23, 30, 38, 47, 57, 68, 80, 93],
        'Paper B (Explosive)': [3, 4, 6, 9, 15, 25, 42, 68, 103, 147, 201, 268],
        'Paper C (Plateauing)': [10, 18, 24, 28, 31, 33, 34, 35, 36, 36, 37, 37]
    }
    
    return pd.DataFrame(papers, index=dates)


def calculate_metrics(citations_series):
    """
    Calculate traction metrics for a citation series.
    
    Returns:
        DataFrame with velocity, smoothed_velocity, and growth_rate columns
    """
    metrics = pd.DataFrame(index=citations_series.index)
    metrics['citations'] = citations_series
    metrics['velocity'] = citations_series.diff()
    metrics['smoothed_velocity'] = metrics['velocity'].rolling(window=3).mean()
    metrics['growth_rate'] = citations_series.pct_change() * 100
    
    return metrics


def plot_comparison(df):
    """Plot all papers on a single comparison chart."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # Citations
    for col in df.columns:
        axes[0, 0].plot(df.index, df[col], marker='o', label=col, linewidth=2)
    axes[0, 0].set_title('Total Citations Over Time', fontweight='bold')
    axes[0, 0].set_ylabel('Citations')
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)
    
    # Velocity
    for col in df.columns:
        velocity = df[col].diff()
        axes[0, 1].plot(df.index, velocity, marker='o', label=col, linewidth=2)
    axes[0, 1].set_title('Velocity (New Citations/Month)', fontweight='bold')
    axes[0, 1].set_ylabel('New Citations')
    axes[0, 1].legend()
    axes[0, 1].grid(True, alpha=0.3)
    
    # Smoothed Velocity
    for col in df.columns:
        smoothed = df[col].diff().rolling(window=3).mean()
        axes[1, 0].plot(df.index, smoothed, marker='o', label=col, linewidth=2)
    axes[1, 0].set_title('Smoothed Velocity (3-Month Avg)', fontweight='bold')
    axes[1, 0].set_ylabel('Avg New Citations')
    axes[1, 0].legend()
    axes[1, 0].grid(True, alpha=0.3)
    
    # Growth Rate
    for col in df.columns:
        growth = df[col].pct_change() * 100
        axes[1, 1].plot(df.index, growth, marker='o', label=col, linewidth=2)
    axes[1, 1].set_title('Growth Rate (% Change)', fontweight='bold')
    axes[1, 1].set_ylabel('Growth %')
    axes[1, 1].axhline(y=0, color='black', linestyle='--', alpha=0.3)
    axes[1, 1].legend()
    axes[1, 1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig


def summarize_paper(paper_name, citations_series):
    """Return summary dict for a single paper."""
    metrics = calculate_metrics(citations_series)
    
    return {
        'paper': paper_name,
        'total_citations': int(metrics['citations'].iloc[-1]),
        'velocity': float(metrics['velocity'].iloc[-1]),
        'smoothed_velocity': float(metrics['smoothed_velocity'].iloc[-1]),
        'growth_rate': float(metrics['growth_rate'].iloc[-1])
    }