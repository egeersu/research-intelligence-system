"""
OpenAlex API client with smart batching and data extraction.

Usage:
    from openalex.client import OpenAlexClient
    
    client = OpenAlexClient(email="your@email.com")
    enriched_papers = client.get_papers_by_dois(doi_list)
"""

import requests
from typing import List, Dict, Any
import time

class OpenAlexClient:
    """Client for fetching and enriching paper data from OpenAlex."""
    
    BASE_URL = "https://api.openalex.org"
    BATCH_SIZE = 200
    MAX_FILTER_IDS = 50
    RATE_LIMIT = 10
    
    def __init__(self, email: str):
        """
        Initialize OpenAlex client.
        
        Args:
            email: Your email for the polite pool (better response times)
        """
        self.email = email
        self.session = requests.Session()
        self.last_request_time = 0
    
    def _rate_limit(self):
        """Ensure we don't exceed 10 requests/second."""
        elapsed = time.time() - self.last_request_time
        if elapsed < 1.0 / self.RATE_LIMIT:
            time.sleep(1.0 / self.RATE_LIMIT - elapsed)
        self.last_request_time = time.time()
    
    def _batch_dois(self, dois: List[str]) -> List[List[str]]:
        """Split DOI list into batches of MAX_FILTER_IDS."""
        return [dois[i:i + self.MAX_FILTER_IDS] 
                for i in range(0, len(dois), self.MAX_FILTER_IDS)]
    
    def _fetch_batch(self, dois: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch a single batch of papers from OpenAlex.
        
        Args:
            dois: List of DOIs (max 50 for OR filter syntax)
            
        Returns:
            List of paper objects from OpenAlex
        """
        self._rate_limit()
        
        doi_list = [f"https://doi.org/{doi}" for doi in dois]
        doi_filter = "|".join(doi_list)
        
        response = self.session.get(
            f"{self.BASE_URL}/works",
            params={
                "filter": f"doi:{doi_filter}",
                "per_page": self.BATCH_SIZE,
                "mailto": self.email
            }
        )

        if response.status_code == 429:
            print(f"   ⚠️  Rate limited on batch {i}")
        if response.status_code != 200:
            print(f"⚠️  API Error {response.status_code} for batch")
            return []
        
        return response.json()['results']
    
    def get_papers_by_dois(self, dois: List[str], verbose: bool = True) -> List[Dict[str, Any]]:
        """
        Fetch papers from OpenAlex with smart batching.
        
        Args:
            dois: List of DOIs to fetch
            verbose: Print progress
            
        Returns:
            List of enriched paper dictionaries
        """
        batches = self._batch_dois(dois)
        
        if verbose:
            print(f"📦 Fetching {len(dois)} papers in {len(batches)} batches...")
        
        all_papers = []
        
        for batch_num, batch in enumerate(batches, 1):
            if verbose:
                print(f"   Batch {batch_num}/{len(batches)}...", end=" ")
            
            results = self._fetch_batch(batch)
            all_papers.extend(results)
            
            if verbose:
                print(f"✅ {len(results)} papers")
        
        if verbose:
            print(f"\n✨ Total fetched: {len(all_papers)}/{len(dois)} papers\n")
        
        enriched = []
        for paper in all_papers:
            enriched.append(self.extract_paper_metadata(paper))
        
        return enriched
    
    @staticmethod
    def extract_topics(paper: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract topic information from a paper."""
        topics = []
        for topic in paper.get('topics', []):
            topics.append({
                'name': topic['display_name'],
                'field': topic['field']['display_name']
            })
        return topics
    
    @staticmethod
    def extract_authors(paper: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract author information from a paper."""
        authors = []
        for authorship in paper.get('authorships', []):
            author_data = authorship.get('author')
            
            if not author_data:
                continue
            
            author_id = author_data.get('id')
            if author_id:
                author_id = author_id.split('/')[-1]
            else:
                author_id = f"UNKNOWN_{hash(author_data.get('display_name', 'unknown'))}"
            
            institution_name = None
            institutions = authorship.get('institutions', [])
            if institutions:
                institution_name = institutions[0].get('display_name')
            
            authors.append({
                'id': author_id,
                'name': author_data.get('display_name') or 'Unknown Author',
                'orcid': author_data.get('orcid'),
                'institution_name': institution_name
            })
        
        return authors
    
    @staticmethod
    def extract_paper_metadata(paper: Dict[str, Any]) -> Dict[str, Any]:
        """Extract core metadata from a paper."""
        return {
            'doi': paper.get('doi', '').replace('https://doi.org/', ''),
            'title': paper.get('title'),
            'cited_by_count': paper.get('cited_by_count', 0),
            'topics': OpenAlexClient.extract_topics(paper),
            'authors': OpenAlexClient.extract_authors(paper)
        }