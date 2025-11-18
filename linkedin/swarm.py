import aiohttp
import asyncio
from typing import Optional
import re

# Swarm API configuration
API_KEY = "INSERT KEY HERE"
SWARM_API_BASE = "https://bee.theswarm.com/v2"


def clean_institution_name(institution: str) -> str:
    """Clean institution name by removing parentheses and extra info."""
    cleaned = re.sub(r'\s*\([^)]*\)', '', institution)
    cleaned = re.sub(r'^The\s+', '', cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def has_company_in_experience(profile: dict, company_name: str) -> bool:
    """Check if company appears in profile's experience."""
    info = profile.get("profile_info", {})
    clean_search = clean_institution_name(company_name).lower()
    
    # Check current company
    current = info.get("current_company_name", "").lower()
    if clean_search in current or current in clean_search:
        return True
    
    # Check experience
    for exp in info.get("experience", []):
        exp_company = exp.get("company", {}).get("name", "").lower()
        if clean_search in exp_company or exp_company in clean_search:
            return True
    
    return False


def parse_author_name(full_name: str) -> tuple[Optional[str], str]:
    """Parse full name into first and last name."""
    parts = full_name.strip().split()
    
    if len(parts) == 1:
        return None, parts[0]
    elif len(parts) == 2:
        return parts[0], parts[1]
    else:
        return parts[0], parts[-1]


async def get_linkedin_url(full_name: str, institution: str) -> str:
    """Get LinkedIn URL for a person. Returns empty string if not found."""
    
    first_name, last_name = parse_author_name(full_name)
    company_name = clean_institution_name(institution)
    
    name_query = f"{first_name} {last_name}" if first_name else last_name
    
    query = {
        "bool": {
            "must": [
                {"match": {"profile_info.full_name": {"query": name_query, "operator": "AND"}}},
                {
                    "nested": {
                        "path": "profile_info.experience",
                        "query": {"match": {"profile_info.experience.company.name": {"query": company_name}}}
                    }
                }
            ]
        }
    }
    
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    
    try:
        async with aiohttp.ClientSession() as session:
            # Search
            async with session.post(
                f"{SWARM_API_BASE}/profiles/search",
                json={"query": query, "limit": 5, "inNetworkOnly": False},
                headers=headers
            ) as response:
                response.raise_for_status()
                search_data = await response.json()
            
            profile_ids = search_data.get("ids", [])
            if not profile_ids:
                return ""
            
            # Fetch profiles
            async with session.post(
                f"{SWARM_API_BASE}/profiles/fetch",
                json={"ids": profile_ids[:5], "fields": ["profile_info"]},
                headers=headers
            ) as response:
                response.raise_for_status()
                fetch_data = await response.json()
            
            results = fetch_data.get("results", [])
            
            # Find first profile that actually has the company in experience
            for profile in results:
                if has_company_in_experience(profile, institution):
                    linkedin_url = profile.get('profile_info', {}).get('linkedin_url', '')
                    if linkedin_url and not linkedin_url.startswith('http'):
                        linkedin_url = f"https://{linkedin_url}"
                    return linkedin_url
            
            return ""
                
    except Exception as e:
        print(f"Error: {e}")
        return ""


async def main():
    full_name = "Eva Corey"
    institution = "University of Washington"
    
    print(f"Searching for: {full_name} at {institution}")
    
    linkedin_url = await get_linkedin_url(full_name, institution)
    
    if linkedin_url:
        print(f"✅ Found: {linkedin_url}")
    else:
        print("❌ Not found")


if __name__ == "__main__":
    asyncio.run(main())