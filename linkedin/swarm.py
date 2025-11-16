import aiohttp
import asyncio
from typing import Dict, List, Optional, Any
import sys
import os
import re

# Add parent directory to path to import storage module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from storage.db import get_connection

# Swarm API configuration
API_KEY = "WXPkNo2Bc15lJtFyBVcTD7VjYJuMuoxK183oM8PG"
SWARM_API_BASE = "https://bee.theswarm.com/v2"

def clean_institution_name(institution: str) -> str:
    """Clean institution name by removing parentheses and extra info."""
    # Remove content in parentheses
    cleaned = re.sub(r'\s*\([^)]*\)', '', institution)
    # Remove "The" at the beginning
    cleaned = re.sub(r'^The\s+', '', cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def has_company_in_experience(profile: Dict[str, Any], company_name: str) -> bool:
    """Check if company appears in profile's experience or current company."""
    info = profile.get("profile_info", {})
    
    # Clean the search company name
    clean_search = clean_institution_name(company_name).lower()
    
    # Check current company
    current_company = info.get("current_company_name", "").lower()
    if clean_search in current_company or current_company in clean_search:
        return True
    
    # Check experience history
    for exp in info.get("experience", []):
        exp_company = exp.get("company", {}).get("name", "").lower()
        if clean_search in exp_company or exp_company in clean_search:
            return True
    
    return False


async def search_swarm_by_name(
    last_name: str,
    first_name: Optional[str] = None,
    company_name: Optional[str] = None,
    current_company_only: bool = False,
    past_company_only: bool = False,
    limit: int = 10,
    verify_company: bool = True
) -> Dict[str, Any]:
    """Search Swarm API for profiles by name and optional company filter."""
    
    name_query = f"{first_name} {last_name}" if first_name else last_name
    
    # Clean company name if provided
    if company_name:
        company_name = clean_institution_name(company_name)
    
    # Build query conditions
    must = [{"match": {"profile_info.full_name": {"query": name_query, "operator": "AND"}}}]
    must_not = []
    
    if company_name:
        if current_company_only:
            must.append({"match": {"profile_info.current_company_name": {"query": company_name}}})
        elif past_company_only:
            must.append({
                "nested": {
                    "path": "profile_info.experience",
                    "query": {"match": {"profile_info.experience.company.name": {"query": company_name}}}
                }
            })
            must_not.append({"match": {"profile_info.current_company_name": {"query": company_name}}})
        else:
            must.append({
                "nested": {
                    "path": "profile_info.experience",
                    "query": {"match": {"profile_info.experience.company.name": {"query": company_name}}}
                }
            })
    
    query = {"bool": {"must": must}}
    if must_not:
        query["bool"]["must_not"] = must_not
    
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    
    try:
        async with aiohttp.ClientSession() as session:
            # Search
            async with session.post(
                f"{SWARM_API_BASE}/profiles/search",
                json={"query": query, "limit": min(limit, 100), "inNetworkOnly": False},
                headers=headers
            ) as response:
                response.raise_for_status()
                search_data = await response.json()
            
            profile_ids = search_data.get("ids", [])
            if not profile_ids:
                return {"profiles": [], "total_count": 0}
            
            # Fetch profiles
            async with session.post(
                f"{SWARM_API_BASE}/profiles/fetch",
                json={"ids": profile_ids[:limit], "fields": ["profile_info", "connections"]},
                headers=headers
            ) as response:
                response.raise_for_status()
                fetch_data = await response.json()
            
            profiles = fetch_data.get("results", [])
            
            # Filter profiles to verify company appears in experience
            if company_name and verify_company:
                profiles = [p for p in profiles if has_company_in_experience(p, company_name)]
            
            return {
                "profiles": profiles,
                "total_count": len(profiles)
            }
                
    except Exception as e:
        return {"error": str(e), "profiles": [], "total_count": 0}


def format_profile_results(results: Dict[str, Any]) -> str:
    """Format search results into readable string."""
    
    if "error" in results:
        return f"Error: {results['error']}"
    
    profiles = results.get("profiles", [])
    if not profiles:
        return "No profiles found"
    
    output = [f"Found {results.get('total_count', 0)} profiles (showing {len(profiles)}):\n"]
    
    for i, profile in enumerate(profiles, 1):
        info = profile.get("profile_info", {})
        
        name = info.get("full_name", "Unknown")
        title = info.get("current_title", "N/A")
        company = info.get("current_company_name", "N/A")
        location = info.get("current_location", "N/A")
        linkedin = info.get("linkedin_url", "")
        if linkedin and not linkedin.startswith("http"):
            linkedin = f"https://{linkedin}"
        
        output.append(f"\n{i}. {name}\n")
        output.append(f"   {title} at {company}\n")
        output.append(f"   Location: {location}\n")
        if linkedin:
            output.append(f"   LinkedIn: {linkedin}\n")
        
        # Email
        emails = info.get("emails", [])
        if emails:
            output.append(f"   Email: {emails[0]}\n")
        
        # Recent experience
        experience = info.get("experience", [])[:3]
        if experience:
            output.append("   Experience:\n")
            for exp in experience:
                exp_company = exp.get("company", {}).get("name", "Unknown")
                exp_title = exp.get("title", "Unknown")
                output.append(f"      • {exp_title} at {exp_company}\n")
    
    return "".join(output)


def parse_author_name(full_name: str) -> tuple[Optional[str], str]:
    """Parse full name into first and last name, handling middle initials."""
    parts = full_name.strip().split()
    
    if len(parts) == 1:
        return None, parts[0]
    elif len(parts) == 2:
        return parts[0], parts[1]
    elif len(parts) >= 3:
        # Handle middle initials/names - take first word as first name, last word as last name
        return parts[0], parts[-1]
    
    return None, full_name


async def main():
    # Connect to database
    con = get_connection()
    
    # Get a random author with an institution
    result = con.execute("""
        SELECT author_name, institution_name 
        FROM paper_authors 
        WHERE institution_name IS NOT NULL 
        ORDER BY RANDOM() 
        LIMIT 1
    """).fetchone()
    
    if not result:
        print("❌ No authors found in database")
        con.close()
        return
    
    author_name, institution = result
    con.close()
    
    # Parse name
    first_name, last_name = parse_author_name(author_name)
    
    # Clean institution
    cleaned_institution = clean_institution_name(institution)
    
    print("=" * 80)
    print("🔍 SWARM SEARCH TEST")
    print("=" * 80)
    print(f"\n📋 Selected Author: {author_name}")
    print(f"🏢 Institution (raw): {institution}")
    print(f"🏢 Institution (cleaned): {cleaned_institution}")
    print(f"🔤 Parsed: First='{first_name}', Last='{last_name}'")
    print("\n" + "=" * 80)
    print("Searching Swarm API...")
    print("=" * 80 + "\n")
    
    # Search on Swarm
    results = await search_swarm_by_name(
        first_name=first_name,
        last_name=last_name,
        company_name=institution,
        verify_company=True,
        limit=5
    )
    
    print(format_profile_results(results))


if __name__ == "__main__":
    asyncio.run(main())