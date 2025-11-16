# analysis/test_jatsxml.py - COMPARISON VERSION

import cloudscraper
import xml.etree.ElementTree as ET
import sys
sys.path.append('..')

from storage.db import get_connection

def fetch_and_parse_jatsxml(jatsxml_url):
    """Fetch and parse JATS XML bypassing Cloudflare"""
    try:
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'darwin',
                'desktop': True
            }
        )
        
        response = scraper.get(jatsxml_url, timeout=30)
        response.raise_for_status()
        
        if response.text.startswith('<!DOCTYPE html>'):
            print(f"  ❌ Got HTML instead of XML")
            return None
        
        return ET.fromstring(response.content)
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return None

def extract_authors_from_xml(root):
    """Extract author information from JATS XML"""
    authors = []
    
    for contrib in root.findall('.//contrib[@contrib-type="author"]'):
        author = {}
        
        # Name
        name_elem = contrib.find('.//name')
        if name_elem is not None:
            given = name_elem.find('given-names')
            surname = name_elem.find('surname')
            author['given_names'] = given.text if given is not None else ''
            author['surname'] = surname.text if surname is not None else ''
            author['full_name'] = f"{author['given_names']} {author['surname']}".strip()
        
        # Affiliation references
        aff_refs = contrib.findall('.//xref[@ref-type="aff"]')
        author['aff_refs'] = [ref.get('rid') for ref in aff_refs]
        
        authors.append(author)
    
    # Get affiliations
    affiliations = {}
    for aff in root.findall('.//aff'):
        aff_id = aff.get('id')
        institution = aff.find('.//institution')
        if institution is not None:
            aff_text = institution.text or ''
        else:
            aff_text = ''.join(aff.itertext()).strip()
            # Remove label number
            if aff_text and aff_text[0].isdigit():
                parts = aff_text.split(None, 1)
                aff_text = parts[1] if len(parts) > 1 else parts[0]
        
        affiliations[aff_id] = aff_text
    
    # Link authors to affiliations
    for author in authors:
        author['affiliations'] = [
            affiliations.get(ref, '') for ref in author.get('aff_refs', [])
        ]
    
    return authors, affiliations

def compare_api_vs_xml(n=3):
    """Compare what API gives vs what XML gives"""
    con = get_connection()
    
    papers = con.execute(f"""
        SELECT doi, title, authors, author_corresponding, 
               author_corresponding_institution, jatsxml
        FROM papers
        WHERE jatsxml IS NOT NULL
        ORDER BY RANDOM()
        LIMIT {n}
    """).fetchall()
    
    for idx, paper in enumerate(papers):
        doi, title, api_authors, corr_author, corr_inst, jatsxml = paper
        
        print(f"\n{'='*80}")
        print(f"PAPER {idx + 1}: {doi}")
        print(f"{'='*80}")
        print(f"Title: {title}")
        
        # SHOW API DATA
        print(f"\n📄 WHAT THE API GIVES US:")
        print(f"{'─'*80}")
        print(f"Authors (abbreviated):")
        print(f"  {api_authors}")
        print(f"\nCorresponding Author:")
        print(f"  {corr_author}")
        print(f"\nCorresponding Author Institution:")
        print(f"  {corr_inst}")
        
        # FETCH AND SHOW XML DATA
        print(f"\n📄 WHAT THE XML GIVES US:")
        print(f"{'─'*80}")
        print(f"Fetching XML...")
        
        root = fetch_and_parse_jatsxml(jatsxml)
        
        if root is not None:
            authors, affiliations = extract_authors_from_xml(root)
            
            print(f"✅ Found {len(authors)} authors with full names and affiliations:\n")
            
            for i, author in enumerate(authors):
                print(f"{i+1}. {author.get('full_name', 'N/A')}")
                author_affs = author.get('affiliations', [])
                if author_affs:
                    for aff in author_affs:
                        if aff:
                            print(f"   → {aff}")
                else:
                    print(f"   → (no affiliation listed)")
                print()
            
            # Show all unique institutions
            all_institutions = set()
            for author in authors:
                all_institutions.update([aff for aff in author.get('affiliations', []) if aff])
            
            print(f"{'─'*80}")
            print(f"ALL UNIQUE INSTITUTIONS ({len(all_institutions)}):")
            for inst in sorted(all_institutions):
                print(f"  • {inst}")
        else:
            print("❌ Failed to fetch/parse XML")
    
    con.close()

if __name__ == "__main__":
    print("🔬 COMPARING API vs XML DATA")
    print("="*80)
    compare_api_vs_xml(n=5)
    
    print("\n" + "="*80)
    print("💡 SUMMARY:")
    print("  API: Abbreviated names + 1 affiliation (corresponding author only)")
    print("  XML: Full names + ALL author affiliations")