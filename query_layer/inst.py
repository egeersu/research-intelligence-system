import requests

def get_institution_network(institution_name):
    # Search for the main institution using v2 API
    search_url = f"https://api.ror.org/v2/organizations?query={institution_name}"
    response = requests.get(search_url)
    results = response.json()
    
    if not results['items']:
        return None
    
    main_org = results['items'][0]
    ror_id = main_org['id']
    
    # Collect all names from the main organization
    all_names = set()
    for name_obj in main_org.get('names', []):
        all_names.add(name_obj['value'])
    
    # Get related/child organizations
    related_ror_ids = [r['id'] for r in main_org.get('relationships', [])]
    
    # Fetch each related organization to get their names too
    for related_id in related_ror_ids[:10]:  # Limit to avoid too many requests
        try:
            related_url = f"https://api.ror.org/v2/organizations/{related_id}"
            related_resp = requests.get(related_url)
            related_org = related_resp.json()
            
            for name_obj in related_org.get('names', []):
                all_names.add(name_obj['value'])
        except:
            continue
    
    return {
        'main_org_id': ror_id,
        'all_names': sorted(list(all_names)),
        'related_count': len(related_ror_ids)
    }

# Example
harvard = get_institution_network("Harvard University")
if harvard:
    print(f"\nFound {len(harvard['all_names'])} unique names")
    print(f"Related orgs: {harvard['related_count']}")
    print("\nSample names:")
    for name in harvard['all_names'][:15]:
        print(f"  - {name}")