"""Download and extract the pre-built database."""

import os
import sys
import urllib.request
import zipfile

RELEASE_URL = 'https://github.com/egeersu/research-intelligence-system/releases/download/v1.0-data/data.zip'
ZIP_FILE = 'data.zip'

def download_and_extract():
    """Download and extract database."""
    print("📥 Downloading database from GitHub Releases...")
    print(f"   URL: {RELEASE_URL}")
    
    # Download with progress
    def show_progress(count, block_size, total_size):
        percent = int(count * block_size * 100 / total_size)
        sys.stdout.write(f"\r   Progress: {percent}%")
        sys.stdout.flush()
    
    try:
        urllib.request.urlretrieve(RELEASE_URL, ZIP_FILE, show_progress)
        print("\n")
        
        print("📦 Extracting...")
        with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
            zip_ref.extractall('.')
        
        print("🗑️  Cleaning up...")
        os.remove(ZIP_FILE)
        
        print("✅ Done! Database ready at data/hummingbird.db")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    download_and_extract()