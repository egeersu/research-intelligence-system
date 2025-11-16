"""
Reset database - delete everything and start fresh
"""
import os
from pathlib import Path


def reset_database():
    """Delete database file and start fresh"""
    
    db_path = Path("data/hummingbird.db")
    
    print("\n" + "=" * 70)
    print("⚠️  DATABASE RESET")
    print("=" * 70)
    print("\nThis will permanently delete:")
    print(f"  - {db_path}")
    print("\nAll papers, enrichment data, and citation history will be lost.")
    
    confirm = input("\nType 'yes' to confirm: ")
    
    if confirm.lower() != 'yes':
        print("\n❌ Reset cancelled\n")
        return
    
    # Delete database file
    if db_path.exists():
        db_path.unlink()
        print(f"\n✅ Deleted {db_path}")
    else:
        print(f"\n⚠️  {db_path} does not exist")
    
    # Delete any other data files if they exist
    other_files = [
        "data/hummingbird.db.wal",
        "data/hummingbird.db.shm"
    ]
    
    for file_path in other_files:
        p = Path(file_path)
        if p.exists():
            p.unlink()
            print(f"✅ Deleted {p}")
    
    print("\n" + "=" * 70)
    print("🎉 Database reset complete!")
    print("=" * 70)
    print("\nRun this to start fresh:")
    print(" python3 -m jobs.run_pipeline")


if __name__ == "__main__":
    reset_database()