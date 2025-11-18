"""
Job 4: Vectorize topic names for similarity search
"""
from sentence_transformers import SentenceTransformer
from storage.db import get_connection
from storage.queries import get_topics_needing_embeddings, store_topic_embedding


def vectorize_topics(con=None):
    """Generate embeddings for topics that don't have them yet."""
    close_after = False
    if con is None:
        con = get_connection()
        close_after = True
    
    topics = get_topics_needing_embeddings(con=con)
    
    if not topics:
        print("✅ All topics already vectorized!")
        if close_after:
            con.close()
        return
    
    print(f"\n🔢 Vectorizing {len(topics)} topics")
    print("=" * 70)
    
    print("Loading model...")
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    print("Generating embeddings...")
    embeddings = model.encode(topics, show_progress_bar=True)
    
    print("Storing embeddings...")
    for topic, embedding in zip(topics, embeddings):
        store_topic_embedding(topic, embedding, con=con)
    
    print(f"✅ Vectorized {len(topics)} topics")
    print("=" * 70)
    
    if close_after:
        con.close()