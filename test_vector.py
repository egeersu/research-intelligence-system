import duckdb
import numpy as np
from sentence_transformers import SentenceTransformer
from sentence_transformers.util import cos_sim

DB_PATH = "data/hummingbird.db"

# Connect to database
conn = duckdb.connect(DB_PATH)

# Load pre-computed embeddings from database
query = "SELECT topic_name, embedding FROM topic_embeddings"
result = conn.execute(query).fetchall()

topics = [row[0] for row in result]
topic_embeddings = np.array([row[1] for row in result], dtype=np.float32)  # ← Cast here

print(f"Loaded {len(topics)} topics with pre-computed embeddings\n")

# Load model (only for encoding the query)
print("Loading model...")
model = SentenceTransformer('all-MiniLM-L6-v2')

# Test with a new topic
new_topic = "obesity treatment"

print(f"Comparing: '{new_topic}'\n")
new_embedding = model.encode(new_topic)

# Calculate similarities
similarities = []
for i, topic in enumerate(topics):
    similarity = cos_sim(new_embedding, topic_embeddings[i]).item()
    similarities.append((similarity, topic))

# Sort by similarity (highest first)
similarities.sort(reverse=True, key=lambda x: x[0])

# Show top 10 matches
print("Top 10 most similar topics:")
print("-" * 80)
for i, (score, topic) in enumerate(similarities[:10], 1):
    print(f"{i:2d}. {score:.4f} - {topic}")

print("-" * 80)

# Close connection
conn.close()