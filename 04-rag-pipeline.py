# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: RAG Pipeline for RBI Circulars
# MAGIC
# MAGIC Builds a Retrieval-Augmented Generation pipeline:
# MAGIC 1. **FAISS Vector Search** — semantic search over RBI circular chunks
# MAGIC 2. **Embedding** — multilingual-e5-small (Hindi/English)
# MAGIC 3. **Retrieval** — Top-k semantic search
# MAGIC 4. **Generation** — Foundation Model API

# COMMAND ----------

# MAGIC %pip install sentence-transformers faiss-cpu --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Chunks source: {catalog}.{schema}.gold_circular_chunks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Circular Chunks

# COMMAND ----------

chunks_df = spark.table(f"{catalog}.{schema}.gold_circular_chunks")
print(f"Total chunks: {chunks_df.count():,}")

chunks_pdf = chunks_df.select("chunk_id", "chunk_text", "citation", "circular_id", "title", "topic_label").toPandas()
print(f"Loaded {len(chunks_pdf)} chunks for embedding")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build FAISS Index

# COMMAND ----------

from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
import pickle
import os

print("Loading multilingual-e5-small (supports Hindi, Tamil, Telugu, etc.)...")
embed_model = SentenceTransformer("intfloat/multilingual-e5-small")
print(f"Model loaded: {embed_model.get_sentence_embedding_dimension()} dimensions")

# Embed all chunks (prefix "passage: " for e5 models)
documents = chunks_pdf["chunk_text"].tolist()
doc_texts = ["passage: " + doc for doc in documents]

print(f"Embedding {len(doc_texts)} chunks...")
embeddings = embed_model.encode(doc_texts, batch_size=32, show_progress_bar=True, normalize_embeddings=True)
print(f"Embeddings shape: {embeddings.shape}")

# Build FAISS index
dimension = embeddings.shape[1]
index = faiss.IndexFlatIP(dimension)
index.add(np.array(embeddings).astype("float32"))
print(f"FAISS index built: {index.ntotal} vectors, {dimension} dimensions")

# Save to Volumes (not DBFS — serverless compatible)
save_dir = f"/Volumes/{catalog}/{schema}/raw_data/faiss_index"
os.makedirs(save_dir, exist_ok=True)

faiss.write_index(index, f"{save_dir}/rbi_circulars.faiss")
with open(f"{save_dir}/chunk_metadata.pkl", "wb") as f:
    pickle.dump(chunks_pdf.to_dict("records"), f)

print(f"Saved FAISS index to {save_dir}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Retrieval Function

# COMMAND ----------

def retrieve_chunks(query: str, top_k: int = 5) -> list:
    """Retrieve relevant RBI circular chunks for a query."""
    query_embedding = embed_model.encode(
        [f"query: {query}"],
        normalize_embeddings=True
    ).astype("float32")

    scores, indices = index.search(query_embedding, top_k)

    results = []
    for i, idx in enumerate(indices[0]):
        if idx < len(chunks_pdf):
            row = chunks_pdf.iloc[idx]
            results.append({
                "chunk_text": row["chunk_text"],
                "citation": row["citation"],
                "title": row["title"],
                "topic": row.get("topic_label", ""),
                "score": float(scores[0][i])
            })
    return results


# Test
test_results = retrieve_chunks("UPI fraud prevention guidelines")
print(f"Retrieved {len(test_results)} chunks for test query")
for i, r in enumerate(test_results):
    print(f"\n--- Result {i+1} (score: {r['score']:.4f}) ---")
    print(f"Title: {r['title']}")
    print(f"Citation: {r['citation']}")
    print(f"Preview: {r['chunk_text'][:200]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. RAG Generation

# COMMAND ----------

def rag_answer(query: str, language: str = "english", top_k: int = 5) -> dict:
    """Full RAG: retrieve → prompt → generate. Returns answer + sources."""
    chunks = retrieve_chunks(query, top_k=top_k)

    if not chunks:
        return {"answer": "No relevant circulars found.", "sources": [], "language": language}

    context_parts = []
    sources = []
    for i, chunk in enumerate(chunks):
        context_parts.append(f"[Source {i+1}: {chunk['citation']}]\n{chunk['chunk_text']}")
        sources.append(chunk["citation"])

    context = "\n\n---\n\n".join(context_parts)

    lang_instr = ""
    if language.lower() in ["hindi", "hi"]:
        lang_instr = "IMPORTANT: Respond entirely in Hindi (Devanagari script). "
    elif language.lower() in ["marathi", "mr"]:
        lang_instr = "IMPORTANT: Respond entirely in Marathi. "

    system_prompt = f"""You are Digital-Artha, an expert on RBI regulations.
{lang_instr}
Answer based ONLY on the provided excerpts. Cite sources with [Source N].
Explain complex regulatory language in simple terms."""

    user_prompt = f"""RBI Circular Excerpts:\n{context}\n\nQuestion: {query}\n\nAnswer:"""

    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        response = w.serving_endpoints.query(
            name="databricks-meta-llama-3-1-70b-instruct",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=800,
            temperature=0.1
        )
        answer = response.choices[0].message.content
    except Exception as e:
        # Fallback: return retrieved chunks without LLM generation
        answer = f"[LLM unavailable on Free Edition: {e}]\n\nRelevant excerpts found:\n\n"
        for i, chunk in enumerate(chunks[:3]):
            answer += f"**[Source {i+1}]** {chunk['citation']}\n{chunk['chunk_text'][:300]}...\n\n"

    return {"answer": answer, "sources": sources, "language": language}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Test RAG

# COMMAND ----------

result = rag_answer("What are the RBI guidelines for digital payment fraud prevention?")
print("=== English Query ===")
print(f"Answer:\n{result['answer']}")
print(f"\nSources: {result['sources']}")

# COMMAND ----------

result_hi = rag_answer("यूपीआई में धोखाधड़ी रोकने के लिए आरबीआई के क्या नियम हैं?", language="hindi")
print("=== Hindi Query ===")
print(f"Answer:\n{result_hi['answer']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Save Config

# COMMAND ----------

import json

rag_config = {
    "vs_mode": "faiss",
    "faiss_index_path": f"/Volumes/{catalog}/{schema}/raw_data/faiss_index/rbi_circulars.faiss",
    "faiss_metadata_path": f"/Volumes/{catalog}/{schema}/raw_data/faiss_index/chunk_metadata.pkl",
    "embedding_model": "intfloat/multilingual-e5-small",
    "llm_endpoint": "databricks-meta-llama-3-1-70b-instruct",
    "top_k": 5,
    "catalog": catalog,
    "schema": schema,
}

# Save as Delta table (more reliable than DBFS on serverless)
spark.createDataFrame([rag_config]).write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.rag_config")
print("RAG config saved.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete
# MAGIC
# MAGIC **Next:** Run `05-loan-eligibility.py`
