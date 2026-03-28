# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: RAG Pipeline for RBI Circulars
# MAGIC
# MAGIC Builds a Retrieval-Augmented Generation pipeline over RBI circulars:
# MAGIC 1. **Vector Search Index** — Databricks managed (auto-syncs with Delta) or FAISS fallback
# MAGIC 2. **Embedding** — multilingual-e5-small for Hindi/English queries
# MAGIC 3. **Retrieval** — Top-k semantic search
# MAGIC 4. **Generation** — Foundation Model API (Llama 3.1 70B)
# MAGIC 5. **Translation** — IndicTrans2 for multilingual output

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %pip install sentence-transformers faiss-cpu --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
dbutils.widgets.dropdown("vector_search_mode", "databricks", ["databricks", "faiss"], "Vector Search Mode")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
vs_mode = dbutils.widgets.get("vector_search_mode")

from databricks.sdk import WorkspaceClient
import mlflow

w = WorkspaceClient()
host = w.config.host

username = spark.sql("SELECT current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{username}/digital-artha-rag")

print(f"Vector search mode: {vs_mode}")
print(f"Chunks source: {catalog}.{schema}.gold_circular_chunks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Circular Chunks

# COMMAND ----------

chunks_df = spark.table(f"{catalog}.{schema}.gold_circular_chunks")
print(f"Total chunks: {chunks_df.count():,}")
display(chunks_df.groupBy("topic_label").count().orderBy(F.desc("count")))

# Collect to driver for embedding
chunks_pdf = chunks_df.select("chunk_id", "chunk_text", "citation", "circular_id", "title", "topic_label").toPandas()
print(f"Loaded {len(chunks_pdf)} chunks for embedding")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2a. Databricks Managed Vector Search (Preferred)

# COMMAND ----------

if vs_mode == "databricks":
    vs_endpoint_name = "digital_artha_vs"
    vs_index_name = f"{catalog}.{schema}.circular_chunks_vs_index"

    # Create Vector Search endpoint (idempotent)
    try:
        w.vector_search_endpoints.create_endpoint(name=vs_endpoint_name)
        print(f"Created VS endpoint: {vs_endpoint_name}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"VS endpoint already exists: {vs_endpoint_name}")
        else:
            print(f"VS endpoint error: {e}")
            print("Falling back to FAISS mode")
            vs_mode = "faiss"

    if vs_mode == "databricks":
        # Create Delta Sync index — auto-syncs when gold_circular_chunks updates
        try:
            w.vector_search_indexes.create_index(
                name=vs_index_name,
                endpoint_name=vs_endpoint_name,
                primary_key="chunk_id",
                index_type="DELTA_SYNC",
                delta_sync_index_spec={
                    "source_table": f"{catalog}.{schema}.gold_circular_chunks",
                    "embedding_source_columns": [
                        {
                            "name": "chunk_text",
                            "embedding_model_endpoint_name": "databricks-bge-large-en"
                        }
                    ],
                    "pipeline_type": "TRIGGERED",
                    "columns_to_sync": ["chunk_id", "chunk_text", "citation", "circular_id", "title", "topic_label"]
                }
            )
            print(f"Created VS index: {vs_index_name}")
            print("Index is syncing... This may take a few minutes.")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"VS index already exists: {vs_index_name}")
            else:
                print(f"VS index error: {e}")
                print("Falling back to FAISS mode")
                vs_mode = "faiss"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2b. FAISS Vector Search (Fallback)

# COMMAND ----------

if vs_mode == "faiss":
    from sentence_transformers import SentenceTransformer
    import faiss
    import numpy as np
    import pickle

    print("Building FAISS index with multilingual-e5-small...")

    # Load multilingual embedding model (supports Hindi, Tamil, Telugu, etc.)
    embed_model = SentenceTransformer("intfloat/multilingual-e5-small")
    print(f"Model loaded: 384 dimensions, multilingual support")

    # Embed all chunks (prefix "passage: " for e5 models)
    documents = chunks_pdf["chunk_text"].tolist()
    doc_texts = ["passage: " + doc for doc in documents]

    print(f"Embedding {len(doc_texts)} chunks...")
    embeddings = embed_model.encode(doc_texts, batch_size=32, show_progress_bar=True, normalize_embeddings=True)
    print(f"Embeddings shape: {embeddings.shape}")

    # Build FAISS index (Inner Product for normalized vectors = cosine similarity)
    dimension = embeddings.shape[1]
    index = faiss.IndexFlatIP(dimension)  # IP for cosine sim with normalized vectors
    index.add(np.array(embeddings).astype("float32"))
    print(f"FAISS index built: {index.ntotal} vectors, {dimension} dimensions")

    # Save to DBFS
    faiss_path = "/dbfs/FileStore/digital_artha/faiss_index/rbi_circulars.faiss"
    metadata_path = "/dbfs/FileStore/digital_artha/faiss_index/chunk_metadata.pkl"

    import os
    os.makedirs(os.path.dirname(faiss_path), exist_ok=True)

    faiss.write_index(index, faiss_path)
    with open(metadata_path, "wb") as f:
        pickle.dump(chunks_pdf.to_dict("records"), f)

    print(f"Saved FAISS index to {faiss_path}")
    print(f"Saved metadata to {metadata_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Retrieval Function

# COMMAND ----------

def retrieve_chunks(query: str, top_k: int = 5) -> list:
    """
    Retrieve relevant RBI circular chunks for a query.
    Works with both Databricks VS and FAISS.
    """
    if vs_mode == "databricks":
        # Databricks managed Vector Search
        results = w.vector_search_indexes.query_index(
            index_name=vs_index_name,
            columns=["chunk_id", "chunk_text", "citation", "title", "topic_label"],
            query_text=query,
            num_results=top_k
        )
        return [
            {
                "chunk_text": row["chunk_text"],
                "citation": row["citation"],
                "title": row["title"],
                "topic": row.get("topic_label", ""),
                "score": row.get("score", 0.0)
            }
            for row in results.get("result", {}).get("data_array", [])
        ] if isinstance(results, dict) else []

    else:
        # FAISS retrieval
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


# Test retrieval
test_results = retrieve_chunks("UPI fraud prevention guidelines")
print(f"Retrieved {len(test_results)} chunks for test query")
for i, r in enumerate(test_results):
    print(f"\n--- Result {i+1} (score: {r['score']:.4f}) ---")
    print(f"Title: {r['title']}")
    print(f"Citation: {r['citation']}")
    print(f"Preview: {r['chunk_text'][:200]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. RAG Generation with Foundation Model API

# COMMAND ----------

def rag_answer(query: str, language: str = "english", top_k: int = 5) -> dict:
    """
    Full RAG pipeline: retrieve → construct prompt → generate answer.
    Supports Hindi and English queries natively (multilingual embeddings).

    Returns dict with 'answer', 'sources', 'language'.
    """
    # Retrieve relevant chunks
    chunks = retrieve_chunks(query, top_k=top_k)

    if not chunks:
        return {
            "answer": "I couldn't find relevant RBI circulars for your query. Please try rephrasing.",
            "sources": [],
            "language": language
        }

    # Build context from retrieved chunks
    context_parts = []
    sources = []
    for i, chunk in enumerate(chunks):
        context_parts.append(f"[Source {i+1}: {chunk['citation']}]\n{chunk['chunk_text']}")
        sources.append(chunk["citation"])

    context = "\n\n---\n\n".join(context_parts)

    # Construct prompt
    language_instruction = ""
    if language.lower() in ["hindi", "hi", "hin"]:
        language_instruction = "IMPORTANT: Respond entirely in Hindi (Devanagari script). "
    elif language.lower() in ["marathi", "mr", "mar"]:
        language_instruction = "IMPORTANT: Respond entirely in Marathi (Devanagari script). "
    elif language.lower() in ["tamil", "ta", "tam"]:
        language_instruction = "IMPORTANT: Respond entirely in Tamil script. "

    system_prompt = f"""You are Digital-Artha, an expert on RBI (Reserve Bank of India) regulations and financial policy.
{language_instruction}
Answer the user's question based ONLY on the provided RBI circular excerpts.
Always cite your sources using [Source N] notation.
If the excerpts don't contain enough information, say so honestly.
Be concise but thorough. Explain complex regulatory language in simple terms."""

    user_prompt = f"""Based on the following RBI circular excerpts, answer my question.

## RBI Circular Excerpts:
{context}

## Question:
{query}

## Answer (cite sources with [Source N]):"""

    # Generate via Foundation Model API
    with mlflow.start_run(run_name=f"rag_query_{hash(query) % 10000}", nested=True):
        mlflow.log_param("query", query[:200])
        mlflow.log_param("language", language)
        mlflow.log_param("chunks_retrieved", len(chunks))

        response = w.serving_endpoints.query(
            name="databricks-meta-llama-3-1-70b-instruct",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=800,
            temperature=0.1  # Low temperature for factual accuracy
        )

        answer = response.choices[0].message.content

        mlflow.log_metric("answer_length", len(answer))
        mlflow.log_metric("sources_cited", answer.count("[Source"))

    return {
        "answer": answer,
        "sources": sources,
        "language": language
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Test RAG Pipeline

# COMMAND ----------

# English test
result = rag_answer("What are the RBI guidelines for digital payment fraud prevention?")
print("=== English Query ===")
print(f"Answer:\n{result['answer']}")
print(f"\nSources: {result['sources']}")

# COMMAND ----------

# Hindi test (multilingual embeddings handle Hindi queries natively)
result_hi = rag_answer("यूपीआई में धोखाधड़ी रोकने के लिए आरबीआई के क्या नियम हैं?", language="hindi")
print("=== Hindi Query ===")
print(f"Answer:\n{result_hi['answer']}")
print(f"\nSources: {result_hi['sources']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Save RAG Configuration

# COMMAND ----------

# Save config for the agent to reuse
rag_config = {
    "vs_mode": vs_mode,
    "vs_endpoint_name": vs_endpoint_name if vs_mode == "databricks" else None,
    "vs_index_name": vs_index_name if vs_mode == "databricks" else None,
    "faiss_index_path": "/dbfs/FileStore/digital_artha/faiss_index/rbi_circulars.faiss" if vs_mode == "faiss" else None,
    "faiss_metadata_path": "/dbfs/FileStore/digital_artha/faiss_index/chunk_metadata.pkl" if vs_mode == "faiss" else None,
    "embedding_model": "intfloat/multilingual-e5-small" if vs_mode == "faiss" else "databricks-bge-large-en",
    "llm_endpoint": "databricks-meta-llama-3-1-70b-instruct",
    "top_k": 5,
    "catalog": catalog,
    "schema": schema,
}

import json
dbutils.fs.put(
    f"dbfs:/FileStore/digital_artha/rag_config.json",
    json.dumps(rag_config, indent=2),
    overwrite=True
)
print(f"RAG config saved. Mode: {vs_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAG Pipeline Complete
# MAGIC
# MAGIC **Artifacts created:**
# MAGIC - Vector Search index (Databricks managed) or FAISS index on DBFS
# MAGIC - `retrieve_chunks()` function for semantic retrieval
# MAGIC - `rag_answer()` function for full RAG with citation
# MAGIC - RAG config JSON for agent consumption
# MAGIC
# MAGIC **Next step:** Run `05-loan-eligibility.py` for scheme matching.
