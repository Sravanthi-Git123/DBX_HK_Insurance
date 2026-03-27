# Databricks notebook source
# CELL 1 — Imports, Config & Databricks LLM Connection
# ============================================================

# Install sentence-transformers locally (no external API)
%pip install sentence-transformers faiss-cpu -q

# Restart Python after install
dbutils.library.restartPython()
%pip install sentence-transformers faiss-cpu -q
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install OpenAI

# COMMAND ----------

# ============================================================
# CELL 2 — Imports & Configuration
# ============================================================

# PySpark SQL functions — used to query and transform Databricks tables
# (filtering, aggregating, column operations in the RAG pipeline)
from pyspark.sql import functions as F

# SentenceTransformer — converts text (policy chunks) into vector embeddings
# We use this to turn natural language into numbers that FAISS can search
from sentence_transformers import SentenceTransformer

# FAISS (Facebook AI Similarity Search) — in-memory vector index
# Given a user's question embedding, FAISS finds the most similar policy chunks instantly
import faiss

# NumPy — required by FAISS to handle embedding arrays (float32 format)
import numpy as np

# datetime — used to timestamp each query saved in rag_query_history
# Helps compliance team audit when each question was asked and answered
import datetime

# OpenAI-compatible client — Databricks exposes its LLM via an OpenAI-style API
# We use this to send retrieved context + user question to the LLM for final answer generation
from openai import OpenAI


# ── Databricks Authentication ──────────────────────────────────────────
# Fetches the current user's API token automatically from the notebook context
# Why: We don't hardcode secrets — this keeps credentials secure in Databricks
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Fetches the Databricks workspace URL (e.g., adb-xxxx.azuredatabricks.net)
# Why: Required to build the correct API endpoint URL for LLM serving
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

# Creates the LLM client pointed at Databricks model serving
# Why: Databricks uses OpenAI-compatible endpoints, so we reuse the OpenAI SDK
#      instead of writing custom HTTP calls
client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)


# ── Model Configuration ────────────────────────────────────────────────
# The LLM hosted on Databricks used to generate the final natural language answer
# Why GPT-OSS-20B: Good balance of quality and speed for insurance Q&A use case
MODEL_NAME = "databricks-gpt-oss-20b"

# The embedding model used to convert text → vectors for semantic search
# Why MiniLM-L6-v2: Lightweight (80MB), fast, and accurate enough for policy text similarity
EMBED_MODEL = "all-MiniLM-L6-v2"


# ── Table References ───────────────────────────────────────────────────
# Gold layer policy dimension table — source of truth for all policy documents
# This is what we chunk, embed, and index for RAG retrieval
POLICY_TABLE = "bricksquad.gold.dim_policy"

# Audit/history table — every user query and the LLM's response is logged here
# Why: Compliance requirement — business users must have a traceable Q&A history
HISTORY_TABLE = "bricksquad.gold.rag_query_history"


# ── RAG Chunking Parameters ────────────────────────────────────────────
# Maximum number of characters per text chunk sent to the embedding model
# Why 400: Keeps chunks small enough to be semantically focused,
#          but large enough to contain full policy clauses
CHUNK_SIZE = 400

# Number of characters overlapping between consecutive chunks
# Why 80: Prevents important context from being lost at chunk boundaries
#         e.g., a sentence split across two chunks still appears in both
CHUNK_OVERLAP = 80

# Number of most-relevant chunks retrieved from FAISS for each user question
# Why 4: Sends enough context to the LLM without exceeding token limits
#        More chunks = more noise; fewer = missing info
TOP_K = 4

# COMMAND ----------

# display the policy table and display the first 5 rows
df = spark.table(POLICY_TABLE)
display(df.limit(5))

# COMMAND ----------

# Convert each Gold policy row into a structured text document for RAG embedding
# Why: FAISS works on plain text chunks — we format each row as a readable
#      paragraph so the embedding model captures field context, not just raw values
def to_text(row):
    return {
        # Preserve policy_number as metadata — used later to trace which
        # policy a retrieved chunk came from (auditability)
        "policy_number": row["policy_number"],

        # Build a human-readable text block from all key policy fields
        # Why this format: Embedding models understand natural language better
        # than raw column=value pairs — "State: TX" embeds more meaningfully
        # than just "TX"
        "text": f"""
Policy Number: {row['policy_number']}
State: {row['policy_state']}
CSL Coverage: {row['policy_csl']}
Deductible: {row['policy_deductable']}
Annual Premium: {row['policy_annual_premium']}
Umbrella Limit: {row['umbrella_limit']}
Vehicle ID: {row['car_id']}
Customer ID: {row['customer_id']}
"""
    }

# Pull all policy records from Spark into Python driver memory
# Why collect(): FAISS and SentenceTransformer run on the driver, not distributed
# Note: Only safe here because dim_policy is a manageable Gold table, not raw Bronze
rows = df.collect()

# Apply to_text() to every row — produces a list of {policy_number, text} dicts
# This becomes the document corpus that gets chunked and embedded in the next step
docs = [to_text(r) for r in rows]

# COMMAND ----------

# Split a single policy text document into overlapping chunks
# Why chunking: Embedding models have a token limit — long policy texts
#               must be broken into smaller pieces before embedding
# Why overlap: Ensures sentences split across chunk boundaries still
#              appear fully in at least one chunk, preserving context
def chunk(text, size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    chunks = []
    start = 0

    while start < len(text):
        end = start + size

        # Extract one chunk from start to end (max 400 chars)
        chunks.append(text[start:end])

        # Stop if this chunk reached the end of the document
        if end >= len(text):
            break

        # Move forward by (size - overlap) so the next chunk
        # re-reads the last 80 characters of the current chunk
        # Why: Prevents context loss at chunk boundaries
        start = end - overlap

    return chunks


# Master list that holds every chunk from every policy document
# Each entry carries chunk_id, policy_number, and the text snippet
all_chunks = []

for d in docs:
    # Chunk this policy's full text into overlapping 400-char segments
    chunks = chunk(d["text"])

    for i, c in enumerate(chunks):
        all_chunks.append({
            # Unique ID per chunk — policy number + chunk index
            # Why: Needed to trace exactly which part of which policy
            #      was retrieved when answering a user question (audit trail)
            "chunk_id": f"{d['policy_number']}_{i}",

            # Keep policy_number on every chunk so we can link retrieved
            # chunks back to the original policy record in dim_policy
            "policy_number": d["policy_number"],

            # The actual text snippet that gets embedded into a vector
            "text": c
        })

# COMMAND ----------

# Load the sentence embedding model into memory on the driver
# Why all-MiniLM-L6-v2: Lightweight (80MB) but accurate — converts
#                        policy text into 384-dimension semantic vectors
#                        fast enough for real-time RAG queries
embed_model = SentenceTransformer(EMBED_MODEL)

# Extract just the raw text from each chunk dict
# Why: encode() only accepts a list of strings, not dicts
texts = [c["text"] for c in all_chunks]

# Convert all policy text chunks into numeric vectors (embeddings)
# Why convert_to_numpy: FAISS requires NumPy float32 arrays, not PyTorch tensors
# Result shape: (num_chunks, 384) — one 384-dim vector per chunk
embeddings = embed_model.encode(texts, convert_to_numpy=True)

# Normalize all vectors to unit length (L2 norm = 1)
# Why: When vectors are normalized, dot product == cosine similarity
#      This makes IndexFlatIP behave as a cosine similarity search,
#      which is more reliable for semantic text matching than raw dot product
faiss.normalize_L2(embeddings)

# Get the vector dimension size (384 for MiniLM)
# Why: FAISS index must be initialized with the exact embedding dimension
dim = embeddings.shape[1]

# Create a FAISS flat index using Inner Product (dot product) similarity
# Why IndexFlatIP: After L2 normalization this equals cosine similarity search
#                  "Flat" means exact search — no approximation, checks all vectors
#                  Best choice when corpus size is manageable (Gold policy table)
index = faiss.IndexFlatIP(dim)

# Add all policy chunk embeddings into the FAISS index
# Why: This builds the searchable vector store in memory —
#      at query time, user question embedding is compared against these
index.add(embeddings)

# COMMAND ----------

# Parse the LLM API response and extract the generated answer text
# Why a separate function: Databricks model serving can return content
#                          in two different formats (list of blocks OR plain string)
#                          depending on the model — this handles both safely
def extract_text(response):

    # Convert the response object to a plain Python dict for safe key access
    # Why model_dump(): Avoids AttributeError on nested Pydantic response objects
    data = response.model_dump()

    # Loop through all choices returned by the LLM
    # Why choices: OpenAI-style APIs can return multiple candidate responses —
    #              we iterate to find the first one with valid text content
    for choice in data.get("choices", []):

        # Extract the message dict from this choice
        msg = choice.get("message", {})

        # Extract the content field — could be a list of blocks or a plain string
        content = msg.get("content", [])

        # Format 1: Content is a list of typed blocks
        # Why: Some Databricks models return structured content like
        #      [{"type": "text", "text": "answer here"}, ...]
        if isinstance(content, list):
            for item in content:
                # Only return text-type blocks, skip tool_use or image blocks
                if item.get("type") == "text":
                    return item.get("text")

        # Format 2: Content is already a plain string
        # Why: Simpler models (like GPT-OSS-20B) return content directly as a string
        #      No need to iterate — just return it as-is
        if isinstance(content, str):
            return content

    # Fallback: return empty string if no valid content found in any choice
    # Why not raise exception: Caller handles empty response gracefully
    #                          avoids crashing the RAG pipeline on edge cases
    return ""

# COMMAND ----------

# Core RAG pipeline function — takes a user question and returns an LLM-generated answer
# grounded in the most relevant policy chunks retrieved from FAISS
# Why RAG: Instead of asking the LLM to answer from memory (hallucination risk),
#          we first retrieve real policy data and feed it as context to the LLM
def rag_query(question, top_k=TOP_K):

    # Step 1: Embed the user's question into a vector using the same model
    #         used to embed policy chunks — must match for similarity to work
    q_vec = embed_model.encode([question], convert_to_numpy=True)

    # Normalize the query vector to unit length — same as we did for policy chunks
    # Why: Ensures cosine similarity comparison is valid (both sides normalized)
    faiss.normalize_L2(q_vec)

    # Step 2: Search FAISS index for the top_k most similar policy chunks
    # Returns:
    #   scores — cosine similarity scores (higher = more relevant)
    #   idxs   — positions in all_chunks list of the matched chunks
    scores, idxs = index.search(q_vec, top_k)

    # Containers to build the LLM context block
    retrieved = []       # reserved for future use (e.g. returning raw chunks)
    policy_ids = []      # tracks which policies were retrieved (for audit trail)
    context = ""         # the text block injected into the LLM prompt

    # Step 3: Build context string from retrieved chunks
    for i, idx in enumerate(idxs[0]):
        c = all_chunks[idx]

        # Track which policy this chunk belongs to
        policy_ids.append(c["policy_number"])

        # Append this chunk's text to context, labelled with its policy number
        # Why labelled: Helps LLM distinguish between multiple policies in context
        context += f"\nPolicy {c['policy_number']}:\n{c['text']}\n"

    # Deduplicate policy IDs — same policy may appear in multiple top chunks
    policy_ids = list(set(policy_ids))

    # Compute average similarity score across retrieved chunks as confidence proxy
    # Why mean of scores: Single number that tells caller how well the retrieved
    #                     chunks matched the question (used in history logging)
    confidence = float(np.mean(scores[0]))

    # Step 4: Build the LLM prompt — inject retrieved policy context + question
    # Why temperature=0.1: Low randomness — insurance answers must be factual
    #                       and consistent, not creative
    # Why "plain English only": Prevents LLM from returning JSON or chain-of-thought
    #                           reasoning which breaks the UI display
    prompt = f"""
You are an insurance policy assistant.

Return ONLY plain English.
Do NOT return JSON or reasoning.

CONTEXT:
{context}

QUESTION:
{question}
"""

    # Step 5: Send prompt to the Databricks-hosted LLM and get response
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1     # Low temperature = deterministic, factual answers
    )

    # Step 6: Extract plain text answer from the LLM response object
    # Why extract_text(): Handles both list-of-blocks and plain string formats
    answer = extract_text(response)

    # Return structured result — question, answer, source policies, confidence
    # Why structured: Caller uses this to log to rag_query_history and display to user
    return {
        "question": question,
        "answer": answer,
        "source_policies": policy_ids,   # Which policies were used to answer
        "confidence_score": confidence    # How relevant the retrieved chunks were
    }

# COMMAND ----------

# Persist every RAG query result to the Gold audit history table
# Why logging: Compliance requirement — regulators and business users
#              must have a full traceable record of every question asked
#              and every answer the LLM gave, with timestamps
def log_rag_history(result):

    # Package the RAG result into a single-row Spark DataFrame
    # Why Spark DataFrame: Allows direct write to Databricks Delta table
    #                      with schema enforcement and ACID guarantees
    log_df = spark.createDataFrame([(
        result["question"],                         # The original user question
        result["answer"],                           # The LLM-generated answer
        float(result["confidence_score"]),          # Avg FAISS similarity score — cast to float for schema compatibility
        ",".join(result["source_policies"]),        # Comma-separated policy numbers used as context
                                                    # Why join: Delta tables don't support list columns natively
        datetime.datetime.now()                     # Exact timestamp of when this query was executed
    )], schema=[
        "question",
        "answer",
        "confidence_score",
        "source_policies",
        "timestamp"
    ])

    # Append this query's log row to the history Delta table
    # Why append: We never overwrite history — every query is a new immutable record
    #             Compliance teams can query full Q&A history ordered by timestamp
    log_df.write.mode("append").saveAsTable(HISTORY_TABLE)

# COMMAND ----------

# Test the end-to-end RAG pipeline with 5 business-relevant insurance questions
# Why these questions: They cover all 3 core business problems —
#   - Regulatory (deductible, CSL coverage details)
#   - Claims (policy lookup by ID)
#   - Revenue (premium comparison, umbrella coverage)
test_questions = [
    "What is deductible for policy POL-10001?",     # Single policy lookup — tests exact retrieval
    "Which policies include umbrella coverage?",     # Multi-policy scan — tests broad context retrieval
    "Compare policies from California and Texas",    # Cross-region comparison — tests multi-chunk reasoning
    "What is CSL coverage in policies?",             # Definition question — tests LLM explanation ability
    "Which policy has highest premium?"              # Aggregation question — tests ranking from context
]

# Run each test question through the full RAG pipeline
for q in test_questions:

    # Visual separator — makes console output easy to read per question
    print("\n" + "="*60)
    print("Q:", q)

    # Step 1: Run RAG — embed question → FAISS search → LLM answer
    res = rag_query(q)

    # Step 2: Print retrieved source policies — confirms correct chunks were fetched
    print("Sources:", res["source_policies"])

    # Step 3: Print confidence score — avg FAISS cosine similarity
    # Why check this: Low score (<0.5) means retrieved chunks may not be relevant
    print("Confidence:", res["confidence_score"])

    # Step 4: Print the LLM-generated answer in plain English
    print("Answer:", res["answer"])

    # Step 5: Persist this Q&A result to Gold audit history table
    # Why every iteration: Each question is independently logged —
    #                      compliance team can audit all 5 queries separately
    log_rag_history(res)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bricksquad.gold.rag_query_history;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM bricksquad.gold.rag_query_history
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bricksquad.gold.rag_query_history
# MAGIC ORDER BY timestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC 🧠 What is Databricks Genie Space?
# MAGIC
# MAGIC A Genie Space is a feature in Databricks that lets business users ask questions in plain English and get SQL-based answers automatically from your data.
# MAGIC
# MAGIC It is basically a “chat interface on top of your tables”.
# MAGIC
# MAGIC 🚀 Simple Definition
# MAGIC
# MAGIC 👉 Genie Space =
# MAGIC A natural language assistant built on your data tables that converts user questions into SQL and returns results instantly.

# COMMAND ----------

# MAGIC %md
# MAGIC genie stepup

# COMMAND ----------

# MAGIC %md
# MAGIC step-1: open the genie space 
# MAGIC
# MAGIC ![](/Volumes/bricksquad/default/images/genie .png)

# COMMAND ----------

# MAGIC %md
# MAGIC connect to the data

# COMMAND ----------

# MAGIC %md
# MAGIC step-2
# MAGIC ![](/Volumes/bricksquad/default/images/load data from genie.png)

# COMMAND ----------

# MAGIC %md
# MAGIC genie instruction

# COMMAND ----------

# MAGIC %md
# MAGIC step-3
# MAGIC ![](/Volumes/bricksquad/default/images/genie instuctions.png)

# COMMAND ----------

# MAGIC %md
# MAGIC questions and answer 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC step-4
# MAGIC ![](/Volumes/bricksquad/default/images/ask questions for genie.png)

# COMMAND ----------

# MAGIC %md
# MAGIC
