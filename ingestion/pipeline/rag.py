"""
RAG layer for NBA pipeline.
- embed_all(): generates summaries from mart tables, embeds with Google, loads to Snowflake
- query(question): natural language query over embeddings using Snowflake vector similarity

Rate limiting: Google free tier allows 100 requests/minute for gemini-embedding-001.
We use a token bucket to maximize throughput without exceeding limits.
Change detection: only re-embeds entities whose summary text has changed since last run.
"""

import os
import re
import time
from collections import deque
from google import genai
import anthropic
from dotenv import load_dotenv
from pipeline.load import get_conn, setup_snowflake
import pickle
from pathlib import Path


load_dotenv()

BATCH_SIZE = 50
EMBED_MODEL = "models/gemini-embedding-001"
CHAT_MODEL = "claude-haiku-4-5-20251001"
RATE_LIMIT = 2  # requests per minute (leave 10% headroom below 100)
WINDOW = 60.0  # seconds
EMBEDDINGS_CACHE = Path(__file__).parent.parent / "embeddings_cache.pkl"


# -- Clients ------------------------------------------------------------------


def get_google_client() -> genai.Client:
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise EnvironmentError("Missing required env var: GOOGLE_API_KEY")
    return genai.Client(api_key=api_key)


# -- Token bucket rate limiter ------------------------------------------------


class RateLimiter:
    """
    Token bucket rate limiter.
    Tracks request timestamps in a sliding window and blocks when the limit
    would be exceeded, rather than using fixed sleeps between batches.
    """

    def __init__(self, rate: int, window: float):
        self.rate = rate
        self.window = window
        self.timestamps: deque = deque()

    def wait(self):
        now = time.monotonic()
        # Remove timestamps outside the current window
        while self.timestamps and self.timestamps[0] < now - self.window:
            self.timestamps.popleft()
        # If at limit, wait until oldest request falls outside window
        if len(self.timestamps) >= self.rate:
            wait_time = self.window - (now - self.timestamps[0]) + 0.1
            if wait_time > 0:
                print(f"  Rate limit reached, waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
        self.timestamps.append(time.monotonic())


# -- Summary generators -------------------------------------------------------


def team_summary(row: dict) -> str:
    return (
        f"The {row['TEAM_NAME']} ({row['TEAM_ABBREVIATION']}) have a "
        f"{row['WINS']}-{row['LOSSES']} record ({row['WIN_PCT']:.1%} win rate) "
        f"in the {row['SEASON']} season, "
        f"averaging {row['AVG_POINTS']} PPG, {row['AVG_REBOUNDS']} RPG, "
        f"{row['AVG_ASSISTS']} APG, {row['AVG_PLUS_MINUS']:+.1f} net rating. "
        f"Home win rate: {row['HOME_WIN_PCT']:.1%}, "
        f"Away win rate: {row['AWAY_WIN_PCT']:.1%}. "
        f"Ranked {row['WIN_PCT_RANK']} in win percentage, "
        f"{row['NET_RATING_RANK']} in net rating."
    )


def _fmt_pct(v) -> str:
    return f"{v:.1%}" if v is not None else "N/A"


def player_summary(row: dict) -> str:
    return (
        f"{row['PLAYER_NAME']} ({row['TEAM_ABBREVIATION']}) played "
        f"{row['GAMES_PLAYED']} games averaging "
        f"{row['PPG']} PPG, {row['RPG']} RPG, {row['APG']} APG, "
        f"{row['SPG']} SPG, {row['BPG']} BPG "
        f"with a {_fmt_pct(row['TRUE_SHOOTING_PCT'])} true shooting percentage "
        f"and {row['AVG_PLUS_MINUS']:+.1f} plus/minus. "
        f"Shoots {_fmt_pct(row['FG_PCT'])} FG, {_fmt_pct(row['FG3_PCT'])} 3PT, "
        f"{_fmt_pct(row['FT_PCT'])} FT."
    )


# -- Embedding ----------------------------------------------------------------


def embed_batch(
    client: genai.Client, limiter: RateLimiter, texts: list[str], max_retries: int = 5
) -> list[list[float]]:
    backoff = 10
    retries = 0
    while True:
        limiter.wait()
        try:
            result = client.models.embed_content(model=EMBED_MODEL, contents=texts)
            return [e.values for e in result.embeddings]
        except Exception as e:
            if "429" in str(e) and retries < max_retries:
                retries += 1
                match = re.search(r"retryDelay.*?(\d+)s", str(e))
                wait = int(match.group(1)) + 2 if match else backoff
                print(
                    f"  429 received (attempt {retries}/{max_retries}), waiting {wait}s..."
                )
                time.sleep(wait)
                backoff = min(backoff * 2, 120)
            else:
                raise


# -- Load embeddings to Snowflake ---------------------------------------------


def get_existing_summaries(conn) -> dict[tuple, str]:
    """
    Return a dict of {(entity_type, entity_id): summary} for all rows
    currently in the EMBEDDINGS table. Used for change detection.
    """
    cur = conn.cursor()
    try:
        cur.execute("SELECT ENTITY_TYPE, ENTITY_ID, SUMMARY FROM NBA_DB.RAW.EMBEDDINGS")
        return {(row[0], row[1]): row[2] for row in cur.fetchall()}
    except Exception:
        return {}
    finally:
        cur.close()


def upsert_embeddings(conn, records: list[dict]):
    cur = conn.cursor()
    cur.execute("USE DATABASE NBA_DB")
    cur.execute("USE SCHEMA RAW")

    cur.execute("""
        CREATE OR REPLACE TEMPORARY TABLE EMBEDDINGS_STAGE (
            ENTITY_TYPE  VARCHAR(20),
            ENTITY_ID    INTEGER,
            ENTITY_NAME  VARCHAR(100),
            SUMMARY      VARCHAR(2000),
            EMBEDDING    VECTOR(FLOAT, 3072),
            EMBEDDED_AT  TIMESTAMP_NTZ
        )
    """)

    for rec in records:
        vector_str = "[" + ",".join(str(v) for v in rec["embedding"]) + "]"
        name = rec["entity_name"].replace("'", "''")
        summary = rec["summary"].replace("'", "''")
        cur.execute(f"""
            INSERT INTO EMBEDDINGS_STAGE
            SELECT
                '{rec["entity_type"]}',
                {rec["entity_id"]},
                '{name}',
                '{summary}',
                {vector_str}::VECTOR(FLOAT, 3072),
                CURRENT_TIMESTAMP()
        """)

    cur.execute("""
        MERGE INTO EMBEDDINGS tgt
        USING EMBEDDINGS_STAGE src
        ON tgt.ENTITY_TYPE = src.ENTITY_TYPE
        AND tgt.ENTITY_ID = src.ENTITY_ID
        WHEN MATCHED THEN UPDATE SET
            ENTITY_NAME = src.ENTITY_NAME,
            SUMMARY     = src.SUMMARY,
            EMBEDDING   = src.EMBEDDING,
            EMBEDDED_AT = src.EMBEDDED_AT
        WHEN NOT MATCHED THEN INSERT
            (ENTITY_TYPE, ENTITY_ID, ENTITY_NAME, SUMMARY, EMBEDDING, EMBEDDED_AT)
        VALUES
            (src.ENTITY_TYPE, src.ENTITY_ID, src.ENTITY_NAME,
             src.SUMMARY, src.EMBEDDING, src.EMBEDDED_AT)
    """)


# -- Main embed entrypoint ----------------------------------------------------


def embed_all() -> int:
    """
    Read mart tables, generate summaries, embed changed entities only,
    and upsert to Snowflake. Returns number of embeddings updated.
    """
    conn = get_conn()
    setup_snowflake(conn)
    client = get_google_client()
    limiter = RateLimiter(rate=RATE_LIMIT, window=WINDOW)

    cur = conn.cursor()

    cur.execute("SELECT * FROM NBA_DB.DBT_DEV_MARTS.MART_TEAM_SEASON_STATS")
    cols = [d[0] for d in cur.description]
    teams = [dict(zip(cols, row)) for row in cur.fetchall()]

    cur.execute("SELECT * FROM NBA_DB.DBT_DEV_MARTS.MART_PLAYER_SEASON_STATS")
    cols = [d[0] for d in cur.description]
    players = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()

    # Build candidate records with summaries
    candidates = []
    for t in teams:
        candidates.append(
            {
                "entity_type": "team",
                "entity_id": t["TEAM_ID"],
                "entity_name": t["TEAM_NAME"],
                "summary": team_summary(t),
            }
        )
    for p in players:
        candidates.append(
            {
                "entity_type": "player",
                "entity_id": p["PLAYER_ID"],
                "entity_name": p["PLAYER_NAME"],
                "summary": player_summary(p),
            }
        )

    # Change detection -- only embed entities whose summary has changed
    existing = get_existing_summaries(conn)
    to_embed = [
        r
        for r in candidates
        if existing.get((r["entity_type"], r["entity_id"])) != r["summary"]
    ]

    total = len(candidates)
    changed = len(to_embed)
    skipped = total - changed
    print(f"{total} entities total: {changed} changed, {skipped} unchanged (skipping)")

    if not to_embed:
        print("Nothing to embed.")
        conn.close()
        return 0

    print(f"Embedding {changed} summaries...")
    vectors = []

    if EMBEDDINGS_CACHE.exists():
        with open(EMBEDDINGS_CACHE, "rb") as f:
            cache = pickle.load(f)
        print(f"  Loaded {len(cache)} vectors from cache")
    else:
        cache = {}

    to_embed_fresh = [r for r in to_embed if r["summary"] not in cache]
    to_embed_cached = [r for r in to_embed if r["summary"] in cache]

    print(f"  {len(to_embed_cached)} from cache, {len(to_embed_fresh)} need embedding")

    for i in range(0, len(to_embed_fresh), BATCH_SIZE):
        batch = to_embed_fresh[i : i + BATCH_SIZE]
        batch_vectors = embed_batch(client, limiter, [r["summary"] for r in batch])
        for rec, vec in zip(batch, batch_vectors):
            cache[rec["summary"]] = vec
        vectors.extend(batch_vectors)
        # Save after every batch so progress survives quota exhaustion
        with open(EMBEDDINGS_CACHE, "wb") as f:
            pickle.dump(cache, f)
        print(
            f"  {min(i + BATCH_SIZE, len(to_embed_fresh))}/{len(to_embed_fresh)} embedded"
        )

    # Save cache after embedding
    with open(EMBEDDINGS_CACHE, "wb") as f:
        pickle.dump(cache, f)

    for rec in to_embed_cached:
        vectors.insert(to_embed.index(rec), cache[rec["summary"]])

    records = []
    for rec, vector in zip(to_embed, [cache[r["summary"]] for r in to_embed]):
        rec["embedding"] = vector
        records.append(rec)

    print(f"Loading {len(records)} embeddings to Snowflake...")
    upsert_embeddings(conn, records)
    conn.close()

    print(f"Done. {len(records)} embeddings updated.")
    return len(records)


# -- Query entrypoint ---------------------------------------------------------


def query(question: str, top_k: int = 5) -> str:
    """
    Answer a natural language question using vector similarity search
    over NBA_DB.RAW.EMBEDDINGS, then pass context to Gemini.
    """
    client = get_google_client()
    conn = get_conn()

    result = client.models.embed_content(
        model=EMBED_MODEL,
        contents=question,
    )
    q_vector = result.embeddings[0].values
    vector_str = "[" + ",".join(str(v) for v in q_vector) + "]"

    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            ENTITY_NAME,
            ENTITY_TYPE,
            SUMMARY,
            VECTOR_COSINE_SIMILARITY(
                EMBEDDING,
                {vector_str}::VECTOR(FLOAT, 3072)
            ) AS similarity
        FROM NBA_DB.RAW.EMBEDDINGS
        ORDER BY similarity DESC
        LIMIT {top_k}
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return "No embeddings found. Run `python cli.py embed` first."
    print("\n--- Context retrieved ---")
    for row in rows:
        print(f"{row[0]} ({row[1]}): similarity={row[3]:.3f}")
        print(f"  {row[2][:100]}")
    print("---\n")
    context = "\n\n".join(f"{row[0]} ({row[1]}):\n{row[2]}" for row in rows)

    prompt = f"""You are an NBA data analyst. Answer the following question using
only the context provided. Be concise and specific.

Question: {question}

Context:
{context}

Answer:"""

    anthropic_client = anthropic.Anthropic()
    message = anthropic_client.messages.create(
        model=CHAT_MODEL,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text
