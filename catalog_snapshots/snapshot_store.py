import os
import json
import hashlib
import datetime
import sqlite3
import pandas as pd
import uuid

# --------------------------------------------------
# 📁 Storage locations
# --------------------------------------------------

BASE_DIR = "catalog_snapshots/snapshots"
BASELINE_FILE = "catalog_snapshots/baseline.json"

os.makedirs(BASE_DIR, exist_ok=True)


# --------------------------------------------------
# 🔐 Hashing
# --------------------------------------------------

def compute_snapshot_hash(snapshot: dict) -> str:
    """
    Stable hash for snapshot content.
    """
    payload = json.dumps(snapshot, sort_keys=True).encode()
    return hashlib.sha256(payload).hexdigest()


# --------------------------------------------------
# 💾 Save snapshot to disk
# --------------------------------------------------

# def persist_snapshot(snapshot: dict) -> str:
#     """
#     Persist snapshot JSON with timestamp and hash.
#     Returns file path.
#     """

#     ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

#     db = snapshot["metadata"]["database"]
#     schema = snapshot["metadata"]["schema"]

#     fname = f"{db}_{schema}_{ts}.json"
#     path = os.path.join(BASE_DIR, fname)

#     snapshot["metadata"]["timestamp"] = ts
#     snapshot["metadata"]["hash"] = compute_snapshot_hash(snapshot)

#     with open(path, "w", encoding="utf-8") as f:
#         json.dump(snapshot, f, indent=2)

#     return path


# --------------------------------------------------
# ⭐ Set baseline snapshot
# --------------------------------------------------

# def set_baseline(snapshot_path: str):
#     """
#     Mark snapshot as baseline.
#     """

#     with open(snapshot_path, encoding="utf-8") as f:
#         snap = json.load(f)

#     baseline_payload = {
#         "baseline_path": snapshot_path,
#         "hash": snap["metadata"]["hash"],
#         "timestamp": snap["metadata"]["timestamp"],
#     }

#     with open(BASELINE_FILE, "w", encoding="utf-8") as f:
#         json.dump(baseline_payload, f, indent=2)


# --------------------------------------------------
# 📥 Load baseline metadata
# --------------------------------------------------

# def get_baseline():
#     """
#     Return baseline metadata dict or None.
#     """

#     if not os.path.exists(BASELINE_FILE):
#         return None

#     with open(BASELINE_FILE, encoding="utf-8") as f:
#         return json.load(f)


# --------------------------------------------------
# 📂 Load any snapshot
# --------------------------------------------------

# def load_snapshot(path: str) -> dict:
#     """
#     Load snapshot JSON from disk.
#     """

#     with open(path, encoding="utf-8") as f:
#         return json.load(f)


# --------------------------------------------------
# 🧹 Retention policy
# --------------------------------------------------

def retain_last_n(n: int = 5):
    """
    Keep only latest N snapshots.
    """

    files = sorted(
        [
            os.path.join(BASE_DIR, f)
            for f in os.listdir(BASE_DIR)
            if f.endswith(".json")
        ],
        reverse=True,
    )

    for old in files[n:]:
        os.remove(old)

def persist_snapshot_sqlite(snapshot, db_path="zeusai_results.db"):

    snapshot["metadata"]["hash"] = compute_snapshot_hash(snapshot)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO catalog_snapshots (
            snapshot_id,
            timestamp,
            database,
            schema,
            dialect,
            hash,
            snapshot_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        snapshot["snapshot_id"],
        snapshot["metadata"]["timestamp"],
        snapshot["metadata"]["database"],
        snapshot["metadata"]["schema"],
        snapshot["metadata"]["dialect"],
        snapshot["metadata"]["hash"],
        json.dumps(snapshot),
    ))

    conn.commit()
    conn.close()

    return snapshot["snapshot_id"]

def get_baseline_snapshot_sqlite(db_path="zeusai_results.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT s.snapshot_json
        FROM catalog_snapshots s
        JOIN snapshot_baseline b
          ON s.snapshot_id = b.snapshot_id
        LIMIT 1
    """)

    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return json.loads(row[0])

def init_snapshot_tables(db_path="zeusai_results.db"):

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS catalog_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            timestamp TEXT,
            database TEXT,
            schema TEXT,
            dialect TEXT,
            hash TEXT,
            snapshot_json TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS snapshot_baseline (
            snapshot_id TEXT
        )
    """)

    conn.commit()
    conn.close()

def set_baseline(snapshot_id, db_path="zeusai_results.db"):

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("DELETE FROM snapshot_baseline")

    cur.execute("""
        INSERT INTO snapshot_baseline(snapshot_id)
        VALUES (?)
    """, (snapshot_id,))

    conn.commit()
    conn.close()

def migrate_snapshot_tables(db_path="zeusai_results.db"):

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(catalog_snapshots)")
    existing_cols = {r[1] for r in cur.fetchall()}

    required = {
        "snapshot_id",
        "timestamp",
        "database",
        "schema",
        "dialect",
        "hash",
        "snapshot_json",
    }

    missing = required - existing_cols

    for col in missing:
        cur.execute(
            f"ALTER TABLE catalog_snapshots ADD COLUMN {col} TEXT"
        )

    conn.commit()
    conn.close()
