import hashlib
import os
import pandas as pd
from sqlalchemy import text


def compute_file_hash(file_path: str, chunk_size: int = 8192) -> str:
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            sha256.update(chunk)
    return sha256.hexdigest()


def mark_file_as_loaded(engine, file_name: str, file_hash: str):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO raw.ingestion_log (file_name, file_hash)
                VALUES (:file_name, :file_hash)
                ON CONFLICT (file_name, file_hash) DO NOTHING
            """),
            {
                "file_name": file_name,
                "file_hash": file_hash
            }
        )


def is_file_already_loaded(engine, file_name: str, file_hash: str) -> bool:
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT 1
                FROM raw.ingestion_log
                WHERE file_name = :file_name
                  AND file_hash = :file_hash
                LIMIT 1
            """),
            {
                "file_name": file_name,
                "file_hash": file_hash
            }
        ).fetchone()

    print(
        f"Checking whether file '{file_name}' with hash '{file_hash[:12]}...' "
        f"has already been loaded: {'YES' if result else 'NO'}"
    )
    return result is not None


def get_max_raw_batch_no(engine) -> int:
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COALESCE(MAX(batch_no), 0) FROM raw.transactions_raw")
        ).scalar()
    return int(result or 0)


def get_raw_batches(engine) -> set[int]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT DISTINCT batch_no
                FROM raw.transactions_raw
                ORDER BY batch_no
            """)
        ).fetchall()
    return {int(row[0]) for row in rows}


def load_raw(engine, csv_file: str, chunk_size: int):
    """
    Incremental, append-only RAW load.
    Each chunk receives a technical batch_no.
    """
    file_name = os.path.basename(csv_file)
    file_hash = compute_file_hash(csv_file)

    start_batch_no = get_max_raw_batch_no(engine)
    batch_no = start_batch_no

    for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
        batch_no += 1
        print(f"Loading RAW batch {batch_no}...")

        raw_chunk = chunk.copy()
        raw_chunk["batch_no"] = batch_no
        raw_chunk["source_file"] = file_name
        raw_chunk["file_hash"] = file_hash

        raw_chunk.to_sql(
            "transactions_raw",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000
        )

    print("RAW complete")