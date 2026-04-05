import os
import time
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from raw import (
    load_raw,
    mark_file_as_loaded,
    is_file_already_loaded,
    compute_file_hash
)
from silver import build_silver
from gold import build_gold

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))
CSV_FILE = os.getenv("DATA_FILE", "transactions.csv")
DB_URL = os.getenv(
    "DB_URL",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/medallion"
)

print(
    f"CSV file in use: {CSV_FILE}"
    f"\nChunk size: {CHUNK_SIZE}"
    f"\nDatabase URL: {DB_URL}"
)


def get_engine():
    for _ in range(30):
        try:
            engine = create_engine(DB_URL)
            with engine.connect():
                pass
            print("Connected to the database")
            return engine
        except OperationalError:
            print("Waiting for the database...")
            time.sleep(1)

    raise Exception("Could not connect to the database")


if __name__ == "__main__":
    engine = get_engine()

    file_name = os.path.basename(CSV_FILE)
    file_hash = compute_file_hash(CSV_FILE)

    if is_file_already_loaded(engine, file_name, file_hash):
        print(f"File {file_name} already loaded with identical hash — skipping RAW")
    else:
        load_raw(engine, CSV_FILE, CHUNK_SIZE)
        build_silver(engine)
        build_gold(engine)
        mark_file_as_loaded(engine, file_name, file_hash)
        print("Pipeline complete for newly ingested file")

    print("Done")