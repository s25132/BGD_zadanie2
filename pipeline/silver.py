import uuid
import pandas as pd
from sqlalchemy import text
from raw import get_raw_batches


def get_silver_batches(engine) -> set[int]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT batch_no
                FROM silver.batch_log
                ORDER BY batch_no
            """)
        ).fetchall()
    return {int(row[0]) for row in rows}


def build_silver(engine):
    """
    Builds SILVER only for RAW batches that were not processed yet.
    Target table is updated with UPSERT on transaction_id.
    """
    raw_batches = get_raw_batches(engine)
    silver_batches = get_silver_batches(engine)

    batches_to_process = sorted(raw_batches - silver_batches)

    if not batches_to_process:
        print("No new batches to process in SILVER")
        return

    for batch_no in batches_to_process:
        print(f"Building SILVER for batch {batch_no}...")

        with engine.connect() as conn:
            chunk = pd.read_sql(
                text("""
                    SELECT
                        batch_no,
                        source_file,
                        file_hash,
                        transaction_id,
                        customer_id,
                        customer_name,
                        merchant_id,
                        transaction_ts,
                        amount,
                        city,
                        country,
                        payment_method,
                        status
                    FROM raw.transactions_raw
                    WHERE batch_no = :batch_no
                """),
                conn,
                params={"batch_no": int(batch_no)}
            )

        silver = chunk.copy()

        silver["transaction_id"] = silver["transaction_id"].astype(str).str.strip()
        silver["customer_id"] = silver["customer_id"].astype(str).str.strip()
        silver["customer_name"] = silver["customer_name"].astype(str).str.strip().str.title()
        silver["merchant_id"] = silver["merchant_id"].astype(str).str.strip()
        silver["city"] = silver["city"].astype(str).str.strip().str.title()
        silver["country"] = silver["country"].astype(str).str.strip().str.upper()
        silver["payment_method"] = silver["payment_method"].astype(str).str.strip().str.lower()
        silver["status"] = silver["status"].astype(str).str.strip().str.lower()

        silver["transaction_ts"] = pd.to_datetime(
            silver["transaction_ts"],
            errors="coerce"
        )

        silver["amount"] = pd.to_numeric(
            silver["amount"].astype(str).str.replace(",", ".", regex=False).str.strip(),
            errors="coerce"
        )

        silver["validation_error"] = ""

        silver.loc[
            silver["transaction_id"].isin(["", "nan", "none", "NaN", "None"]),
            "validation_error"
        ] += "missing transaction_id; "

        silver.loc[
            silver["transaction_ts"].isna(),
            "validation_error"
        ] += "bad date; "

        silver.loc[
            silver["amount"].isna(),
            "validation_error"
        ] += "bad amount; "

        silver.loc[
            silver["amount"] < 0,
            "validation_error"
        ] += "negative amount; "

        silver["is_valid"] = silver["validation_error"] == ""

        silver = silver[
            [
                "transaction_id",
                "batch_no",
                "source_file",
                "file_hash",
                "customer_id",
                "customer_name",
                "merchant_id",
                "transaction_ts",
                "amount",
                "city",
                "country",
                "payment_method",
                "status",
                "is_valid",
                "validation_error"
            ]
        ]

        stage_table = f"transactions_clean_stage_{uuid.uuid4().hex[:8]}"

        with engine.begin() as conn:
            conn.execute(text(f"""
                CREATE TABLE silver.{stage_table} (
                    transaction_id TEXT,
                    batch_no INT,
                    source_file TEXT,
                    file_hash TEXT,
                    customer_id TEXT,
                    customer_name TEXT,
                    merchant_id TEXT,
                    transaction_ts TIMESTAMP,
                    amount NUMERIC,
                    city TEXT,
                    country TEXT,
                    payment_method TEXT,
                    status TEXT,
                    is_valid BOOLEAN,
                    validation_error TEXT
                )
            """))

        silver.to_sql(
            stage_table,
            engine,
            schema="silver",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000
        )

        with engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO silver.transactions_clean (
                    transaction_id,
                    batch_no,
                    source_file,
                    file_hash,
                    customer_id,
                    customer_name,
                    merchant_id,
                    transaction_ts,
                    amount,
                    city,
                    country,
                    payment_method,
                    status,
                    is_valid,
                    validation_error,
                    updated_at
                )
                SELECT
                    transaction_id,
                    batch_no,
                    source_file,
                    file_hash,
                    customer_id,
                    customer_name,
                    merchant_id,
                    transaction_ts,
                    amount,
                    city,
                    country,
                    payment_method,
                    status,
                    is_valid,
                    validation_error,
                    CURRENT_TIMESTAMP
                FROM silver.{stage_table}
                ON CONFLICT (transaction_id) DO UPDATE
                SET
                    batch_no = EXCLUDED.batch_no,
                    source_file = EXCLUDED.source_file,
                    file_hash = EXCLUDED.file_hash,
                    customer_id = EXCLUDED.customer_id,
                    customer_name = EXCLUDED.customer_name,
                    merchant_id = EXCLUDED.merchant_id,
                    transaction_ts = EXCLUDED.transaction_ts,
                    amount = EXCLUDED.amount,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country,
                    payment_method = EXCLUDED.payment_method,
                    status = EXCLUDED.status,
                    is_valid = EXCLUDED.is_valid,
                    validation_error = EXCLUDED.validation_error,
                    updated_at = CURRENT_TIMESTAMP
            """))

            conn.execute(
                text("""
                    INSERT INTO silver.batch_log (batch_no)
                    VALUES (:batch_no)
                    ON CONFLICT (batch_no) DO NOTHING
                """),
                {"batch_no": int(batch_no)}
            )

            conn.execute(text(f"DROP TABLE silver.{stage_table}"))

    print("SILVER complete")